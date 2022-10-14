/*
Copyright 2022 Authors of Global Resource Service.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package storage

import (
	"bytes"
	"fmt"
	"k8s.io/klog/v2"
	"math"
	"sort"
	"sync"

	"global-resource-service/resource-management/pkg/common-lib/hash"
	"global-resource-service/resource-management/pkg/common-lib/interfaces/store"
	"global-resource-service/resource-management/pkg/common-lib/types"
	"global-resource-service/resource-management/pkg/common-lib/types/location"
	"global-resource-service/resource-management/pkg/common-lib/types/runtime"
	"global-resource-service/resource-management/pkg/distributor/cache"
	"global-resource-service/resource-management/pkg/distributor/node"
)

const (
	VirtualStoreInitSize = 0
	BatchPersistSize     = 100
)

// Maximal two levels:
// 1. Original VS
// 2. Splitted VirtualNodeStores
type VirtualNodeStore struct {
	mu sync.RWMutex

	// node events belong to current VS
	nodeEventByHash map[float64]*node.ManagedNodeEvent
	// lower bound - inclusive - immutable for top level VS, not used for child vs
	lowerbound float64
	// upper bound - exclusive - immutable for top level VS, not used for child vs
	upperbound float64

	// effective lower bound
	adjustedLowerBound float64
	// effective upper bound
	adjustedUpperBound float64

	// empty for child VS
	splittVirtualNodeStores []*VirtualNodeStore

	// nil for top level VS
	parentVirtualNodeStore *VirtualNodeStore

	// one virtual store can only have nodes from one resource partition
	location location.Location

	// client current VS is assigned to
	clientId string

	// event queue that send node events to
	eventQueue *cache.NodeEventQueue
}

func (vs *VirtualNodeStore) GetHostNum() int {
	vs.mu.RLock()
	defer vs.mu.RUnlock()
	return len(vs.nodeEventByHash)
}

func (vs *VirtualNodeStore) GetFreeHostNum() int {
	if len(vs.splittVirtualNodeStores) > 0 {
		freeHostCount := 0
		for i := 0; i < len(vs.splittVirtualNodeStores); i++ {
			if !vs.splittVirtualNodeStores[i].IsAssignedToClient() {
				vs.splittVirtualNodeStores[i].mu.RLock()
				freeHostCount += len(vs.splittVirtualNodeStores[i].nodeEventByHash)
				vs.splittVirtualNodeStores[i].mu.RUnlock()
			}
		}
		return freeHostCount
	} else if vs.IsAssignedToClient() {
		return 0
	} else {
		return vs.GetHostNum()
	}
}

func (vs *VirtualNodeStore) GetAllFreeChildStores() []*VirtualNodeStore {
	if len(vs.splittVirtualNodeStores) > 0 {
		freeStores := make([]*VirtualNodeStore, 0)
		for i := 0; i < len(vs.splittVirtualNodeStores); i++ {
			if !vs.splittVirtualNodeStores[i].IsAssignedToClient() {
				freeStores = append(freeStores, vs.splittVirtualNodeStores[i])
			}
		}
		return freeStores
	} else if vs.IsAssignedToClient() {
		return nil
	} else {
		return []*VirtualNodeStore{vs}
	}
}

func (vs *VirtualNodeStore) IsValidTopVirtualNodeStore() bool {
	if vs.parentVirtualNodeStore != nil {
		return false
	}

	storeCount := len(vs.splittVirtualNodeStores)
	if storeCount == 0 {
		return true
	} else if storeCount == 1 {
		return false
	}

	// check lower/upperbound of top vNode
	if vs.lowerbound != vs.splittVirtualNodeStores[0].adjustedLowerBound || vs.upperbound != vs.splittVirtualNodeStores[storeCount-1].adjustedUpperBound {
		return false
	}
	// check adjusted lower/upperbound
	if vs.splittVirtualNodeStores[0].adjustedLowerBound >= vs.splittVirtualNodeStores[0].adjustedUpperBound {
		return false
	}
	isParentVNodeFound := false
	previousUpperBound := vs.splittVirtualNodeStores[0].adjustedUpperBound
	for i := 0; i < storeCount; i++ {
		currentVNode := vs.splittVirtualNodeStores[i]
		if currentVNode.parentVirtualNodeStore == nil {
			if !isParentVNodeFound {
				isParentVNodeFound = true
			} else {
				return false
			}
			if i > 0 {
				if previousUpperBound != currentVNode.adjustedLowerBound || currentVNode.adjustedLowerBound >= currentVNode.adjustedUpperBound {
					return false
				}
				previousUpperBound = currentVNode.adjustedUpperBound
			}
		}
	}

	return true
}

func (vs *VirtualNodeStore) GetLocation() location.Location {
	return vs.location
}

func (vs *VirtualNodeStore) GetAssignedClient() string {
	return vs.clientId
}

func (vs *VirtualNodeStore) IsAssignedToClient() bool {
	if vs.clientId != "" {
		return true
	}
	return false
}

func (vs *VirtualNodeStore) AssignToClient(clientId string, eventQueue *cache.NodeEventQueue) bool {
	if vs.clientId != "" {
		return false
	} else if clientId == "" {
		return false
	} else if eventQueue == nil {
		return false
	}
	vs.clientId = clientId
	vs.eventQueue = eventQueue

	return true
}

func (vs *VirtualNodeStore) Release() {
	vs.clientId = ""
}

func (vs *VirtualNodeStore) GetOriginalRange() (float64, float64) {
	return vs.lowerbound, vs.upperbound
}

func (vs *VirtualNodeStore) GetAdjustedRange() (float64, float64) {
	if vs.parentVirtualNodeStore == nil && len(vs.splittVirtualNodeStores) == 0 { // top level vNode never being splitted
		return vs.GetOriginalRange()
	}
	return vs.adjustedLowerBound, vs.adjustedUpperBound
}

// Snapshot generates a list of node for the List() call from a client, and a current RV map to client
func (vs *VirtualNodeStore) SnapShot() ([]*types.LogicalNode, types.TransitResourceVersionMap) {
	vs.mu.RLock()
	defer vs.mu.RUnlock()
	nodesCopy := make([]*types.LogicalNode, len(vs.nodeEventByHash))
	index := 0
	rvs := make(types.TransitResourceVersionMap)
	for _, node := range vs.nodeEventByHash {
		nodesCopy[index] = node.CopyNode()
		newRV := node.GetResourceVersionInt64()
		rvLoc := *node.GetRvLocation()
		if lastRV, isOK := rvs[rvLoc]; isOK {
			if lastRV < newRV {
				rvs[rvLoc] = newRV
			}
		} else {
			rvs[rvLoc] = newRV
		}
		index++
	}

	return nodesCopy, rvs
}

func (vs *VirtualNodeStore) GenerateBookmarkEvent() *node.ManagedNodeEvent {
	vs.mu.RLock()
	defer vs.mu.RUnlock()

	for _, n := range vs.nodeEventByHash {
		logicalNode := n.CopyNode()
		nodeEvent := runtime.NewNodeEvent(logicalNode, runtime.Bookmark)
		return node.NewManagedNodeEvent(nodeEvent, n.GetLocation())
	}
	return nil
}

// Input requested host count
// TODO: error cases
func (vs *VirtualNodeStore) RequestCapacity(requestedHostCount int) []*VirtualNodeStore {
	vs.mu.RLock()
	defer vs.mu.RUnlock()

	freeHostNum := vs.GetFreeHostNum()
	if freeHostNum <= requestedHostCount {
		klog.Errorf("Free host count (%d) is less than or equal to requested host count (%d)", freeHostNum, requestedHostCount)
		return nil
	}
	if vs.parentVirtualNodeStore != nil {
		klog.Error("Request free host can only be done from parent node")
		return nil
	}

	if len(vs.splittVirtualNodeStores) == 0 { // first splitted vs
		// create new virtual node store
		newVSStore := &VirtualNodeStore{
			mu:                     sync.RWMutex{},
			nodeEventByHash:        make(map[float64]*node.ManagedNodeEvent, freeHostNum-requestedHostCount),
			location:               vs.location,
			parentVirtualNodeStore: vs,
			adjustedLowerBound:     vs.lowerbound,
			adjustedUpperBound:     vs.upperbound,
		}
		vs.adjustedLowerBound = vs.lowerbound
		vs.adjustedUpperBound = vs.upperbound
		vs.moveNodes(vs, newVSStore, freeHostNum-requestedHostCount, true)

		// Add new virtual node into store
		vs.splittVirtualNodeStores = make([]*VirtualNodeStore, 2)
		vs.splittVirtualNodeStores[0] = vs
		vs.splittVirtualNodeStores[1] = newVSStore
		return []*VirtualNodeStore{vs}
	} else {
		freeStores := vs.GetAllFreeChildStores()
		candidateStores := make([]*VirtualNodeStore, 0)
		count := 0
		for i := 0; i < len(freeStores); i++ {
			newHostCount := count + freeStores[i].GetHostNum()
			if newHostCount == requestedHostCount {
				return append(candidateStores, freeStores[i])
			} else if newHostCount < requestedHostCount {
				candidateStores = append(candidateStores, freeStores[i])
			} else {
				newVSStore := &VirtualNodeStore{
					mu:                     sync.RWMutex{},
					nodeEventByHash:        make(map[float64]*node.ManagedNodeEvent, newHostCount-requestedHostCount),
					location:               vs.location,
					parentVirtualNodeStore: vs,
					adjustedLowerBound:     freeStores[i].adjustedLowerBound,
					adjustedUpperBound:     freeStores[i].adjustedUpperBound,
				}
				vs.moveNodes(freeStores[i], newVSStore, newHostCount-requestedHostCount, true)

				vs.splittVirtualNodeStores = append(vs.splittVirtualNodeStores, newVSStore)
				sort.Sort(vs)
				return append(candidateStores, freeStores[i])
			}
		}

		// this should never be reached
		klog.Error("Unexpected statement reach. Free host #: %v, requested host #: %v", freeHostNum, requestedHostCount)
		return candidateStores
	}
}

func (vs *VirtualNodeStore) findIndexOfVS(vsToLocate *VirtualNodeStore) int {
	for i := 0; i < len(vs.splittVirtualNodeStores); i++ {
		if vs.splittVirtualNodeStores[i] == vsToLocate {
			return i
		}
	}
	return -1
}

// Return:
// 1. virtualNodeStore that is free and can move nodes to, if there is no, create a new one
// 2. False if adjancent VS is to the left of store[index], true if adjancent VS is to the right of the store[index]
func (vs *VirtualNodeStore) findAdjacentVacantStore(index int) (*VirtualNodeStore, bool) {
	if index == 0 { // leftmost
		if vs.splittVirtualNodeStores[1].clientId == "" { // assigned
			return vs.splittVirtualNodeStores[1], true
		}
	} else if index == len(vs.splittVirtualNodeStores)-1 { // rightmost
		if vs.splittVirtualNodeStores[index-1].clientId == "" {
			return vs.splittVirtualNodeStores[index-1], false
		}
	} else {
		if vs.splittVirtualNodeStores[index-1].clientId == "" {
			return vs.splittVirtualNodeStores[index-1], false
		} else if vs.splittVirtualNodeStores[index+1].clientId == "" {
			return vs.splittVirtualNodeStores[index+1], true
		}
	}

	// create new store
	newVSStore := &VirtualNodeStore{
		mu:                     sync.RWMutex{},
		nodeEventByHash:        make(map[float64]*node.ManagedNodeEvent),
		location:               vs.location,
		parentVirtualNodeStore: vs,
		lowerbound:             -1,
		upperbound:             -1,
	}
	vs.splittVirtualNodeStores = append(vs.splittVirtualNodeStores, newVSStore)
	return newVSStore, true
}

// Move #hostCount from sourceVNS to dstVNS
// If countFromHighEnd == true, choose hash value from biggest (move to right store)
// If countFromHighEnd == false, choose hash value from smallest (move to left store)
// Internal function, assume input is valid, no need to validate
func (vs *VirtualNodeStore) moveNodes(sourceVNS, dstVNS *VirtualNodeStore, hostCountToMove int, countFromHighEnd bool) error {
	// optimization
	if len(dstVNS.nodeEventByHash) == 0 && len(sourceVNS.nodeEventByHash) < hostCountToMove*2 {
		switchVirtualNodeStore(sourceVNS, dstVNS)
		sourceVNS, dstVNS = dstVNS, sourceVNS
		hostCountToMove = len(sourceVNS.nodeEventByHash) - hostCountToMove
		countFromHighEnd = !countFromHighEnd
	}

	// sort nodes in sourceVNS by hash value
	nodeHashes := make([]float64, len(sourceVNS.nodeEventByHash))
	index := 0
	for k, _ := range sourceVNS.nodeEventByHash {
		nodeHashes[index] = k
		index++
	}
	sort.Float64s(nodeHashes)

	// starting position and bound adjustment
	start := 0
	if countFromHighEnd {
		start = index - 1
		sourceVNS.adjustedUpperBound = nodeHashes[len(sourceVNS.nodeEventByHash)-hostCountToMove]
		dstVNS.adjustedLowerBound = sourceVNS.adjustedUpperBound
	} else {
		sourceVNS.adjustedLowerBound = nodeHashes[hostCountToMove]
		dstVNS.adjustedUpperBound = sourceVNS.adjustedLowerBound
	}
	count := 0

	// move nodes
	for {
		nodeHashKey := nodeHashes[start]
		dstVNS.nodeEventByHash[nodeHashKey] = sourceVNS.nodeEventByHash[nodeHashKey]
		delete(sourceVNS.nodeEventByHash, nodeHashKey)
		count++

		if count < hostCountToMove {
			if countFromHighEnd {
				start--
			} else {
				start++
			}
		} else {
			break
		}
	}

	return nil
}

// This function switch all elements of store1 and store2
// No need to switch location - it should always be the same (otherwise, nodes cannot be switched)
func switchVirtualNodeStore(store1, store2 *VirtualNodeStore) {
	// lock cannot be switched
	//store1.mu, store2.mu = store2.mu, store1.mu
	store1.nodeEventByHash, store2.nodeEventByHash = store2.nodeEventByHash, store1.nodeEventByHash

	// lower and upper bounds are immutable
	//store1.lowerbound, store2.lowerbound = store2.lowerbound, store1.lowerbound
	//store1.upperbound, store2.upperbound = store2.upperbound, store1.upperbound
	store1.adjustedLowerBound, store2.adjustedLowerBound = store2.adjustedLowerBound, store1.adjustedLowerBound
	store1.adjustedUpperBound, store2.adjustedUpperBound = store2.adjustedUpperBound, store1.adjustedUpperBound

	store1.splittVirtualNodeStores, store2.splittVirtualNodeStores = store2.splittVirtualNodeStores, store1.splittVirtualNodeStores
	// parent stores CANNOT be switched
	//store1.parentVirtualNodeStore, store2.parentVirtualNodeStore = store2.parentVirtualNodeStore, store1.parentVirtualNodeStore

	if store1.clientId != store2.clientId {
		store1.clientId, store2.clientId = store2.clientId, store1.clientId
	}
	if store1.eventQueue != store2.eventQueue {
		store1.eventQueue, store2.eventQueue = store2.eventQueue, store1.eventQueue
	}
}

// Implement sort.Interface
func (vs *VirtualNodeStore) Len() int {
	return len(vs.splittVirtualNodeStores)
}

func (vs *VirtualNodeStore) Less(i, j int) bool {
	return vs.splittVirtualNodeStores[i].adjustedLowerBound < vs.splittVirtualNodeStores[j].adjustedLowerBound
}

func (vs *VirtualNodeStore) Swap(i, j int) {
	vs.splittVirtualNodeStores[i], vs.splittVirtualNodeStores[j] = vs.splittVirtualNodeStores[j], vs.splittVirtualNodeStores[i]
}

func (vs *VirtualNodeStore) GetBoundaris() string {
	vs.mu.RLock()
	defer vs.mu.RUnlock()

	result := new(bytes.Buffer)
	result.WriteString(fmt.Sprintf("original boundary (%v, %v), adjusted boundary (%v, %v)\n",
		vs.lowerbound, vs.upperbound, vs.adjustedLowerBound, vs.adjustedUpperBound))
	for i := 0; i < len(vs.splittVirtualNodeStores); i++ {
		result.WriteString(fmt.Sprintf("Child store %d, boundary (%v, %v)\n",
			i, vs.splittVirtualNodeStores[i].adjustedLowerBound, vs.splittVirtualNodeStores[i].adjustedUpperBound))
	}

	return string(result.Bytes())
}

type NodeStore struct {
	// granularity of the ring - degree for each virtual node managed arc
	granularOfRing float64

	// # of regions
	regionNum int

	// # of max resource partition in each region
	partitionMaxNum int

	// # of different resource slots - computation various
	resourceSlots int

	virtualNodeNum int
	// node stores by virtual nodes
	// Using map instead of array to avoid expanding cost
	vNodeStores *[]*VirtualNodeStore
	// mutex for virtual store number/size adjustment
	nsLock sync.RWMutex

	totalHostNum int
	hostNumLock  sync.RWMutex

	// Latest resource version map
	currentRVs [][]uint64
	rvLock     sync.RWMutex
}

func NewNodeStore(vNodeNumPerRP int, regionNum int, partitionMaxNum int) *NodeStore {
	klog.V(3).Infof("Initialize node store with virtual node per RP: %d\n", vNodeNumPerRP)

	totalVirtualNodeNum := vNodeNumPerRP * regionNum * partitionMaxNum
	virtualNodeStores := make([]*VirtualNodeStore, totalVirtualNodeNum)

	rvArray := make([][]uint64, regionNum)
	for i := 0; i < regionNum; i++ {
		rvArray[i] = make([]uint64, partitionMaxNum)
	}

	ns := &NodeStore{
		virtualNodeNum:  totalVirtualNodeNum,
		vNodeStores:     &virtualNodeStores,
		granularOfRing:  location.RingRange / (float64(totalVirtualNodeNum)),
		regionNum:       regionNum,
		partitionMaxNum: partitionMaxNum,
		resourceSlots:   regionNum * partitionMaxNum,
		currentRVs:      rvArray,
		totalHostNum:    0,
	}

	ns.generateVirtualNodeStores(vNodeNumPerRP)
	return ns
}

// TODO - verify whether the original value can be changed. If so, return a deepcopy
func (ns *NodeStore) GetCurrentResourceVersions() types.TransitResourceVersionMap {
	ns.rvLock.RLock()
	defer ns.rvLock.RUnlock()
	rvMap := make(types.TransitResourceVersionMap)
	for i := 0; i < ns.regionNum; i++ {
		for j := 0; j < ns.partitionMaxNum; j++ {
			if ns.currentRVs[i][j] > 0 {
				rvMap[types.RvLocation{Region: location.Regions[i], Partition: location.ResourcePartitions[j]}] = ns.currentRVs[i][j]
			}
		}
	}
	return rvMap
}

func (ns *NodeStore) GetTotalHostNum() int {
	ns.hostNumLock.RLock()
	defer ns.hostNumLock.RUnlock()
	return ns.totalHostNum
}

func (ns *NodeStore) CheckFreeCapacity(requestedHostNum int) bool {
	ns.nsLock.Lock()
	defer ns.nsLock.Unlock()
	allocatableHostNum := 0
	for _, vs := range *ns.vNodeStores {
		allocatableHostNum += vs.GetFreeHostNum()
		if allocatableHostNum >= requestedHostNum {
			return true
		}
	}

	return false
}

func (ns *NodeStore) GetVirtualStores() *[]*VirtualNodeStore {
	return ns.vNodeStores
}

func (ns *NodeStore) generateVirtualNodeStores(vNodeNumPerRP int) {
	ns.nsLock.Lock()
	defer ns.nsLock.Unlock()

	vNodeIndex := 0
	for k := 0; k < ns.regionNum; k++ {
		region := location.Regions[k]
		rpsInRegion := location.GetRPsForRegion(region)

		for m := 0; m < ns.partitionMaxNum; m++ {
			loc := location.NewLocation(region, rpsInRegion[m])
			lowerBound, upperBound := loc.GetArcRangeFromLocation()

			for i := 0; i < vNodeNumPerRP; i++ {

				(*ns.vNodeStores)[vNodeIndex] = &VirtualNodeStore{
					mu:              sync.RWMutex{},
					nodeEventByHash: make(map[float64]*node.ManagedNodeEvent, VirtualStoreInitSize),
					lowerbound:      lowerBound,
					upperbound:      lowerBound + ns.granularOfRing,
					location:        *loc,
				}
				lowerBound += ns.granularOfRing
				vNodeIndex++
			}

			// remove the impact of inaccuracy
			(*ns.vNodeStores)[vNodeIndex-1].upperbound = upperBound
		}
	}

	(*ns.vNodeStores)[ns.virtualNodeNum-1].upperbound = location.RingRange
}

func (ns *NodeStore) CreateNode(nodeEvent *node.ManagedNodeEvent) {
	isNewNode := ns.addNodeToRing(nodeEvent)
	if !isNewNode {
		ns.updateNodeInRing(nodeEvent)
	}
}

func (ns *NodeStore) UpdateNode(nodeEvent *node.ManagedNodeEvent) {
	ns.updateNodeInRing(nodeEvent)
}

// TODO
func (ns NodeStore) DeleteNode(nodeEvent runtime.NodeEvent) {
}

func (ns NodeStore) GetNode(region location.Region, resourcePartition location.ResourcePartition, nodeId string) (*types.LogicalNode, error) {
	n := &types.LogicalNode{Id: nodeId}
	ne := runtime.NewNodeEvent(n, runtime.Bookmark)

	loc := location.NewLocation(location.Region(region), location.ResourcePartition((resourcePartition)))
	mgmtNE := node.NewManagedNodeEvent(ne, loc)

	hashValue, _, vNodeStore := ns.getVirtualNodeStore(mgmtNE)
	if oldNode, isOK := vNodeStore.nodeEventByHash[hashValue]; isOK {
		return oldNode.CopyNode(), nil
	} else {
		return nil, types.Error_ObjectNotFound
	}
}

func (ns *NodeStore) ProcessNodeEvents(nodeEvents []*node.ManagedNodeEvent, persistHelper *DistributorPersistHelper) (bool, types.TransitResourceVersionMap) {
	persistHelper.SetWaitCount(len(nodeEvents))

	eventsToPersist := make([]*types.LogicalNode, BatchPersistSize)
	i := 0
	for _, e := range nodeEvents {
		if e == nil {
			persistHelper.persistNodeWaitGroup.Done()
			continue
		}
		ns.processNodeEvent(e)
		eventsToPersist[i] = e.GetNodeEvent().Node
		i++
		if i == BatchPersistSize {
			persistHelper.PersistNodes(eventsToPersist)
			i = 0
			eventsToPersist = make([]*types.LogicalNode, BatchPersistSize)
		}
	}
	if i > 0 {
		remainingEventsToPersist := eventsToPersist[0:i]
		persistHelper.PersistNodes(remainingEventsToPersist)
	}

	// persist disk
	result := persistHelper.PersistStoreConfigs(ns.getNodeStoreStatus())
	if !result {
		// TODO
	}

	// TODO - make a copy of currentRVs in case modification happen unexpectedly
	return true, ns.GetCurrentResourceVersions()
}

func (ns *NodeStore) processNodeEvent(nodeEvent *node.ManagedNodeEvent) bool {
	switch nodeEvent.GetEventType() {
	case runtime.Added:
		ns.CreateNode(nodeEvent)
	case runtime.Modified:
		ns.UpdateNode(nodeEvent)
	default:
		// TODO - action needs to take when non acceptable events happened
		klog.Warningf("Invalid event type [%v] for node %v, location %v, rv %v",
			nodeEvent.GetNodeEvent(), nodeEvent.GetId(), nodeEvent.GetRvLocation(), nodeEvent.GetResourceVersionInt64())
		return false
	}

	// Update ResourceVersionMap
	newRV := nodeEvent.GetResourceVersionInt64()
	ns.rvLock.Lock()
	region := nodeEvent.GetLocation().GetRegion()
	resourcePartition := nodeEvent.GetLocation().GetResourcePartition()
	if ns.currentRVs[region][resourcePartition] < newRV {
		ns.currentRVs[region][resourcePartition] = newRV
	}
	ns.rvLock.Unlock()

	return true
}

// return location on the ring, and ring Id
// ring Id is reserved for multiple rings
func (ns *NodeStore) getNodeHash(node *node.ManagedNodeEvent) (float64, int) {
	// map node id to uint32
	initHashValue := hash.HashStrToUInt64(node.GetId())

	// map node id to hash ring: (0 - 1]
	var ringValue float64
	if initHashValue == 0 {
		ringValue = 1
	} else {
		ringValue = float64(initHashValue) / float64(math.MaxUint64)
	}

	// compact to ring slice where this location belongs to
	lower, upper := node.GetLocation().GetArcRangeFromLocation()

	// compact ringValue onto (lower, upper]
	return lower + ringValue*(upper-lower), 0
}

func (ns *NodeStore) getVirtualNodeStore(node *node.ManagedNodeEvent) (float64, int, *VirtualNodeStore) {
	hashValue, ringId := ns.getNodeHash(node)
	virtualNodeIndex := int(math.Floor(hashValue / ns.granularOfRing))
	parentVNS := (*ns.vNodeStores)[virtualNodeIndex]
	if len(parentVNS.splittVirtualNodeStores) > 0 && (hashValue < parentVNS.adjustedLowerBound || hashValue >= parentVNS.adjustedUpperBound) {
		// prevent node splitting happening during search
		parentVNS.mu.RLock()
		defer parentVNS.mu.RUnlock()
		// node shall locate in child VirtualNodeStore
		// linear search first, might need to change to binary search based on performance
		for i := 0; i < len(parentVNS.splittVirtualNodeStores); i++ {
			if hashValue >= parentVNS.splittVirtualNodeStores[i].adjustedLowerBound && hashValue < parentVNS.splittVirtualNodeStores[i].adjustedUpperBound {
				return hashValue, ringId, parentVNS.splittVirtualNodeStores[i]
			}
		}

		klog.Errorf("Could not find node position in virtual node stores. HashValue %v, VS %s", hashValue, parentVNS.GetBoundaris())
	}

	return hashValue, ringId, parentVNS
}

func (ns *NodeStore) addNodeToRing(nodeEvent *node.ManagedNodeEvent) (isNewNode bool) {
	hashValue, _, vNodeStore := ns.getVirtualNodeStore(nodeEvent)
	// add event to event queue
	// During list snapshot, eventQueue will be locked first and virtual node stores will be locked later
	// Keep the locking sequence here to prevent deadlock
	if vNodeStore.eventQueue != nil {
		vNodeStore.eventQueue.EnqueueEvent(nodeEvent)
	}

	vNodeStore.mu.Lock()
	defer vNodeStore.mu.Unlock()

	if oldNode, isOK := vNodeStore.nodeEventByHash[hashValue]; isOK {
		if oldNode.GetId() != nodeEvent.GetId() {
			klog.V(3).Infof("Found existing node (uuid %s) with same hash value %f. New node (uuid %s)\n", oldNode.GetId(), hashValue, nodeEvent.GetId())
			// TODO - put node into linked list
		} else {
			return false
		}
	}
	vNodeStore.nodeEventByHash[hashValue] = nodeEvent

	ns.hostNumLock.Lock()
	ns.totalHostNum++
	ns.hostNumLock.Unlock()

	return true
}

func (ns *NodeStore) updateNodeInRing(nodeEvent *node.ManagedNodeEvent) {
	hashValue, _, vNodeStore := ns.getVirtualNodeStore(nodeEvent)
	// add event to event queue
	// During list snapshot, eventQueue will be locked first and virtual node stores will be locked later
	// Keep the locking sequence here to prevent deadlock
	if vNodeStore.eventQueue != nil {
		vNodeStore.eventQueue.EnqueueEvent(nodeEvent)
	}

	vNodeStore.mu.Lock()
	if oldNode, isOK := vNodeStore.nodeEventByHash[hashValue]; isOK {
		// TODO - check uuid to make sure updating right node
		if oldNode.GetId() == nodeEvent.GetId() {
			if oldNode.GetResourceVersionInt64() < nodeEvent.GetResourceVersionInt64() {
				vNodeStore.nodeEventByHash[hashValue] = nodeEvent
			} else {
				klog.V(3).Infof("Discard node update events due to resource version is older: %d. Existing rv %d",
					nodeEvent.GetResourceVersionInt64(), oldNode.GetResourceVersionInt64())
				vNodeStore.mu.Unlock()
				return
			}
		} else {
			// TODO - check linked list to get right
			klog.V(3).Infof("Updating node got same hash value (%f) but different node id: (%s and %s)", hashValue,
				oldNode.GetId(), nodeEvent.GetId())
		}

		vNodeStore.mu.Unlock()
	} else {
		// ?? - report error or not?
		vNodeStore.mu.Unlock()
		ns.addNodeToRing(nodeEvent)
	}
}

func (ns *NodeStore) getNodeStoreStatus() *store.NodeStoreStatus {
	return &store.NodeStoreStatus{
		RegionNum:              ns.regionNum,
		PartitionMaxNum:        ns.partitionMaxNum,
		VirtualNodeNumPerRP:    ns.virtualNodeNum / (ns.regionNum * ns.partitionMaxNum),
		CurrentResourceVerions: ns.GetCurrentResourceVersions(),
	}
}
