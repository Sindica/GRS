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
	"k8s.io/klog/v2"
	"math"
	"sort"
	"sync"

	"global-resource-service/resource-management/pkg/common-lib/hash"
	"global-resource-service/resource-management/pkg/common-lib/interfaces/store"
	"global-resource-service/resource-management/pkg/common-lib/types"
	"global-resource-service/resource-management/pkg/common-lib/types/event"
	"global-resource-service/resource-management/pkg/common-lib/types/location"
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
	// lower bound for current VS - inclusive - immutable for top level VS
	lowerbound float64
	// upper bound for current VS - exclusive - immutable for top level VS
	upperbound float64
	// effective upper bound for top level VS - same for child VS
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

func (vs *VirtualNodeStore) GetLocation() location.Location {
	return vs.location
}

func (vs *VirtualNodeStore) GetAssignedClient() string {
	return vs.clientId
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

func (vs *VirtualNodeStore) GetRange() (float64, float64) {
	return vs.lowerbound, vs.upperbound
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
		newRV := node.GetResourceVersion()
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
		nodeEvent := event.NewNodeEvent(logicalNode, event.Bookmark)
		return node.NewManagedNodeEvent(nodeEvent, n.GetLocation())
	}
	return nil
}

// Input requested host count
func (vs *VirtualNodeStore) AdjustCapacity(requestedHostCount int) {
	vs.mu.Lock()
	defer vs.mu.Unlock()
	currentSize := len(vs.nodeEventByHash)
	if currentSize <= requestedHostCount {
		klog.Errorf("Requested to split virtual node when capacity (%d) is less than or equal to requested host count (%d)", currentSize, requestedHostCount)
		return
	}

	// first splitted vs
	if vs.parentVirtualNodeStore == nil && len(vs.splittVirtualNodeStores) == 0 {
		nodeHashes := make([]float64, currentSize)
		index := 0
		for k, _ := range vs.nodeEventByHash {
			nodeHashes[index] = k
			index++
		}
		sort.Float64s(nodeHashes)
		newUBoundForCurrentVS := nodeHashes[requestedHostCount]
		vs.adjustedUpperBound = newUBoundForCurrentVS

		// create new virtual node store
		newVSStore := &VirtualNodeStore{
			mu:              sync.RWMutex{},
			nodeEventByHash: make(map[float64]*node.ManagedNodeEvent, currentSize-requestedHostCount),
			lowerbound:      newUBoundForCurrentVS,
			upperbound:      vs.upperbound,
			location:        vs.location,
		}

		// move nodes from existing vs to new vs
		for i := requestedHostCount; i < currentSize; i++ {
			nodeHashKey := nodeHashes[i]
			newVSStore.nodeEventByHash[nodeHashKey] = vs.nodeEventByHash[nodeHashKey]
			delete(vs.nodeEventByHash, nodeHashKey)
		}

		// Add new virtual node into store
	}
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
		allocatableHostNum += vs.GetHostNum()
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
func (ns NodeStore) DeleteNode(nodeEvent event.NodeEvent) {
}

func (ns NodeStore) GetNode(region location.Region, resourcePartition location.ResourcePartition, nodeId string) (*types.LogicalNode, error) {
	n := &types.LogicalNode{Id: nodeId}
	ne := event.NewNodeEvent(n, event.Bookmark)

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
	case event.Added:
		ns.CreateNode(nodeEvent)
	case event.Modified:
		ns.UpdateNode(nodeEvent)
	default:
		// TODO - action needs to take when non acceptable events happened
		klog.Warningf("Invalid event type [%v] for node %v, location %v, rv %v",
			nodeEvent.GetNodeEvent(), nodeEvent.GetId(), nodeEvent.GetRvLocation(), nodeEvent.GetResourceVersion())
		return false
	}

	// Update ResourceVersionMap
	newRV := nodeEvent.GetResourceVersion()
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
	return hashValue, ringId, (*ns.vNodeStores)[virtualNodeIndex]
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
			if oldNode.GetResourceVersion() < nodeEvent.GetResourceVersion() {
				vNodeStore.nodeEventByHash[hashValue] = nodeEvent
			} else {
				klog.V(3).Infof("Discard node update events due to resource version is older: %d. Existing rv %d", nodeEvent.GetResourceVersion(), oldNode.GetResourceVersion())
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
