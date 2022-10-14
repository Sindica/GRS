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

package distributor

import (
	"errors"
	"fmt"
	"k8s.io/klog/v2"
	"sync"

	"global-resource-service/resource-management/pkg/common-lib/interfaces/store"
	"global-resource-service/resource-management/pkg/common-lib/metrics"
	"global-resource-service/resource-management/pkg/common-lib/types"
	"global-resource-service/resource-management/pkg/common-lib/types/location"
	"global-resource-service/resource-management/pkg/common-lib/types/runtime"
	"global-resource-service/resource-management/pkg/distributor/cache"
	"global-resource-service/resource-management/pkg/distributor/node"
	"global-resource-service/resource-management/pkg/distributor/storage"
)

type ResourceDistributor struct {
	defaultNodeStore *storage.NodeStore

	// clientId to node event queue
	nodeEventQueueMap map[string]*cache.NodeEventQueue

	// clientId to virtual node store map
	clientToStores map[string][]*storage.VirtualNodeStore
	allocateLock   sync.RWMutex

	persistHelper store.StoreInterface
}

var _distributor *ResourceDistributor = nil
var once sync.Once

const (
	MinimalRequestHostNum = 50
)

var virutalStoreNumPerResourcePartition = 1000 // 50K per resource partition, 50 hosts per virtual node store

func GetResourceDistributor() *ResourceDistributor {
	once.Do(func() {
		_distributor = &ResourceDistributor{
			defaultNodeStore:  createNodeStore(),
			nodeEventQueueMap: make(map[string]*cache.NodeEventQueue),
			clientToStores:    make(map[string][]*storage.VirtualNodeStore),
		}
	})
	return _distributor
}

func (dis *ResourceDistributor) SetPersistHelper(persistTool store.StoreInterface) {
	dis.persistHelper = persistTool
}

// TODO - get virtual node number, region num, partition num from external
func createNodeStore() *storage.NodeStore {
	return storage.NewNodeStore(virutalStoreNumPerResourcePartition, location.GetRegionNum(), location.GetRPNum())
}

// TODO: post 630, allocate resources per request for different type of hardware and regions
func (dis *ResourceDistributor) RegisterClient(client *types.Client) error {
	clientId := client.ClientId
	assignedHostNum, err := dis.allocateNodesToClient(clientId, client.Resource.TotalMachines)
	if err != nil {
		klog.Errorf("Error allocate resource for client. Error %v\n", err)
		return err
	}

	err = dis.persistHelper.PersistClient(clientId, client)
	if err != nil {
		klog.Errorf("Error persistent client to store. Error %v\n", err)
		return err
	}

	klog.Infof("Registered client id: %s, requested host # = %d, assigned host # = %d\n", clientId, client.Resource.TotalMachines, assignedHostNum)
	return nil
}

func (dis *ResourceDistributor) allocateNodesToClient(clientId string, requestedHostNum int) (int, error) {
	dis.allocateLock.Lock()
	defer dis.allocateLock.Unlock()
	if requestedHostNum <= MinimalRequestHostNum {
		return 0, types.Error_HostRequestLessThanMiniaml
	} else if requestedHostNum > dis.defaultNodeStore.GetTotalHostNum() {
		return 0, types.Error_HostRequestExceedLimit
	} else if !dis.defaultNodeStore.CheckFreeCapacity(requestedHostNum) {
		return 0, types.Error_HostRequestExceedCapacity
	}

	// check client id existence
	if _, isOK := dis.nodeEventQueueMap[clientId]; isOK {
		return 0, types.Error_ClientIdExisted
	}
	if _, isOK := dis.clientToStores[clientId]; isOK {
		return 0, types.Error_ClientIdExisted
	}

	// allocate virtual nodes to client
	// get all virtual stores that are unassigned
	allStores := dis.defaultNodeStore.GetVirtualStores()
	freeStores := make(map[*storage.VirtualNodeStore]bool)
	for _, vs := range *allStores {
		if vs.GetFreeHostNum() > 0 {
			freeStores[vs] = true
		}
	}
	if len(freeStores) == 0 {
		return 0, errors.New("No available hosts")
	}

	// Get sorted virtual node stores based on ordering criteria
	storesToSelectInorder := dis.getSortedVirtualStores(freeStores)
	selectedStores := make([]*storage.VirtualNodeStore, 0)
	assignedHostCount := 0
	hostAssignIsOK := false
	for i := 0; i < len(storesToSelectInorder); i++ {
		candidateStore := storesToSelectInorder[i]

		newHostCount := assignedHostCount + candidateStore.GetFreeHostNum()
		if newHostCount <= requestedHostNum {
			selectedStores = append(selectedStores, candidateStore.GetAllFreeChildStores()...)
			assignedHostCount = newHostCount
			if newHostCount == requestedHostNum {
				hostAssignIsOK = true
				break
			}
		} else { // split vNode
			requestedStores := candidateStore.RequestCapacity(requestedHostNum - assignedHostCount)
			selectedStores = append(selectedStores, requestedStores...)
			assignedHostCount += candidateStore.GetHostNum()
			hostAssignIsOK = true
			break
		}
	}
	if !hostAssignIsOK {
		return 0, errors.New("Not enough hosts")
	}

	// Create event queue for client
	eventQueue := cache.NewNodeEventQueue(clientId)
	dis.nodeEventQueueMap[clientId] = eventQueue
	dis.addBookmarkEvent(selectedStores, eventQueue)

	// Assign virtual node stores to client
	for _, store := range selectedStores {
		store.AssignToClient(clientId, eventQueue)
	}
	dis.clientToStores[clientId] = selectedStores

	// persist virtual node assignment
	dis.persistVirtualNodesAssignment(clientId, selectedStores)

	return assignedHostCount, nil
}

func (dis *ResourceDistributor) addBookmarkEvent(stores []*storage.VirtualNodeStore, eventQueue *cache.NodeEventQueue) {
	locations := make(map[location.Location]bool)

	for _, store := range stores {
		loc := store.GetLocation()
		if _, isOK := locations[loc]; !isOK {
			locations[loc] = true

			eventQueue.EnqueueEvent(store.GenerateBookmarkEvent())
		}
	}
}

// TODO: sort virtual node stores based on ordering criteria
// Do not sort by host number since this can lead to over assignment more and more
func (dis *ResourceDistributor) getSortedVirtualStores(stores map[*storage.VirtualNodeStore]bool) []*storage.VirtualNodeStore {
	sortedStores := make([]*storage.VirtualNodeStore, len(stores))
	index := 0
	for vs, isOK := range stores {
		if isOK {
			sortedStores[index] = vs
			index++
		}
	}

	return sortedStores
}

// ListNodesForClient returns list of nodes for a client request and a RV map to the client for WATCH
//                            or error encountered during the node allocation/listing for the client
func (dis *ResourceDistributor) ListNodesForClient(clientId string) ([]*types.LogicalNode, types.TransitResourceVersionMap, error) {
	if clientId == "" {
		return nil, nil, errors.New("Empty clientId")
	}
	dis.allocateLock.RLock()
	assignedStores, isOK := dis.clientToStores[clientId]
	dis.allocateLock.RUnlock()
	if !isOK {
		return nil, nil, errors.New(fmt.Sprintf("Client %s not registered.", clientId))
	}
	eventQueue, isOK := dis.nodeEventQueueMap[clientId]
	if !isOK {
		return nil, nil, errors.New(fmt.Sprintf("Internal error: missing event queue for Client %s.", clientId))
	}

	eventQueue.AcquireSnapshotRLock()
	nodesByStore := make([][]*types.LogicalNode, len(assignedStores))
	rvMapByStore := make([]types.TransitResourceVersionMap, len(assignedStores))
	hostCount := 0
	for i := 0; i < len(assignedStores); i++ {
		nodesByStore[i], rvMapByStore[i] = assignedStores[i].SnapShot()
		hostCount += len(nodesByStore[i])
	}
	eventQueue.ReleaseSnapshotRLock()

	// combine to single array of nodeEvent
	nodes := make([]*types.LogicalNode, hostCount)
	index := 0
	for i := 0; i < len(nodesByStore); i++ {
		for j := 0; j < len(nodesByStore[i]); j++ {
			nodes[index] = nodesByStore[i][j]
			index++
		}
	}

	// combine to single ResourceVersionMap
	finalRVs := make(types.TransitResourceVersionMap)
	for i := 0; i < len(rvMapByStore); i++ {
		currentRVs := rvMapByStore[i]
		for loc, rv := range currentRVs {
			if oldRV, isOK := finalRVs[loc]; isOK {
				if oldRV < rv {
					finalRVs[loc] = rv
				}
			} else {
				finalRVs[loc] = rv
			}
		}
	}

	return nodes, finalRVs, nil
}

func (dis *ResourceDistributor) Watch(clientId string, rvs types.TransitResourceVersionMap, watchChan chan runtime.Object, stopCh chan struct{}) error {
	var nodeEventQueue *cache.NodeEventQueue
	var isOK bool
	if nodeEventQueue, isOK = dis.nodeEventQueueMap[clientId]; !isOK || nodeEventQueue == nil {
		return errors.New(fmt.Sprintf("Client %s not registered", clientId))
	}
	if rvs == nil {
		return errors.New("Invalid resource versions: nil")
	}
	if watchChan == nil {
		return errors.New("Watch channel not provided")
	}
	if stopCh == nil {
		return errors.New("Stop watch channel not provided")
	}

	internal_rvs := types.ConvertToInternalResourceVersionMap(rvs)

	return nodeEventQueue.Watch(internal_rvs, watchChan, stopCh)
}

func (dis *ResourceDistributor) ProcessEvents(events []*runtime.NodeEvent) (bool, types.TransitResourceVersionMap) {
	eventsToProcess := make([]*node.ManagedNodeEvent, len(events))
	for i := 0; i < len(events); i++ {
		if events[i] != nil {
			loc := location.NewLocation(location.Region(events[i].Node.GeoInfo.Region), location.ResourcePartition(events[i].Node.GeoInfo.ResourcePartition))
			events[i].SetCheckpoint(int(metrics.Distributor_Received))
			if loc != nil {
				eventsToProcess[i] = node.NewManagedNodeEvent(events[i], loc)
			} else {
				klog.Errorf("Invalid region %v and/or resource partition %v\n", events[i].Node.GeoInfo.Region, events[i].Node.GeoInfo.ResourcePartition)
			}
		} else {
			break
		}
	}

	persistHelper := storage.NewDistributorPersistHelper(dis.persistHelper)
	result, rvMap := dis.defaultNodeStore.ProcessNodeEvents(eventsToProcess, persistHelper)
	persistHelper.WaitForAllNodesSaved()
	return result, rvMap
}

func (dis *ResourceDistributor) persistVirtualNodesAssignment(clientId string, assignedStores []*storage.VirtualNodeStore) bool {
	vNodeConfigs := make([]*store.VirtualNodeConfig, len(assignedStores))
	for i, s := range assignedStores {
		vNodeToSave := &store.VirtualNodeConfig{
			Location: s.GetLocation(),
		}
		vNodeToSave.Lowerbound, vNodeToSave.Upperbound = s.GetOriginalRange()
		vNodeConfigs[i] = vNodeToSave
	}
	assignment := &store.VirtualNodeAssignment{
		ClientId:     clientId,
		VirtualNodes: vNodeConfigs,
	}
	result := storage.NewDistributorPersistHelper(dis.persistHelper).PersistVirtualNodesAssignment(assignment)
	if !result {
		// TODO
	}
	return result
}

func (dis *ResourceDistributor) GetNodeStatus(region location.Region, resourcePartition location.ResourcePartition, nodeId string) (*types.LogicalNode, error) {
	return dis.defaultNodeStore.GetNode(region, resourcePartition, nodeId)
}
