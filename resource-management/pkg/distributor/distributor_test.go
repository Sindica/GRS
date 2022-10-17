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
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"k8s.io/klog/v2"
	"strconv"
	"sync"
	"testing"
	"time"

	"global-resource-service/resource-management/pkg/common-lib/types"
	"global-resource-service/resource-management/pkg/common-lib/types/location"
	"global-resource-service/resource-management/pkg/common-lib/types/runtime"
	"global-resource-service/resource-management/pkg/distributor/cache"
	"global-resource-service/resource-management/pkg/distributor/storage"
)

var existedNodeId = make(map[uuid.UUID]bool)
var rvToGenerate = 0

var singleTestLock = sync.Mutex{}

var defaultLocBeijing_RP1 = location.NewLocation(location.Beijing, location.ResourcePartition1)
var defaultRegion = location.Beijing
var defaultPartition = location.ResourcePartition1

const defaultVirtualStoreNumPerRP = 1000 // 50K per resource partition, 50 hosts per virtual node store

var fakeStorage = &storage.FakeStorageInterface{
	PersistDelayInNS: 20,
}

func setUp() *ResourceDistributor {
	singleTestLock.Lock()
	distributor := GetResourceDistributor()

	// flush node stores
	distributor.defaultNodeStore = createNodeStore()

	// flush nodeEventQueueMap
	distributor.nodeEventQueueMap = make(map[string]*cache.NodeEventQueue)

	// flush clientToStores map
	distributor.clientToStores = make(map[string][]*storage.VirtualNodeStore)

	// initialize persistent store
	distributor.SetPersistHelper(fakeStorage)

	return distributor
}

func tearDown() {
	virutalStoreNumPerResourcePartition = defaultVirtualStoreNumPerRP
	singleTestLock.Unlock()
}

func TestDistributorInit(t *testing.T) {
	distributor := setUp()
	defer tearDown()

	assert.NotNil(t, distributor, "Distributor cannot be nil")

	// check default virtual node stores
	defaultNodeStores := distributor.defaultNodeStore.GetVirtualStores()
	assert.Equal(t, true, len(*defaultNodeStores) > 500, "Expecting virtual store number >= 500")

	lower := float64(0)
	for i := 0; i < len(*defaultNodeStores); i++ {
		store := (*defaultNodeStores)[i]
		assert.Equal(t, 0, store.GetHostNum(), "Initial host number should be 0")
		assert.Equal(t, "", store.GetAssignedClient(), "Virtual store should not be assigned to any client")
		lowerBound, upperBound := store.GetOriginalRange()
		assert.Equal(t, lower, lowerBound, "Expecting lower bound %f but got %f. store id %d, hash range (%f, %f]", lower, lowerBound, i, lowerBound, upperBound)
		assert.NotEqual(t, lowerBound, upperBound, "Expecting lower bound not equal to upper bound for virtual store %d. Got hash range (%f, %f]", i, lowerBound, upperBound)
		lower = upperBound
		if i == len(*defaultNodeStores)-1 {
			assert.Equal(t, location.RingRange, upperBound, "Expecting last virtual store upper bound equals %f but got %f", location.RingRange, upperBound)
		}

		loc := store.GetLocation()
		assert.NotNil(t, loc, "Location of store should not be empty")
		if defaultLocBeijing_RP1.Equal(loc) {
			t.Logf("virtual node store %d, location %v, hash range (%f, %f]\n", i, store.GetLocation(), lowerBound, upperBound)
		}
	}
}

func measureProcessEvent(t *testing.T, dis *ResourceDistributor, eventType string, events []*runtime.NodeEvent, previousNodeCount int) {
	// get all node ids
	nodeIds := make(map[string]bool, len(events))
	eventCount := 0
	for i := 0; i < len(events); i++ {
		if events[i] != nil {
			nodeIds[events[i].Node.Id] = true
			eventCount++
		}
	}
	assert.Equal(t, len(events), eventCount)
	assert.Equal(t, len(events), len(nodeIds))

	dis.persistHelper.SetTestNodeIdMatch(true)
	dis.persistHelper.InitNodeIdCache()
	start := time.Now()
	result, rvMap := dis.ProcessEvents(events)
	duration := time.Since(start)
	t.Logf("Processing %d %s events took %v. Composite RVs %v\n", len(events), eventType, duration, rvMap)

	assert.True(t, result, "Expecting successfull event processing but got error")
	assert.NotNil(t, rvMap, "Expecting non nill rv map")
	assert.Equal(t, len(events)+previousNodeCount, dis.defaultNodeStore.GetTotalHostNum(), "Expected host number %d does not match actual host number %d", len(events), dis.defaultNodeStore.GetTotalHostNum())

	// iterate over virtual node stores
	hostCount := 0
	for _, vNodeStore := range *dis.defaultNodeStore.GetVirtualStores() {
		hostCount += vNodeStore.GetHostNum()
		assert.NotNil(t, vNodeStore.GetLocation())
	}
	assert.Equal(t, len(events)+previousNodeCount, hostCount, "Expected host number %d does not match actual host number %d", len(events), hostCount)

	// match node ids in events and fake storage
	assert.Equal(t, len(nodeIds), dis.persistHelper.GetNodeIdCount())
	dis.persistHelper.SetTestNodeIdMatch(false)
}

/*
RV using map - has lock:
Processing 10 AddNode events took 50.668µs.
Processing 100 AddNode events took 84.67µs.
Processing 1000 AddNode events took 838.216µs.
Processing 10000 AddNode events took 8.393787ms.
Processing 100000 AddNode events took 102.707352ms.
Processing 1000000 AddNode events took 1.184265289s.

RV using map - NO lock:
Processing 10 AddNode events took 35.453µs.
Processing 100 AddNode events took 80.803µs.
Processing 1000 AddNode events took 817.802µs.
Processing 10000 AddNode events took 7.555092ms.
Processing 100000 AddNode events took 91.526917ms.
Processing 1000000 AddNode events took 1.152776809s.

RV using array - NO lock, has lock on rv check
Processing 10 AddNode events took 34.957µs.
Processing 100 AddNode events took 63.625µs.
Processing 1000 AddNode events took 667.154µs.
Processing 10000 AddNode events took 5.899166ms.
Processing 100000 AddNode events took 77.327117ms.
Processing 1000000 AddNode events took 831.232514ms.

Updated to logical node
Processing 10 AddNode events took 51.792µs.
Processing 100 AddNode events took 87.546µs.
Processing 1000 AddNode events took 834.395µs.
Processing 10000 AddNode events took 7.914261ms.
Processing 100000 AddNode events took 106.144575ms.
Processing 1000000 AddNode events took 1.170175248s. - latency increased 40%, will improve later

. Added persistence
Processing 10 AddNode events took 78.813µs.
Processing 100 AddNode events took 722.073µs.
Processing 1000 AddNode events took 2.270763ms.
Processing 10000 AddNode events took 14.54155ms.
Processing 100000 AddNode events took 136.840846ms.
Processing 1000000 AddNode events took 2.077560132s.

. Batch persist nodes - 100 per batch - with checkpoints enabled
Processing 10 AddNode events took 118.371µs.
Processing 100 AddNode events took 253.852µs.
Processing 1000 AddNode events took 2.126007ms.
Processing 10000 AddNode events took 19.244113ms.
Processing 100000 AddNode events took 197.925013ms.
Processing 1000000 AddNode events took 2.268421774s.

. Batch persist nodes - 100 per batch - with checkpoints disabled
Processing 10 AddNode events took 88.647µs.
Processing 100 AddNode events took 90.774µs.
Processing 1000 AddNode events took 1.924601ms.
Processing 10000 AddNode events took 7.019417ms.
Processing 100000 AddNode events took 62.484907ms.
Processing 1000000 AddNode events took 867.098414ms.

. Updated metrics data - 9/22/2022 main - PR 169 - with checkpoints enabled
Processing 10 AddNode events took 86.209µs.
Processing 100 AddNode events took 369.59µs.
Processing 1000 AddNode events took 2.503705ms.
Processing 10000 AddNode events took 25.833047ms.
Processing 100000 AddNode events took 212.483275ms.
Processing 1000000 AddNode events took 2.78702993s.

. Updated metrics data - 9/22/2022 main - PR 169 - with checkpoints disabled
Processing 10 AddNode events took 136.435µs.
Processing 100 AddNode events took 294.906µs.
Processing 1000 AddNode events took 1.101831ms.
Processing 10000 AddNode events took 6.30712ms.
Processing 100000 AddNode events took 85.126281ms.
Processing 1000000 AddNode events took 1.423102935s.
*/
func TestAddNodes(t *testing.T) {
	distributor := setUp()
	defer tearDown()

	//common_lib.ResourceManagementMeasurement_Enabled = false
	nodeCounts := []int{10, 100, 1000, 10000, 100000, 1000000}
	previousNodeCount := 0
	for i := 0; i < len(nodeCounts); i++ {
		eventsAdd := generateAddNodeEvent(nodeCounts[i], defaultLocBeijing_RP1)
		measureProcessEvent(t, distributor, "AddNode", eventsAdd, previousNodeCount)
		previousNodeCount += nodeCounts[i]
	}
}

func generateAddNodeEvent(eventNum int, loc *location.Location) []*runtime.NodeEvent {
	result := make([]*runtime.NodeEvent, eventNum)
	for i := 0; i < eventNum; i++ {
		rvToGenerate += 1
		node := createRandomNode(rvToGenerate, loc)
		nodeEvent := runtime.NewNodeEvent(node, runtime.Added)
		result[i] = nodeEvent
	}
	return result
}

func generateAddNodeEventToArray(eventNum int, loc *location.Location, generatedEvents *[]*runtime.NodeEvent, startIndex int) {
	for i := 0; i < eventNum; i++ {
		rvToGenerate += 1
		node := createRandomNode(rvToGenerate, loc)
		nodeEvent := runtime.NewNodeEvent(node, runtime.Added)
		(*generatedEvents)[i+startIndex] = nodeEvent
	}
}

func generateUpdateNodeEvents(originalEvents []*runtime.NodeEvent) []*runtime.NodeEvent {
	result := make([]*runtime.NodeEvent, len(originalEvents))
	for i := 0; i < len(originalEvents); i++ {
		rvToGenerate += 1

		lNode := &types.LogicalNode{
			Id:              originalEvents[i].Node.Id,
			ResourceVersion: strconv.Itoa(rvToGenerate),
			GeoInfo: types.NodeGeoInfo{
				Region:            types.RegionName(defaultRegion),
				ResourcePartition: types.ResourcePartitionName(defaultPartition),
			},
		}

		newEvent := runtime.NewNodeEvent(lNode, runtime.Modified)
		result[i] = newEvent
	}
	return result
}

func generatedUpdateNodeEventsFromNodeList(nodes []*types.LogicalNode) []*runtime.NodeEvent {
	result := make([]*runtime.NodeEvent, len(nodes))
	for i := 0; i < len(nodes); i++ {
		rvToGenerate += 1
		node := nodes[i].Copy()
		node.ResourceVersion = strconv.Itoa(rvToGenerate)
		newEvent := runtime.NewNodeEvent(node, runtime.Modified)
		result[i] = newEvent
	}
	return result
}

func createRandomNode(rv int, loc *location.Location) *types.LogicalNode {
	id := uuid.New()
	return &types.LogicalNode{
		Id:              id.String(),
		ResourceVersion: strconv.Itoa(rv),
		GeoInfo: types.NodeGeoInfo{
			Region:            types.RegionName(loc.GetRegion()),
			ResourcePartition: types.ResourcePartitionName(loc.GetResourcePartition()),
		},
	}
}

func TestUpdateNodes(t *testing.T) {
	distributor := setUp()
	defer tearDown()

	nodeCounts := []int{10, 100, 1000, 10000, 100000, 1000000}
	previousNodeCount := 0
	for i := 0; i < len(nodeCounts); i++ {
		addAndUpdateNodes(t, distributor, nodeCounts[i], previousNodeCount)
		previousNodeCount += nodeCounts[i]
	}
}

func addAndUpdateNodes(t *testing.T, distributor *ResourceDistributor, eventNum int, previousNodeCount int) {
	eventsAdd := generateAddNodeEvent(eventNum, defaultLocBeijing_RP1)
	measureProcessEvent(t, distributor, "AddNode", eventsAdd, previousNodeCount)
	// update nodes
	eventsUpdate := generateUpdateNodeEvents(eventsAdd)
	measureProcessEvent(t, distributor, "UpdateNode", eventsUpdate, previousNodeCount)
}

func TestRegisterClient_ErrorCases(t *testing.T) {
	distributor := setUp()
	defer tearDown()

	result, rvMap := distributor.ProcessEvents(generateAddNodeEvent(10, defaultLocBeijing_RP1))
	assert.True(t, result)
	assert.NotNil(t, rvMap)
	assert.Equal(t, 10, distributor.defaultNodeStore.GetTotalHostNum())

	client := types.Client{ClientId: uuid.New().String(), Resource: types.ResourceRequest{TotalMachines: 100}, ClientInfo: types.ClientInfoType{}}
	// not enough hosts
	err := distributor.RegisterClient(&client)
	clientId := client.ClientId
	assert.NotNil(t, clientId, "Expecting not nil client id")
	assert.False(t, clientId == "", "Expecting non empty client id")
	assert.Equal(t, types.Error_HostRequestExceedLimit, err)

	// less than minimal request host number
	client = types.Client{ClientId: uuid.New().String(), Resource: types.ResourceRequest{TotalMachines: MinimalRequestHostNum - 1}, ClientInfo: types.ClientInfoType{}}
	err = distributor.RegisterClient(&client)
	clientId = client.ClientId
	assert.NotNil(t, clientId, "Expecting not nil client id")
	assert.False(t, clientId == "", "Expecting non empty client id")
	assert.Equal(t, types.Error_HostRequestLessThanMiniaml, err)
}

func TestRegisterClient_WithinLimit(t *testing.T) {
	vNodeNumberPerRPToTest := []int{2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 50, 51, 100, 101, 200, 201, 500, 501}
	for _, v := range vNodeNumberPerRPToTest {
		t.Logf("Test register client with virutalStoreNumPerResourcePartition = %d", v)
		virutalStoreNumPerResourcePartition = v
		distributor := setUp()

		totalHostCount := 10000
		result, rvMap := distributor.ProcessEvents(generateAddNodeEvent(totalHostCount, defaultLocBeijing_RP1))
		assert.True(t, result)
		assert.NotNil(t, rvMap)
		assert.Equal(t, totalHostCount, distributor.defaultNodeStore.GetTotalHostNum())

		allHosts := make(map[string]bool, 0)
		requestedHostNum := 500
		for i := 0; i < 20; i++ {
			start := time.Now()
			client := types.Client{ClientId: uuid.New().String(), Resource: types.ResourceRequest{TotalMachines: requestedHostNum}, ClientInfo: types.ClientInfoType{}}
			err := distributor.RegisterClient(&client)
			duration := time.Since(start)

			clientId := client.ClientId
			assert.NotNil(t, clientId, "Expecting not nil client id")
			assert.False(t, clientId == "", "Expecting non empty client id")
			assert.Nil(t, err, "Expecting nil error")

			// check virtual node assignment
			virtualStoresAssignedToClient, isOK := distributor.clientToStores[clientId]
			assert.True(t, isOK, "Expecting get virtual stores assigned to client %s", clientId)
			assert.True(t, len(virtualStoresAssignedToClient) > 0, "Expecting get non empty virtual stores assigned to client %s", clientId)
			hostCount := 0
			for j := 0; j < len(virtualStoresAssignedToClient); j++ {
				vs := virtualStoresAssignedToClient[j]
				assert.Equal(t, clientId, vs.GetAssignedClient(), "Unexpected virtual store client id %s", clientId)
				lower, upper := vs.GetAdjustedRange()
				t.Logf("Virtual node store (%f, %f] is assigned to client %s, host number %d. vNodePerPR %v", lower, upper, clientId, vs.GetHostNum(), virutalStoreNumPerResourcePartition)
				if !vs.IsValidVirtualNodeStore() {
					t.Fatalf("Invalid vNode store for client %v", clientId)
				}
				hostCount += vs.GetHostNum()
			}
			t.Logf("Total %d hosts are assigned to client %s\nTook %v to register the client.\n", hostCount, clientId, duration)
			if requestedHostNum != hostCount {
				assert.Equal(t, requestedHostNum, hostCount, "Assigned host number %d is not equal to requested %d", hostCount, requestedHostNum)
				t.Fatalf("virutalStoreNumPerResourcePartition = %d, client count %v", virutalStoreNumPerResourcePartition, i)
			}

			// check nodes number with list nodes
			nodes, _, err := distributor.ListNodesForClient(clientId)
			assert.Nil(t, err, "List nodes by client id should be successful")
			assert.Equal(t, hostCount, len(nodes), "Node count from virtual store should be same as list nodes")

			// check node allocation and make sure there is no duplicates
			for _, n := range nodes {
				if _, isOK := allHosts[n.Id]; isOK {
					t.Fatalf("Host was assigned to another client but reassigned again, id %v", n.Id)
				} else {
					allHosts[n.Id] = true
				}
			}
		}

		assert.Equal(t, totalHostCount, len(allHosts))

		tearDown()
	}
}

func TestRegistrationWorkflow(t *testing.T) {
	distributor := setUp()
	defer tearDown()

	// initialize node store with 10K nodes
	eventsAdd := generateAddNodeEvent(10000, defaultLocBeijing_RP1)
	result, rvMap := distributor.ProcessEvents(eventsAdd)
	assert.True(t, result)
	assert.NotNil(t, rvMap)
	assert.Equal(t, 10000, distributor.defaultNodeStore.GetTotalHostNum())

	// update nodes
	eventsUpdate := generateUpdateNodeEvents(eventsAdd)
	result, rvMap = distributor.ProcessEvents(eventsUpdate)
	assert.True(t, result)
	assert.NotNil(t, rvMap)
	assert.Equal(t, 10000, distributor.defaultNodeStore.GetTotalHostNum())

	// register client
	requestedHostNum := 500

	client := types.Client{ClientId: uuid.New().String(), Resource: types.ResourceRequest{TotalMachines: requestedHostNum}, ClientInfo: types.ClientInfoType{}}
	err := distributor.RegisterClient(&client)
	clientId := client.ClientId
	assert.NotNil(t, clientId, "Expecting not nil client id")
	assert.False(t, clientId == "", "Expecting non empty client id")
	assert.Nil(t, err, "Expecting nil error")

	// client list nodes
	nodes, latestRVs, err := distributor.ListNodesForClient(clientId)
	assert.Nil(t, err)
	assert.NotNil(t, latestRVs)
	assert.Equal(t, requestedHostNum, len(nodes))
	t.Logf("Latest rvs: %v. Total hosts: %d\n", latestRVs, len(nodes))
	// check each node event
	nodeIds := make(map[string]bool)
	for _, node := range nodes {
		nodeLoc := types.RvLocation{Region: location.Region(node.GeoInfo.Region), Partition: location.ResourcePartition(node.GeoInfo.ResourcePartition)}
		assert.NotNil(t, nodeLoc)
		assert.True(t, latestRVs[nodeLoc] >= node.GetResourceVersionInt64())
		if _, isOK := nodeIds[node.Id]; isOK {
			assert.Fail(t, "List nodes cannot have more than one copy of a node")
		} else {
			nodeIds[node.Id] = true
		}
	}
	assert.Equal(t, len(nodes), len(nodeIds))

	// update nodes
	oldNodeRV := nodes[0].GetResourceVersionInt64()
	updateNodeEvents := generatedUpdateNodeEventsFromNodeList(nodes)
	result2, rvMap2 := distributor.ProcessEvents(updateNodeEvents)
	assert.True(t, result2, "Expecting update nodes successfully")
	loc := types.RvLocation{Region: location.Region(nodes[0].GeoInfo.Region), Partition: location.ResourcePartition(nodes[0].GeoInfo.ResourcePartition)}

	assert.Equal(t, uint64(rvToGenerate), rvMap2[loc])
	assert.Equal(t, oldNodeRV, nodes[0].GetResourceVersionInt64(), "Expecting listed nodes are snapshoted and cannot be affected by update")

	// client watch node update
	watchCh := make(chan runtime.Object)
	stopCh := make(chan struct{})
	err = distributor.Watch(clientId, latestRVs, watchCh, stopCh)
	if err != nil {
		assert.Fail(t, "Encountered error while building watch connection.", "Encountered error while building watch connection. Error %v", err)
		return
	}
	watchedEventCount := 0
	for e := range watchCh {
		assert.Equal(t, runtime.Modified, e.GetEventType())
		nodeLoc := types.RvLocation{Region: location.Region(e.GetGeoInfo().Region), Partition: location.ResourcePartition(e.GetGeoInfo().ResourcePartition)}

		assert.Equal(t, loc, nodeLoc)
		watchedEventCount++

		if watchedEventCount >= len(nodes) {
			break
		}
	}
	assert.Equal(t, len(nodes), watchedEventCount)
	t.Logf("Latest rvs after updates: %v\n", rvMap2)
}

func TestRegistration5MCase(t *testing.T) {
	testCases := []struct {
		name             string
		regionNum        int
		rpNum            int
		hostPerRP        int
		schedulerNum     int
		hostPerScheduler int
	}{
		{
			// 5 region, each has 40 resource partitions, each partition has 25K nodes; 100 scheduler, each request 50K nodes
			name:             "5 regions, 40 RPs (25K nodes each); 100 schedulers, 50K nodes each",
			regionNum:        5,
			rpNum:            40,
			hostPerRP:        25000,
			schedulerNum:     100,
			hostPerScheduler: 50000,
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			distributor := setUp()
			defer tearDown()

			previousNodeCount := 0
			// initialize node store with nodes
			for i := 0; i < tt.regionNum; i++ {
				region := location.Region(i)
				eventsAdd := make([]*runtime.NodeEvent, tt.rpNum*tt.hostPerRP)
				for j := 0; j < tt.rpNum; j++ {
					resourcePartition := location.ResourcePartition(j)

					loc := location.NewLocation(region, resourcePartition)
					generateAddNodeEventToArray(tt.hostPerRP, loc, &eventsAdd, j*tt.hostPerRP)
				}
				measureProcessEvent(t, distributor, "AddNode", eventsAdd, previousNodeCount)
				previousNodeCount += len(eventsAdd)
			}

			// register scheduler
			wg := new(sync.WaitGroup)
			wg.Add(tt.schedulerNum)
			totalErrors := 0
			errLock := sync.RWMutex{}

			for i := 0; i < tt.schedulerNum; i++ {
				go func(w *sync.WaitGroup, errCount *int, lock sync.RWMutex) {
					client := types.Client{
						ClientId:   uuid.New().String(),
						Resource:   types.ResourceRequest{TotalMachines: tt.hostPerScheduler},
						ClientInfo: types.ClientInfoType{}}

					start := time.Now()
					err := distributor.RegisterClient(&client)
					duration := time.Since(start)
					clientId := client.ClientId

					assert.NotNil(t, clientId, "Expecting not nil client id")
					assert.False(t, clientId == "", "Expecting non empty client id")
					if err != nil {
						assert.Equal(t, "Not enough hosts", err.Error())
						lock.Lock()
						*errCount = *errCount + 1
						lock.Unlock()
					} else {
						klog.Infof("Register client %s took %v", clientId, duration)

						// list
						start = time.Now()
						nodes, latestRVs, err2 := distributor.ListNodesForClient(clientId)
						duration = time.Since(start)

						assert.Nil(t, err2)
						assert.NotNil(t, latestRVs)
						assert.Equal(t, tt.hostPerScheduler, len(nodes))
						klog.Infof("List nodes for client %s took %v", clientId, duration)
					}

					w.Done()
				}(wg, &totalErrors, errLock)
			}

			wg.Wait()
			assert.Equal(t, 1, totalErrors)
			t.Logf("%s succeed", tt.name)
		})
	}
}

func TestWatchRenewal(t *testing.T) {
	distributor := setUp()
	defer tearDown()

	// initialize node store with 10K nodes
	eventsAdd := generateAddNodeEvent(10000, defaultLocBeijing_RP1)
	result, rvMap := distributor.ProcessEvents(eventsAdd)
	assert.True(t, result)
	assert.NotNil(t, rvMap)
	assert.Equal(t, 10000, distributor.defaultNodeStore.GetTotalHostNum())

	// register client
	requestedHostNum := 500

	client := types.Client{ClientId: uuid.New().String(), Resource: types.ResourceRequest{TotalMachines: requestedHostNum}, ClientInfo: types.ClientInfoType{}}
	err := distributor.RegisterClient(&client)
	clientId := client.ClientId
	assert.NotNil(t, clientId, "Expecting not nil client id")
	assert.False(t, clientId == "", "Expecting non empty client id")
	assert.Nil(t, err, "Expecting nil error")

	// client list nodes
	nodes, latestRVs, err := distributor.ListNodesForClient(clientId)
	assert.Nil(t, err)
	assert.NotNil(t, latestRVs)
	assert.Equal(t, requestedHostNum, len(nodes))
	t.Logf("Latest rvs: %v. Total hosts: %d\n", latestRVs, len(nodes))
	// check each node event
	nodeIds := make(map[string]bool)
	for _, node := range nodes {
		nodeLoc := types.RvLocation{Region: location.Region(node.GeoInfo.Region), Partition: location.ResourcePartition(node.GeoInfo.ResourcePartition)}
		assert.NotNil(t, nodeLoc)
		assert.True(t, latestRVs[nodeLoc] >= node.GetResourceVersionInt64())
		if _, isOK := nodeIds[node.Id]; isOK {
			assert.Fail(t, "List nodes cannot have more than one copy of a node")
		} else {
			nodeIds[node.Id] = true
		}
	}
	assert.Equal(t, len(nodes), len(nodeIds))
	loc := location.NewLocation(location.Region(nodes[0].GeoInfo.Region), location.ResourcePartition(nodes[0].GeoInfo.ResourcePartition))

	// client watch node update
	watchCh := make(chan runtime.Object)
	stopCh := make(chan struct{})
	err = distributor.Watch(clientId, latestRVs, watchCh, stopCh)
	if err != nil {
		assert.Fail(t, "Encountered error while building watch connection.", "Encountered error while building watch connection. Error %v", err)
		return
	}

	wgForWatchEvent := new(sync.WaitGroup)
	wgForWatchEvent.Add(1)
	lastRVWatched := new(uint64)
	watchedEventCount := new(int)
	*watchedEventCount = 0
	watch(t, wgForWatchEvent, lastRVWatched, watchedEventCount, len(nodes), watchCh, loc)

	// generate update node events
	updateNodeEvents := generatedUpdateNodeEventsFromNodeList(nodes)
	result2, rvMap2 := distributor.ProcessEvents(updateNodeEvents)
	assert.True(t, result2, "Expecting update nodes successfully")

	// wait for event watch
	wgForWatchEvent.Wait()

	assert.Equal(t, len(nodes), *watchedEventCount)
	assert.Equal(t, rvMap2[types.RvLocation{Region: loc.GetRegion(), Partition: loc.GetResourcePartition()}], uint64(*lastRVWatched))
	t.Logf("Latest rvs after updates: %v\n", rvMap2)

	// watch renewal
	t.Logf("Watch renewal .....................")
	close(stopCh)
	time.Sleep(100 * time.Millisecond) // note here sleep is necessary. otherwise previous watch channel was not successfully discarded
	watchCh2 := make(chan runtime.Object)
	stopCh2 := make(chan struct{})
	err = distributor.Watch(clientId, rvMap2, watchCh2, stopCh2)
	if err != nil {
		assert.Fail(t, "Encountered error while building watch connection.", "Encountered error while building watch connection. Error %v", err)
		return
	}

	wgForWatchEvent2 := new(sync.WaitGroup)
	wgForWatchEvent2.Add(1)
	lastRVWatched2 := new(uint64)
	watchedEventCount2 := new(int)
	*watchedEventCount2 = 0
	watch(t, wgForWatchEvent2, lastRVWatched2, watchedEventCount2, len(nodes), watchCh2, loc)

	// generate update node events
	updateNodeEvents2 := generatedUpdateNodeEventsFromNodeList(nodes)
	result3, rvMap3 := distributor.ProcessEvents(updateNodeEvents2)
	assert.True(t, result3, "Expecting update nodes successfully")

	// wait for event watch
	wgForWatchEvent2.Wait()

	assert.Equal(t, len(nodes), *watchedEventCount2)
	assert.Equal(t, rvMap3[types.RvLocation{Region: loc.GetRegion(), Partition: loc.GetResourcePartition()}], uint64(*lastRVWatched2))
	t.Logf("Latest rvs after updates: %v\n", rvMap3)
}

func watch(t *testing.T, wg *sync.WaitGroup, lastRVResult *uint64, watchedEventCount *int, expectedEventCount int, watchCh chan runtime.Object, loc *location.Location) {
	go func(wg *sync.WaitGroup, t *testing.T, lastRVResult *uint64, watchedEventCount *int, expectedEventCount int) {
		lastRV := uint64(0)
		for e := range watchCh {
			assert.Equal(t, runtime.Modified, e.GetEventType())
			nodeLoc := location.NewLocation(location.Region(e.GetGeoInfo().Region), location.ResourcePartition(e.GetGeoInfo().ResourcePartition))
			assert.Equal(t, loc, nodeLoc)
			*watchedEventCount++

			newRV := e.GetResourceVersionInt64()
			if newRV < lastRV {
				t.Logf("Got event with rv %d later than rv %d", newRV, lastRV)
			} else {
				lastRV = newRV
			}

			if *watchedEventCount >= expectedEventCount {
				t.Logf("Last RV watched %v\n", lastRV)
				*lastRVResult = lastRV
				wg.Done()
				return
			}
		}
	}(wg, t, lastRVResult, watchedEventCount, expectedEventCount)
}
