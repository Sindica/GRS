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
	"strconv"
	"sync"
	"testing"
	"time"

	"global-resource-service/resource-management/pkg/common-lib/types"
	"global-resource-service/resource-management/pkg/common-lib/types/location"
	"global-resource-service/resource-management/pkg/common-lib/types/runtime"
)

func TestSingleRPMutipleClients_Workflow(t *testing.T) {
	testCases := []struct {
		name           string
		nodeNum        int
		clientNum      int
		hostPerClient  int
		updateEventNum int
	}{
		{
			name:           "Test 10K nodes with 5 clients each has 500 hosts, each got 1K update events",
			nodeNum:        10000,
			clientNum:      5,
			hostPerClient:  500,
			updateEventNum: 1000,
		},
		{
			name:           "Test 10K nodes with 5 clients each has 500 hosts, each got 10K update events",
			nodeNum:        10000,
			clientNum:      5,
			hostPerClient:  500,
			updateEventNum: 10000,
		},
		{
			name:           "Test 10K nodes with 5 clients each has 500 hosts, each got 100K update events",
			nodeNum:        10000,
			clientNum:      5,
			hostPerClient:  500,
			updateEventNum: 100000,
		},
		{
			name:           "Test 1M nodes with 50 clients each has 15000 hosts, each got 100K update events",
			nodeNum:        1000000,
			clientNum:      50,
			hostPerClient:  15000,
			updateEventNum: 100000,
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			distributor := setUp()
			defer tearDown()

			// initialize node store with tt.nodeNum nodes
			eventsAdd := generateAddNodeEvent(tt.nodeNum, defaultLocBeijing_RP1)

			start := time.Now()
			result, rvMap := distributor.ProcessEvents(eventsAdd)
			duration := time.Since(start)

			assert.True(t, result)
			assert.NotNil(t, rvMap)
			assert.Equal(t, tt.nodeNum, distributor.defaultNodeStore.GetTotalHostNum())

			// register clients
			clientIds := make([]string, tt.clientNum)
			for i := 0; i < tt.clientNum; i++ {
				start = time.Now()

				client := types.Client{ClientId: uuid.New().String(), Resource: types.ResourceRequest{TotalMachines: tt.hostPerClient}, ClientInfo: types.ClientInfoType{}}
				err := distributor.RegisterClient(&client)
				duration += time.Since(start)
				clientId := client.ClientId

				assert.True(t, result, "Expecting register client successfully")
				assert.NotNil(t, clientId, "Expecting not nil client id")
				assert.False(t, clientId == "", "Expecting non empty client id")
				assert.Nil(t, err, "Expecting nil error")
				clientIds[i] = clientId
			}

			// client list nodes
			latestRVsByClient := make([]types.TransitResourceVersionMap, tt.clientNum)
			nodesByClient := make([][]*types.LogicalNode, tt.clientNum)
			for i := 0; i < tt.clientNum; i++ {
				clientId := clientIds[i]

				start = time.Now()
				nodes, latestRVs, err := distributor.ListNodesForClient(clientId)
				duration += time.Since(start)

				assert.Nil(t, err)
				assert.NotNil(t, latestRVs)
				assert.Equal(t, tt.hostPerClient, len(nodes))
				// t.Logf("Client %d %s latest rvs: %v.Total hosts: %d\n", i, clientId, latestRVs, len(nodes))
				latestRVsByClient[i] = latestRVs
				nodesByClient[i] = nodes

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
			}

			// clients watch nodes
			stopCh := make(chan struct{})
			allWaitGroup := new(sync.WaitGroup)
			start = time.Now()
			for i := 0; i < tt.clientNum; i++ {
				watchCh := make(chan runtime.Object)
				err := distributor.Watch(clientIds[i], latestRVsByClient[i], watchCh, stopCh)
				if err != nil {
					assert.Fail(t, "Encountered error while building watch connection.", "Encountered error while building watch connection. Error %v", err)
					return
				}
				allWaitGroup.Add(1)

				go func(expectedEventCount int, watchCh chan runtime.Object, wg *sync.WaitGroup) {
					eventCount := 0

					for e := range watchCh {
						assert.Equal(t, runtime.Modified, e.GetEventType())
						eventCount++

						if eventCount >= expectedEventCount {
							wg.Done()
							return
						}
					}
				}(tt.updateEventNum, watchCh, allWaitGroup)
			}

			// update nodes
			for i := 0; i < tt.clientNum; i++ {
				go func(expectedEventCount int, nodes []*types.LogicalNode, clientId string) {
					for j := 0; j < expectedEventCount/len(nodes)+2; j++ {
						updateNodeEvents := make([]*runtime.NodeEvent, len(nodes))
						for k := 0; k < len(nodes); k++ {
							rvToGenerate += 1

							newNode := nodes[k].Copy()
							newNode.ResourceVersion = strconv.Itoa(rvToGenerate)
							updateNodeEvents[k] = runtime.NewNodeEvent(newNode, runtime.Modified)
						}
						result, rvMap := distributor.ProcessEvents(updateNodeEvents)
						assert.True(t, result)
						assert.NotNil(t, rvMap)
						//t.Logf("Successfully processed %d update node events. RV map returned: %v. ClientId %s\n", len(nodes), rvMap, clientId)
					}
				}(tt.updateEventNum, nodesByClient[i], clientIds[i])
			}

			// wait for watch done
			allWaitGroup.Wait()
			duration += time.Since(start)
			t.Logf("Test %s succeed! Total duration %v\n", tt.name, duration)
		})
	}
}

func TestMultipleRPsMutipleClients_Workflow(t *testing.T) {
	testCases := []struct {
		name           string
		regionNum      int
		rpNum          int
		hostPerRP      int
		clientNum      int
		hostPerClient  int
		updateEventNum int
	}{
		{
			name:           "Test 1 region, 10 RP, 20K hosts per RP, 200K hosts with 10 clients, each got 1K hosts, 10K update events",
			regionNum:      1,
			rpNum:          10,
			hostPerRP:      20000,
			clientNum:      10,
			hostPerClient:  1000,
			updateEventNum: 10000,
		},
		{
			name:           "Test 5 region, each has 20 RPs, 20K hosts per RP, 2M nodes with 100 clients, each got 10K hosts, 10K update events",
			regionNum:      5,
			rpNum:          20,
			hostPerRP:      20000,
			clientNum:      100,
			hostPerClient:  10000,
			updateEventNum: 10000,
		},
		{
			name:           "Test 6 region, each has 20 RPs, 40K hosts per RP, 4.8M nodes with 200 clients, each got 20K hosts, 20K update events",
			regionNum:      6,
			rpNum:          20,
			hostPerRP:      40000,
			clientNum:      200,
			hostPerClient:  20000,
			updateEventNum: 20000,
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			virutalStoreNumPerResourcePartition = tt.hostPerRP / 50
			distributor := setUp()
			defer tearDown()

			// create nodes
			eventsAdd := make([][][]*runtime.NodeEvent, tt.regionNum)
			for i := 0; i < tt.regionNum; i++ {
				regionName := location.Regions[i]
				eventsAdd[i] = make([][]*runtime.NodeEvent, tt.rpNum)
				for j := 0; j < tt.rpNum; j++ {
					rpName := location.ResourcePartitions[j]
					loc := location.NewLocation(regionName, rpName)

					eventsAdd[i][j] = generateAddNodeEvent(tt.hostPerRP, loc)
				}
			}

			wg := new(sync.WaitGroup)
			wg.Add(tt.regionNum * tt.rpNum)

			start := time.Now()
			for i := 0; i < tt.regionNum; i++ {
				for j := 0; j < tt.rpNum; j++ {
					go func(done *sync.WaitGroup, events []*runtime.NodeEvent) {
						result, rvMap := distributor.ProcessEvents(events)
						done.Done()
						assert.True(t, result)
						assert.NotNil(t, rvMap)
					}(wg, eventsAdd[i][j])
				}
			}
			wg.Wait()
			duration := time.Since(start)

			// register clients
			clientIds := make([]string, tt.clientNum)
			wg.Add(tt.clientNum)

			start = time.Now()
			for i := 0; i < tt.clientNum; i++ {
				go func(done *sync.WaitGroup, hostPerClient int, clientIds []string, i int) {
					client := types.Client{ClientId: uuid.New().String(), Resource: types.ResourceRequest{TotalMachines: hostPerClient}, ClientInfo: types.ClientInfoType{}}
					err := distributor.RegisterClient(&client)
					clientId := client.ClientId
					clientIds[i] = clientId
					done.Done()

					assert.NotNil(t, clientId, "Expecting not nil client id")
					assert.False(t, clientId == "", "Expecting non empty client id")
					assert.Nil(t, err, "Expecting nil error")
				}(wg, tt.hostPerClient, clientIds, i)
			}
			wg.Wait()
			duration += time.Since(start)

			// client list nodes
			latestRVsByClient := make([]types.TransitResourceVersionMap, tt.clientNum)
			nodesByClient := make([][]*types.LogicalNode, tt.clientNum)
			wg.Add(tt.clientNum)

			start = time.Now()
			for i := 0; i < tt.clientNum; i++ {
				go func(done *sync.WaitGroup, clientId string, i int) {
					nodes, latestRVs, err := distributor.ListNodesForClient(clientId)
					done.Done()

					assert.Nil(t, err)
					assert.NotNil(t, latestRVs)
					assert.Equal(t, tt.hostPerClient, len(nodes))
					// t.Logf("Client %d %s latest rvs: %v.Total hosts: %d\n", i, clientId, latestRVs, len(nodes))
					latestRVsByClient[i] = latestRVs
					nodesByClient[i] = nodes

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
				}(wg, clientIds[i], i)
			}
			wg.Wait()
			duration += time.Since(start)

			// clients watch nodes
			allWaitGroup := new(sync.WaitGroup)
			start = time.Now()
			for i := 0; i < tt.clientNum; i++ {
				watchCh := make(chan runtime.Object)
				stopCh := make(chan struct{})
				err := distributor.Watch(clientIds[i], latestRVsByClient[i], watchCh, stopCh)
				if err != nil {
					assert.Fail(t, "Encountered error while building watch connection.", "Encountered error while building watch connection. Error %v", err)
					return
				}
				allWaitGroup.Add(1)

				go func(expectedEventCount int, watchCh chan runtime.Object, wg *sync.WaitGroup) {
					eventCount := 0

					for e := range watchCh {
						assert.Equal(t, runtime.Modified, e.GetEventType())
						eventCount++

						if eventCount >= expectedEventCount {
							wg.Done()
							t.Logf("Successfully watched %d update node events.**************************\n", expectedEventCount)
							return
						}
					}
				}(tt.updateEventNum, watchCh, allWaitGroup)
			}

			t.Log("Starting to watch update events ##################\n")

			// update nodes
			for i := 0; i < tt.clientNum; i++ {
				go func(expectedEventCount int, nodes []*types.LogicalNode, clientId string) {
					eventCount := 0

					for j := 0; j < expectedEventCount/len(nodes)+2; j++ {
						updateNodeEvents := make([]*runtime.NodeEvent, len(nodes))
						for k := 0; k < len(nodes); k++ {
							rvToGenerate += 1
							newNode := nodes[k].Copy()
							newNode.ResourceVersion = strconv.Itoa(rvToGenerate)
							updateNodeEvents[k] = runtime.NewNodeEvent(newNode, runtime.Modified)

							eventCount++
							if eventCount >= expectedEventCount {
								break
							}
						}
						result, rvMap := distributor.ProcessEvents(updateNodeEvents)
						assert.True(t, result)
						assert.NotNil(t, rvMap)
						//t.Logf("Successfully processed %d update node events. RV map returned: %v. ClientId %s\n", len(nodes), rvMap, clientId)
					}
				}(tt.updateEventNum, nodesByClient[i], clientIds[i])
			}

			// wait for watch done
			allWaitGroup.Wait()
			duration += time.Since(start)

			t.Logf("Test %s succeed! Total duration %v\n", tt.name, duration)
		})
	}
}

/*
RV using map, has lock on ResourceDistributor.ProcessEvents
Processing 20 AddNode events took 47.672µs.
Processing 200 AddNode events took 180.329µs.
Processing 2000 AddNode events took 1.639455ms.
Processing 20000 AddNode events took 15.005638ms.
Processing 200000 AddNode events took 202.639629ms.
Processing 2000000 AddNode events took 2.202974335s.

RV using map, NO lock on ResourceDistributor.ProcessEvents
Processing 20 AddNode events took 49.985µs.
Processing 200 AddNode events took 177.707µs.
Processing 2000 AddNode events took 1.6575ms.
Processing 20000 AddNode events took 15.87493ms.
Processing 200000 AddNode events took 203.718498ms.
Processing 2000000 AddNode events took 2.338442347s.

RV using array, NO lock on ResourceDistributor.ProcessEvents, has lock on rv check
Processing 20 AddNode events took 71.154µs.
Processing 200 AddNode events took 183.098µs.
Processing 2000 AddNode events took 1.607259ms.
Processing 20000 AddNode events took 11.793391ms.
Processing 200000 AddNode events took 154.692465ms.
Processing 2000000 AddNode events took 1.85247087s.

RV using array, added lock for snapshot in event queue
Processing 20 AddNode events took 46.419µs.
Processing 200 AddNode events took 144.9µs.
Processing 2000 AddNode events took 1.390383ms.
Processing 20000 AddNode events took 11.794798ms.
Processing 200000 AddNode events took 144.229808ms.
Processing 2000000 AddNode events took 1.86103252s.

Updated to logical node:
Processing 20 AddNode events took 58.735µs.
Processing 200 AddNode events took 182.211µs.
Processing 2000 AddNode events took 1.663906ms.
Processing 20000 AddNode events took 15.068522ms.
Processing 200000 AddNode events took 179.720773ms.
Processing 2000000 AddNode events took 2.324969732s. 24% increase. 8.16% get location from name (can be further optimized), 2% create ManagedNodeEvent

Use int for region and resource partition field
Processing 20 AddNode events took 46.946µs.
Processing 200 AddNode events took 165.089µs.
Processing 2000 AddNode events took 1.494359ms.
Processing 20000 AddNode events took 13.193021ms.
Processing 200000 AddNode events took 168.53739ms.
Processing 2000000 AddNode events took 2.172339411s. 6.5% improvement

. Added persistence
Processing 20 AddNode events took 197.943µs.
Processing 200 AddNode events took 1.571032ms.
Processing 2000 AddNode events took 4.219236ms.
Processing 20000 AddNode events took 28.11632ms.
Processing 200000 AddNode events took 294.131531ms.
Processing 2000000 AddNode events took 4.040301206s.

. Added checkpoints with array
Processing 20 AddNode events took 314.825µs.
Processing 200 AddNode events took 982.61µs.
Processing 2000 AddNode events took 6.241914ms.
Processing 20000 AddNode events took 57.767247ms.
Processing 200000 AddNode events took 583.935804ms.
Processing 2000000 AddNode events took 6.429718129s.

. Batch persist nodes - 100 per batch - with checkpoints enabled
Processing 20 AddNode events took 144.422µs.
Processing 200 AddNode events took 1.380407ms.
Processing 2000 AddNode events took 4.123098ms.
Processing 20000 AddNode events took 40.019265ms.
Processing 200000 AddNode events took 394.402522ms.
Processing 2000000 AddNode events took 4.549515804s.

. Batch persist nodes - 100 per batch - with checkpoints disabled
Processing 20 AddNode events took 98.111µs.
Processing 200 AddNode events took 1.266727ms.
Processing 2000 AddNode events took 1.563561ms.
Processing 20000 AddNode events took 11.765084ms.
Processing 200000 AddNode events took 165.029239ms.
Processing 2000000 AddNode events took 1.752964732s.

. Updated metrics data - 9/22/2022 main - PR 169 - with checkpoints enabled
Processing 20 AddNode events took 273.684µs.
Processing 200 AddNode events took 508.966µs.
Processing 2000 AddNode events took 4.405378ms.
Processing 20000 AddNode events took 48.118097ms.
Processing 200000 AddNode events took 461.86578ms.
Processing 2000000 AddNode events took 4.86233929s.

. Updated metrics data - 9/22/2022 main - PR 169 - with checkpoints disabled
Processing 20 AddNode events took 234.834µs.
Processing 200 AddNode events took 316.923µs.
Processing 2000 AddNode events took 1.83691ms.
Processing 20000 AddNode events took 17.937767ms.
Processing 200000 AddNode events took 215.819078ms.
Processing 2000000 AddNode events took 2.165953214s.
*/
func TestProcessEvents_TwoRPs_AddNodes_Sequential(t *testing.T) {
	distributor := setUp()
	defer tearDown()

	//common_lib.ResourceManagementMeasurement_Enabled = false
	nodeCounts := []int{10, 100, 1000, 10000, 100000, 1000000}
	// generate add node events
	for i := 0; i < len(nodeCounts); i++ {
		eventsAdd1 := generateAddNodeEvent(nodeCounts[i], defaultLocBeijing_RP1)
		eventsAdd2 := generateAddNodeEvent(nodeCounts[i], location.NewLocation(location.Shanghai, location.ResourcePartition2))
		start := time.Now()
		distributor.ProcessEvents(eventsAdd1)
		_, rvMap := distributor.ProcessEvents(eventsAdd2)
		duration := time.Since(start)
		t.Logf("Processing %d AddNode events took %v. Composite RVs %v\n", nodeCounts[i]*2, duration, rvMap)
	}
}

/*
RV using map, has lock on ResourceDistributor.ProcessEvents
Processing 20 AddNode events took 114.303µs.
Processing 200 AddNode events took 232.242µs.
Processing 2000 AddNode events took 2.335808ms.
Processing 20000 AddNode events took 15.309473ms.
Processing 200000 AddNode events took 182.779783ms.
Processing 2000000 AddNode events took 2.251335613s.

RV using map, NO lock on ResourceDistributor.ProcessEvents
Processing 20 AddNode events took 77.699µs.
Processing 200 AddNode events took 188.916µs.
Processing 2000 AddNode events took 1.206867ms.
Processing 20000 AddNode events took 11.0227ms.
Processing 200000 AddNode events took 126.118044ms.
Processing 2000000 AddNode events took 1.568443698s.

RV using array, NO lock on ResourceDistributor.ProcessEvents, has lock on rv check
Processing 20 AddNode events took 70.499µs.
Processing 200 AddNode events took 167.544µs.
Processing 2000 AddNode events took 906.622µs.
Processing 20000 AddNode events took 6.965772ms.
Processing 200000 AddNode events took 106.936265ms.
Processing 2000000 AddNode events took 1.24163592s.

RV using array, added lock for snapshot in event queue
Processing 20 AddNode events took 87.807µs.
Processing 200 AddNode events took 177.349µs.
Processing 2000 AddNode events took 859.874µs.
Processing 20000 AddNode events took 6.772558ms.
Processing 200000 AddNode events took 109.476743ms.
Processing 2000000 AddNode events took 1.21830443s.

Updated to logical node:
Processing 20 AddNode events took 123.203µs.
Processing 200 AddNode events took 274.647µs.
Processing 2000 AddNode events took 1.32293ms.
Processing 20000 AddNode events took 9.247576ms.
Processing 200000 AddNode events took 133.387737ms.
Processing 2000000 AddNode events took 1.549799563s. - 27% increase

Use int for region and resource partition field
Processing 20 AddNode events took 94.507µs.
Processing 200 AddNode events took 237.786µs.
Processing 2000 AddNode events took 2.307846ms.
Processing 20000 AddNode events took 8.299796ms.
Processing 200000 AddNode events took 130.861087ms.
Processing 2000000 AddNode events took 1.449320502s. - 6.5% improvement

. Added persistence
Processing 20 AddNode events took 430.548µs.
Processing 200 AddNode events took 1.231249ms.
Processing 2000 AddNode events took 2.68838ms.
Processing 20000 AddNode events took 18.140626ms.
Processing 200000 AddNode events took 175.578763ms.
Processing 2000000 AddNode events took 2.460715575s.

. Added checkpoints with array
Processing 20 AddNode events took 288.991µs.
Processing 200 AddNode events took 629.264µs.
Processing 2000 AddNode events took 3.910318ms.
Processing 20000 AddNode events took 32.042223ms.
Processing 200000 AddNode events took 365.70946ms.
Processing 2000000 AddNode events took 4.189824513s.

. Batch persist nodes - 100 per batch - with checkpoints enabled
Processing 20 AddNode events took 203.159µs.
Processing 200 AddNode events took 392.641µs.
Processing 2000 AddNode events took 2.403222ms.
Processing 20000 AddNode events took 24.039274ms.
Processing 200000 AddNode events took 253.344984ms.
Processing 2000000 AddNode events took 2.927105773s.

. Batch persist nodes - 100 per batch - with checkpoints disabled
Processing 20 AddNode events took 186.784µs.
Processing 200 AddNode events took 1.342371ms.
Processing 2000 AddNode events took 1.12844ms.
Processing 20000 AddNode events took 7.427902ms.
Processing 200000 AddNode events took 115.241136ms.
Processing 2000000 AddNode events took 1.175839189s.

. Updated metrics data - 9/22/2022 main - PR 169 - with checkpoints enabled
Processing 20 AddNode events took 250.904µs.
Processing 200 AddNode events took 1.46763ms.
Processing 2000 AddNode events took 2.528231ms.
Processing 20000 AddNode events took 23.925124ms.
Processing 200000 AddNode events took 265.018771ms.
Processing 2000000 AddNode events took 3.02275436s.

. Updated metrics data - 9/22/2022 main - PR 169 - with checkpoints disabled
Processing 20 AddNode events took 263.876µs.
Processing 200 AddNode events took 276.009µs.
Processing 2000 AddNode events took 1.294924ms.
Processing 20000 AddNode events took 12.04409ms.
Processing 200000 AddNode events took 125.329976ms.
Processing 2000000 AddNode events took 1.391614298s.
*/
func TestProcessEvents_TwoRPs_Concurrent(t *testing.T) {
	distributor := setUp()
	defer tearDown()

	//common_lib.ResourceManagementMeasurement_Enabled = false
	nodeCounts := []int{10, 100, 1000, 10000, 100000, 1000000}
	// generate add node events
	for i := 0; i < len(nodeCounts); i++ {
		eventsAdd1 := generateAddNodeEvent(nodeCounts[i], defaultLocBeijing_RP1)
		eventsAdd2 := generateAddNodeEvent(nodeCounts[i], location.NewLocation(location.Shanghai, location.ResourcePartition2))
		start := time.Now()

		wg := new(sync.WaitGroup)
		wg.Add(2)

		go func(done *sync.WaitGroup, eventsToProcess []*runtime.NodeEvent) {
			distributor.ProcessEvents(eventsToProcess)
			done.Done()
		}(wg, eventsAdd1)

		go func(done *sync.WaitGroup, eventsToProcess []*runtime.NodeEvent) {
			distributor.ProcessEvents(eventsToProcess)
			done.Done()
		}(wg, eventsAdd2)

		wg.Wait()
		duration := time.Since(start)
		t.Logf("Processing %d AddNode events took %v.\n", nodeCounts[i]*2, duration)
	}
}
