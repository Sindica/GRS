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

package data

import (
	"errors"
	"global-resource-service/resource-management/test/resourceRegionMgrSimulator/config"
	"math/rand"
	"strconv"
	"time"

	"github.com/google/uuid"
	"k8s.io/klog/v2"

	"global-resource-service/resource-management/pkg/common-lib/types"
	"global-resource-service/resource-management/pkg/common-lib/types/event"
	"global-resource-service/resource-management/pkg/common-lib/types/location"

	"global-resource-service/resource-management/test/resourceRegionMgrSimulator/cache"
	simulatorTypes "global-resource-service/resource-management/test/resourceRegionMgrSimulator/types"
)

// The following varables are used to create Region Node Event List in multiply RPs of one region
//
// RegionNodeEventsList      - for initpull
// SnapshotNodeListEvents    - for subsequent pull and
//                                 post CRV to discard all old node events
var RegionNodeEventsList simulatorTypes.RegionNodeEvents
var RegionNodeEventQueue *cache.NodeEventQueue
var CurrentRVs types.TransitResourceVersionMap

// The constants are for repeatly generate new modified events
// Outage pattern - one RP down

// Daily Patttern - 10 modified changes per minute
const atEachMin10 = 10

// Initialize two events list
// RegionNodeEventsList - for initpull
//
func Init(regionName string, rpNum, nodesPerRP int) {
	config.RegionId = int(location.GetRegionFromRegionName(regionName))
	config.RpNum = rpNum
	config.NodesPerRP = nodesPerRP

	RegionNodeEventQueue = cache.NewNodeEventQueue(config.RpNum)
	RegionNodeEventsList, CurrentRVs = generateAddedNodeEvents(regionName, rpNum, nodesPerRP)
}

// Generate region node update event changes to
// add them into RegionNodeEventsList
//
func MakeDataUpdate(data_pattern string, wait_time_for_make_rp_down int) {
	go func(data_pattern string, wait_time_for_make_rp_down int) {
		switch data_pattern {
		case "Outage":
			for {
				// Generate one RP down event during specfied interval
				time.Sleep(time.Duration(wait_time_for_make_rp_down) * time.Minute)
				makeOneRPDown()
				klog.V(3).Info("Generating one RP down event is completed")

				time.Sleep(120 * time.Minute)
				klog.V(6).Info("Simulate to delay 2 hours")
			}
		case "Daily":
			for {
				// At each minute mark, generate 10 modified node events
				time.Sleep(1 * time.Minute)
				go makeDataUpdate(atEachMin10)

				klog.V(3).Info("At each minute mark, generating 10 modified and added node events is completed")
			}
		default:
			klog.V(3).Infof("Current Simulator Data Pattern (%v) is supported", data_pattern)
			return
		}
	}(data_pattern, wait_time_for_make_rp_down)
}

///////////////////////////////////////////////
// The following functions are for handler.
//////////////////////////////////////////////

// Return region node added events in BATCH LENGTH from all RPs
// TO DO: paginate support
//
func ListNodes() (simulatorTypes.RegionNodeEvents, uint64, types.TransitResourceVersionMap) {
	klog.V(6).Infof("Total (%v) Added events are to be pulled", config.RpNum*config.NodesPerRP)

	nodeEventsByRP := make(simulatorTypes.RegionNodeEvents, config.RpNum)
	for i := 0; i < config.RpNum; i++ {
		nodeEventsByRP[i] = make([]*event.NodeEvent, config.NodesPerRP)
	}

	RegionNodeEventQueue.AcquireSnapshotRLock()
	for i := 0; i < config.RpNum; i++ {
		for j := 0; j < config.NodesPerRP; j++ {
			node := RegionNodeEventsList[i][j].Node.Copy()
			nodeEventsByRP[i][j] = event.NewNodeEvent(node, event.Added)
		}
	}

	currentRVs := CurrentRVs.Copy()
	RegionNodeEventQueue.ReleaseSnapshotRLock()

	return nodeEventsByRP, uint64(config.RpNum * config.NodesPerRP), currentRVs

}

// Return region node modified events with CRVs in BATCH LENGTH from all RPs
// TO DO: paginate support
//
func Watch(rvs types.TransitResourceVersionMap, watchChan chan *event.NodeEvent, stopCh chan struct{}) error {
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
	return RegionNodeEventQueue.Watch(internal_rvs, watchChan, stopCh)
}

////////////////////////////////////////
// The below are all helper functions
////////////////////////////////////////

// This function is used to initialize the region node added event
//
func generateAddedNodeEvents(regionName string, rpNum, nodesPerRP int) (simulatorTypes.RegionNodeEvents, types.TransitResourceVersionMap) {
	regionId := location.GetRegionFromRegionName(regionName)
	eventsAdd := make(simulatorTypes.RegionNodeEvents, rpNum)
	cvs := make(types.TransitResourceVersionMap)

	for j := 0; j < rpNum; j++ {
		rpName := location.ResourcePartitions[j]
		loc := location.NewLocation(regionId, rpName)
		rvLoc := types.RvLocation{
			Region:    regionId,
			Partition: rpName,
		}

		// Initialize the resource version starting from 0 for each RP
		var rvToGenerateRPs = 0
		eventsAdd[j] = make([]*event.NodeEvent, nodesPerRP)
		for i := 0; i < nodesPerRP; i++ {
			rvToGenerateRPs += 1

			nodeToAdd := createRandomNode(rvToGenerateRPs, loc)
			nodeEvent := event.NewNodeEvent(nodeToAdd, event.Added)
			eventsAdd[j][i] = nodeEvent

			// node event enqueue
			RegionNodeEventQueue.EnqueueEvent(nodeEvent)
		}

		cvs[rvLoc] = uint64(rvToGenerateRPs)
	}
	return eventsAdd, cvs
}

//This function simulates one random RP down
func makeOneRPDown() {
	selectedRP := int(rand.Intn(config.RpNum))
	klog.V(3).Infof("Generating all node down events in selected RP (%v) is starting", selectedRP)

	// Get the highestRVForRP of selectRP
	rvLoc := types.RvLocation{
		Region:    location.Region(config.RegionId),
		Partition: location.ResourcePartition(selectedRP),
	}
	highestRVForRP := CurrentRVs[rvLoc]

	// Make the modified changes for all nodes of selected RP
	rvToGenerateRPs := highestRVForRP + 1
	for i := 0; i < config.NodesPerRP; i++ {
		// Update the version of node with the current rvToGenerateRPs
		node := RegionNodeEventsList[selectedRP][i].Node
		node.ResourceVersion = strconv.FormatUint(rvToGenerateRPs, 10)

		// record the time to change resource version in resource partition
		node.LastUpdatedTime = time.Now().UTC()

		newEvent := event.NewNodeEvent(node, event.Modified)

		//RegionNodeEventsList[selectedRP][i] = no need: keep event as added, node will be updated as pointer
		RegionNodeEventQueue.EnqueueEvent(newEvent)

		rvToGenerateRPs++
	}

	// Record the highest RV for selected RP
	if config.NodesPerRP > 0 {
		CurrentRVs[rvLoc] = rvToGenerateRPs - 1
	}
}

// This function is used to create region node modified event by go routine
//
func makeDataUpdate(changesThreshold int) {
	// Calculate how many node changes per RP
	var nodeChangesPerRP = 1
	if changesThreshold >= 2*config.RpNum {
		nodeChangesPerRP = changesThreshold / config.RpNum
	}

	// Make data update for each RP
	for j := 0; j < config.RpNum; j++ {
		// get the highestRV
		rvLoc := types.RvLocation{
			Region:    location.Region(config.RegionId),
			Partition: location.ResourcePartition(j),
		}
		highestRVForRP := CurrentRVs[rvLoc]

		// Pick up 'nodeChangesPerRP' nodes and make changes and assign the node RV to highestRV + 1
		count := 0
		rvToGenerateRPs := highestRVForRP + 1
		for count < nodeChangesPerRP {
			// Randonly create data update per RP node events list
			i := rand.Intn(config.NodesPerRP)
			node := RegionNodeEventsList[j][i].Node

			// special case: Consider 5000 changes per RP for 500 nodes per RP
			// each node has 10 changes within this cycle
			node.ResourceVersion = strconv.FormatUint(rvToGenerateRPs, 10)
			// record the time to change resource version in resource partition
			node.LastUpdatedTime = time.Now().UTC()

			newEvent := event.NewNodeEvent(node, event.Modified)
			//RegionNodeEventsList[j][i] = newEvent - no need: keep event as added, node will be updated as pointer
			RegionNodeEventQueue.EnqueueEvent(newEvent)

			count++
			rvToGenerateRPs++
			time.Sleep(1 * time.Second)
		}
		if nodeChangesPerRP > 0 {
			CurrentRVs[rvLoc] = rvToGenerateRPs - 1
		}
	}

	klog.V(6).Infof("Actually total (%v) new modified events are created.", changesThreshold)
}

// Create logical node with random UUID
//
func createRandomNode(rv int, loc *location.Location) *types.LogicalNode {
	id := uuid.New()

	return &types.LogicalNode{
		Id:              id.String(),
		ResourceVersion: strconv.Itoa(rv),
		GeoInfo: types.NodeGeoInfo{
			Region:            types.RegionName(loc.GetRegion()),
			ResourcePartition: types.ResourcePartitionName(loc.GetResourcePartition()),
			DataCenter:        types.DataCenterName(strconv.Itoa(int(rand.Intn(10000))) + "-DataCenter"),
			AvailabilityZone:  types.AvailabilityZoneName("AZ-" + strconv.Itoa(int(rand.Intn(6)))),
			FaultDomain:       types.FaultDomainName(id.String() + "-FaultDomain"),
		},
		Taints: types.NodeTaints{
			NoSchedule: false,
			NoExecute:  false,
		},
		SpecialHardwareTypes: types.NodeSpecialHardWareTypeInfo{
			HasGpu:  true,
			HasFPGA: true,
		},
		AllocatableResource: types.NodeResource{
			MilliCPU:         int64(rand.Intn(200) + 20),
			Memory:           int64(rand.Intn(2000)),
			EphemeralStorage: int64(rand.Intn(2000000)),
			AllowedPodNumber: int(rand.Intn(20000000)),
			ScalarResources: map[types.ResourceName]int64{
				"GPU":  int64(rand.Intn(200)),
				"FPGA": int64(rand.Intn(200)),
			},
		},
		Conditions:      111,
		Reserved:        false,
		MachineType:     types.NodeMachineType(id.String() + "-highend"),
		LastUpdatedTime: time.Now().UTC(),
	}
}
