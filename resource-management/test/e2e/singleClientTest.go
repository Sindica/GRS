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

package main

import (
	"flag"
	"fmt"
	"k8s.io/klog/v2"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"global-resource-service/resource-management/pkg/clientSdk/rmsclient"
	"global-resource-service/resource-management/pkg/clientSdk/tools/cache"
	utilruntime "global-resource-service/resource-management/pkg/clientSdk/util/runtime"
	"global-resource-service/resource-management/pkg/common-lib/types"
	"global-resource-service/resource-management/pkg/common-lib/types/runtime"
	"global-resource-service/resource-management/test/e2e/stats"
)

const (
	register = "register"
	list     = "list"
	watch    = "watch"
)

type testConfig struct {
	testDuration time.Duration
	action       string
	repeats      int
}

func main() {
	flag.Usage = printUsage

	testCfg := testConfig{}
	cfg := rmsclient.Config{}
	listOpts := rmsclient.ListOptions{}
	var regions string

	flag.StringVar(&cfg.ServiceUrl, "service_url", "localhost:8080", "Service IP address, if not set, default to localhost")
	flag.DurationVar(&cfg.RequestTimeout, "request_timeout", 30*time.Minute, "Timeout for client requests and responses")
	flag.StringVar(&cfg.ClientFriendlyName, "friendly_name", "testclient", "Client friendly name other that the assigned Id")
	flag.StringVar(&cfg.ClientRegion, "client_region", "Beijing", "Client identify where it is located")
	flag.IntVar(&cfg.InitialRequestTotalMachines, "request_machines", 2500, "Initial request of number of machines")
	flag.StringVar(&cfg.RegionIdToWatch, "region_id_to_watch", "", "Region id(s) to watch, separated with comma")
	flag.BoolVar(&cfg.CaptureDetailedLog, "capture_detailed_log", false, "Write selected log")
	flag.StringVar(&regions, "request_regions", "Beijing", "list of regions, in comma separated string, to allocate the machines for the client")
	flag.DurationVar(&testCfg.testDuration, "test_duration", 10*time.Minute, "Test duration, measured by number minutes of watch of node changes. default 10 minutes")
	flag.StringVar(&testCfg.action, "action", "", "action to perform, can be register list or watch, default to register-list-watch")
	flag.IntVar(&testCfg.repeats, "repeats", 1, "number of repeats of the action, default to 1")
	flag.IntVar(&listOpts.Limit, "limit", 25000, "limit for list nodes, default to 25000")

	if !flag.Parsed() {
		klog.InitFlags(nil)
		flag.Parse()
	}
	klog.StartFlushDaemon(time.Second * 1)
	defer klog.Flush()

	cfg.InitialRequestRegions = strings.Split(regions, ",")
	client := rmsclient.NewRmsClient(cfg)

	store := createStore()

	registerStats := stats.NewRegisterClientStats()
	listStats := stats.NewListStats()
	watchStats := stats.NewWatchStats()

	hasRegionIdToWatch := true
	var regionIdsToWatch map[types.RegionName]bool
	if cfg.RegionIdToWatch == "" {
		hasRegionIdToWatch = false
	} else {
		idsToWatch := strings.Split(cfg.RegionIdToWatch, ",")
		regionIdsToWatch = make(map[types.RegionName]bool, len(idsToWatch))
		for i := 0; i < len(idsToWatch); i++ {
			id, err := strconv.Atoi(idsToWatch[i])
			if err != nil || i < 0 || i >= 5 { // currently only support 5 regions
				klog.Infof("Invalid region_id_to_watch to watch (%v). Needs to be have format 0,1,2,3,4", cfg.RegionIdToWatch)
				printUsage()
				return
			}
			regionIdsToWatch[types.RegionName(id)] = true
		}
	}

	switch testCfg.action {
	case register:
		for i := 0; i < testCfg.repeats; i++ {
			clientId := registerClient(client, registerStats)
			client.Id = clientId
			printTestStats(registerStats, listStats, watchStats)
		}
	case list:
		clientId := registerClient(client, registerStats)
		client.Id = clientId
		for i := 0; i < testCfg.repeats; i++ {
			_ = listNodes(client, client.Id, store, listStats, listOpts)
			printTestStats(registerStats, listStats, watchStats)
		}
	case watch:
		clientId := registerClient(client, registerStats)
		client.Id = clientId
		crv := listNodes(client, client.Id, store, listStats, listOpts)
		watchNodes(client, client.Id, crv, store, watchStats, hasRegionIdToWatch, regionIdsToWatch, cfg.CaptureDetailedLog)
		printTestStats(registerStats, listStats, watchStats)
	default:
		clientId := registerClient(client, registerStats)
		client.Id = clientId
		crv := listNodes(client, client.Id, store, listStats, listOpts)
		watchNodes(client, client.Id, crv, store, watchStats, hasRegionIdToWatch, regionIdsToWatch, cfg.CaptureDetailedLog)
		printTestStats(registerStats, listStats, watchStats)
	}

}

// function to print the usage info for the resource management api server
func printUsage() {
	fmt.Println("Usage: ")
	fmt.Println("--service_url=127.0.0.1:8080 --request_machines=10000 --request_regions=Beijing,Shanghai ")

	os.Exit(0)
}

func createStore() cache.Store {
	keyFunc := func(obj interface{}) (string, error) {
		node := obj.(types.LogicalNode)
		if len(node.Id) == 0 {
			return "", fmt.Errorf("empty node")
		}
		return node.Id, nil
	}
	return cache.NewStore(keyFunc)

}

func registerClient(client rmsclient.RmsInterface, registerStats *stats.RegisterClientStats) string {
	var start, end time.Time

	klog.Infof("Register client to service  ...")
	start = time.Now().UTC()
	registrationResp, err := client.Register()
	end = time.Now().UTC()
	if err != nil {
		klog.Errorf("failed register client to service. error %v", err)
		os.Exit(1)
	}
	klog.V(6).Infof("Got client registration from service: %v", registrationResp)

	registerStats.RegisterClientDuration = end.Sub(start)
	return registrationResp.ClientId
}

func listNodes(client rmsclient.RmsInterface, clientId string, store cache.Store, listStats *stats.ListStats, listOpts rmsclient.ListOptions) types.TransitResourceVersionMap {
	var start, end time.Time

	klog.Infof("List resources from service ...")
	start = time.Now().UTC()
	nodeList, crv, err := client.List(clientId, listOpts)
	end = time.Now().UTC()
	if err != nil {
		klog.Errorf("failed list resource from service. error %v", err)
		os.Exit(1)
	}
	klog.V(3).Infof("Got [%v] nodes from service", len(nodeList))
	klog.V(3).Infof("Got [%v] resource versions service", crv)

	stats.GroupByRegion(nodeList)
	stats.GroupByRegionByRP(nodeList)

	for _, node := range nodeList {
		store.Add(*node)
	}

	listStats.ListDuration = end.Sub(start)
	listStats.NumberOfNodesListed = len(nodeList)
	return crv
}

func watchNodes(client rmsclient.RmsInterface, clientId string, crv types.TransitResourceVersionMap, store cache.Store,
	watchStats *stats.WatchStats, hasRegionToWatch bool, regionsToWatch map[types.RegionName]bool, captureDetailedLog bool) {
	var start, end time.Time

	klog.V(3).Infof("Watch resources update from service ...")
	start = time.Now().UTC()
	watcher, err := client.Watch(clientId, crv)
	if err != nil {
		klog.Errorf("failed list resource from service. error %v", err)
	}

	watchCh := watcher.ResultChan()

	var regionWatchStart, regionWatchEnd *time.Time
	regionEventCount := 0

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer utilruntime.HandleCrash()
		// retrieve updates from watcher
		for {
			select {
			case record, ok := <-watchCh:
				if !ok {
					// End of results.
					klog.Infof("End of results")
					return
				}
				currentTime := time.Now().UTC()
				watchDelay := currentTime.Sub(record.Node.LastUpdatedTime)
				if captureDetailedLog {
					klog.Infof("[Metrics][Detail] node %v watch delay %v", record.Node.Id, watchDelay)
				}
				addWatchLatency(watchDelay, watchStats)
				logIfProlonged(&record, watchDelay, watchStats)

				if hasRegionToWatch {
					if _, isOK := regionsToWatch[record.Node.GeoInfo.Region]; isOK {
						regionEventCount++
						if regionWatchStart == nil {
							regionWatchStart = &currentTime
							klog.Infof("[Throughput] Time to start getting event from region %v: %v", record.Node.GeoInfo.Region, regionWatchStart)
						} else {
							regionWatchEnd = &currentTime
						}
					}
				}

				switch record.Type {
				case runtime.Added:
					store.Add(*record.Node)
					watchStats.NumberOfAddedNodes++
				case runtime.Modified:
					store.Update(*record.Node)
					watchStats.NumberOfUpdatedNodes++
				case runtime.Deleted:
					store.Delete(*record.Node)
					watchStats.NumberOfDeletedNodes++

				default:
					klog.Error("not supported event type")
				}

			}
		}
	}()
	wg.Wait()
	end = time.Now().UTC()
	watchStats.WatchDuration = end.Sub(start)
	if hasRegionToWatch {
		regionWatchDuration := regionWatchEnd.Sub(*regionWatchStart)
		klog.Infof("[Throughput] Time to get last event from region %v: %v. Duration %v. Event count %v", regionsToWatch, regionWatchEnd, regionWatchDuration, regionEventCount)
	}
	return
}

func addWatchLatency(delay time.Duration, ws *stats.WatchStats) {
	// Current watch latency is only summarized once. Remove lock/unlock for latency record per event to minimize performance impact
	// See PR 111.
	// If later we need to call ws.GetSummary() multiple times, lock needs to be added back.
	//ws.WatchDelayLock.Lock()
	ws.WatchDelayPerEvent.AddLatencyMetrics(delay)
	//ws.WatchDelayLock.Unlock()
}

func logIfProlonged(record *runtime.NodeEvent, delay time.Duration, ws *stats.WatchStats) {
	if delay > stats.LongWatchThreshold {
		klog.Warningf("Prolonged watch node from server: %v with time (%v)", record.Node.Id, delay)
		ws.NumberOfProlongedItems++
	}
}

func printTestStats(rs *stats.RegisterClientStats, ls *stats.ListStats, ws *stats.WatchStats) {
	rs.PrintStats()
	ls.PrintStats()
	ws.PrintStats()
}
