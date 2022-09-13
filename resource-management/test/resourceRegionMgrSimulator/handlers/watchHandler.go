/*
Copyright The Kubernetes Authors.
Copyright 2022 Authors of Global Resource Service - file modified.
Copyright 2020 Authors of Arktos - file modified.

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

package handlers

import (
	"encoding/json"
	"io/ioutil"
	"k8s.io/klog/v2"
	"net/http"

	"global-resource-service/resource-management/pkg/common-lib/types"
	"global-resource-service/resource-management/pkg/common-lib/types/event"
	ep "global-resource-service/resource-management/pkg/service-api/endpoints"
	apiTypes "global-resource-service/resource-management/pkg/service-api/types"
	"global-resource-service/resource-management/test/resourceRegionMgrSimulator/data"
	simulatorTypes "global-resource-service/resource-management/test/resourceRegionMgrSimulator/types"
)

type WatchHandler struct{}

func NewWatchHandler() *WatchHandler {
	return &WatchHandler{}
}

func (w *WatchHandler) ResourceHandler(resp http.ResponseWriter, req *http.Request) {
	klog.V(3).Infof("handle /resources. URL path: %s", req.URL.Path)

	switch req.Method {
	case http.MethodGet:
		w.list(resp, req)
		return

	// hack: currently crv is used for watch watermark, this is up to 200 RPs which cannot fit as parameters or headers
	// unfortunately GET will return 405 with request body.
	// It's unlikely we can change to other solutions for now. so use POST to test verify the watch logic and flows for now.
	// TODO: switch to logical record or other means to set the water mark as query parameter
	case http.MethodPost:
		if req.URL.Query().Get(ep.WatchParameter) == ep.WatchParameterTrue {
			w.serverWatch(resp, req, "aggregator") // TODO: when scale put GRS, add clientId to differentiate the GRS
			return
		}
		return
	case http.MethodPut:
		resp.WriteHeader(http.StatusMethodNotAllowed)
		return
	default:
		resp.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

}

func (w *WatchHandler) list(resp http.ResponseWriter, req *http.Request) {
	nodeEvents, count, _ := data.ListNodes()

	if count == 0 {
		klog.V(6).Info("Pulling Region Node Events with batch is in the end")
	} else {
		klog.V(6).Infof("Pulling Region Node Event with final batch size (%v) for (%v) RPs", count, len(nodeEvents))
	}

	response := &simulatorTypes.ResponseFromRRM{
		RegionNodeEvents: nodeEvents,
		RvMap:            nil,
		Length:           uint64(count),
	}

	// Serialize region node events result to JSON
	err := response.ToJSON(resp)

	if err != nil {
		klog.Errorf("Error - Unable to marshal json : ", err)
	}
}

// simple watch routine
// TODO: add timeout support
// TODO: with serialization options
// TODO: error code and string definition
//
func (w *WatchHandler) serverWatch(resp http.ResponseWriter, req *http.Request, clientId string) {
	klog.V(3).Infof("Serving watch for client: %s", clientId)

	channelSize := 25000
	// TODO: per perf test results, adjust the channel buffer size
	watchCh := make(chan *event.NodeEvent, channelSize /*ep.WatchChannelSize*/)
	stopCh := make(chan struct{})

	// Signal the distributor to stop the watch for this client on exit
	defer stopWatch(stopCh)

	// read request body and get the crv
	crvMap, err := getResourceVersionsMap(req)
	if err != nil {
		klog.Errorf("unable to get the resource versions. Error %v", err)
		resp.WriteHeader(http.StatusInternalServerError)
		return
	}

	klog.V(9).Infof("Received CRV: %v", crvMap)

	// start the watcher
	klog.V(3).Infof("Start watching resource changes for client: %v", clientId)
	err = data.Watch(crvMap, watchCh, stopCh)
	if err != nil {
		klog.Errorf("unable to start the watcher. Error %v", err)
		resp.WriteHeader(http.StatusInternalServerError)
		return
	}

	done := req.Context().Done()
	flusher, ok := resp.(http.Flusher)
	if !ok {
		klog.Errorf("unable to start watch - can't get http.Flusher")
		resp.WriteHeader(http.StatusInternalServerError)
		return
	}

	// begin the stream
	resp.Header().Set("Content-Type", req.Header.Get("Content-Type"))
	resp.Header().Set("Transfer-Encoding", "chunked")
	resp.WriteHeader(http.StatusOK)
	flusher.Flush()

	klog.V(3).Infof("Start processing watch event for client: %v", clientId)
	i := 0
	flushBatchSize := 100 // optimized for daily change pattern
	for {
		select {
		case <-done:
			return
		case record, ok := <-watchCh:
			if !ok {
				// End of results.
				klog.Infof("End of results")
				return
			}

			klog.V(6).Infof("Getting event from resource providers, node Id: %v", record.Node.Id)

			if err := json.NewEncoder(resp).Encode(*record); err != nil {
				klog.Errorf("encoding record failed. error %v", err)
				resp.WriteHeader(http.StatusInternalServerError)
				return
			}

			i++
			if i == flushBatchSize || len(watchCh) == 0 {
				flusher.Flush()
				i = 0
			}
		}
	}
}

// Helper functions
func stopWatch(stopCh chan struct{}) {
	stopCh <- struct{}{}
}

func getResourceVersionsMap(req *http.Request) (types.TransitResourceVersionMap, error) {
	body, err := ioutil.ReadAll(req.Body)

	if err != nil {
		klog.Errorf("Failed read request body, error %v", err)
		return nil, err
	}

	wr := apiTypes.WatchRequest{}

	err = json.Unmarshal(body, &wr)
	if err != nil {
		klog.Errorf("Failed unmarshal request body, error %v", err)
		return nil, err
	}

	return wr.ResourceVersions, nil
}
