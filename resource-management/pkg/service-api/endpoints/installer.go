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

package endpoints

import (
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"io/ioutil"
	"k8s.io/klog/v2"
	"net/http"
	"strconv"
	"strings"

	di "global-resource-service/resource-management/pkg/common-lib/interfaces/distributor"
	store "global-resource-service/resource-management/pkg/common-lib/interfaces/store"
	"global-resource-service/resource-management/pkg/common-lib/metrics"
	"global-resource-service/resource-management/pkg/common-lib/types"
	"global-resource-service/resource-management/pkg/common-lib/types/event"
	"global-resource-service/resource-management/pkg/common-lib/types/location"
	apiTypes "global-resource-service/resource-management/pkg/service-api/types"
)

type Installer struct {
	dist di.Interface
}

func NewInstaller(d di.Interface) *Installer {
	return &Installer{d}
}

func (i *Installer) ClientAdministrationHandler(resp http.ResponseWriter, req *http.Request) {
	klog.V(3).Infof("handle /client. URL path: %s", req.URL.Path)

	switch req.Method {
	case http.MethodPost:
		i.handleClientRegistration(resp, req)
		return
	case http.MethodDelete:
		i.handleClientUnRegistration(resp, req)
		return
	default:
		resp.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

}

// TODO: error handling function
func (i *Installer) handleClientRegistration(resp http.ResponseWriter, req *http.Request) {
	klog.Infof("handle client registration")
	body, err := ioutil.ReadAll(req.Body)
	if err != nil {
		klog.V(3).Infof("error read request. error %v", err)
		resp.WriteHeader(http.StatusBadRequest)
		return
	}

	clientReq := apiTypes.ClientRegistrationRequest{}

	err = json.Unmarshal(body, &clientReq)
	if err != nil {
		klog.V(3).Infof("error unmarshal request body. error %v", err)
		resp.WriteHeader(http.StatusInternalServerError)
		return
	}

	requestedMachines := clientReq.InitialRequestedResource.TotalMachines
	if requestedMachines > types.MaxTotalMachinesPerRequest || requestedMachines < types.MinTotalMachinesPerRequest {
		klog.V(3).Infof("Invalid request of resources. requested total machines: %v", requestedMachines)
		resp.WriteHeader(http.StatusBadRequest)
		return
	}

	// TODO: need to design to avoid client to register itself
	c_id := fmt.Sprintf("%s-%s", store.Preserve_Client_KeyPrefix, uuid.New().String())
	client := types.Client{ClientId: c_id, Resource: clientReq.InitialRequestedResource, ClientInfo: clientReq.ClientInfo}

	err = i.dist.RegisterClient(&client)

	if err != nil {
		klog.V(3).Infof("error register client. error %v", err)
		resp.WriteHeader(http.StatusInternalServerError)
		return
	}

	// for 630, request of initial resource request with client registration is either denied or granted in full
	ret := apiTypes.ClientRegistrationResponse{ClientId: client.ClientId, GrantedResource: client.Resource}

	b, err := json.Marshal(ret)
	if err != nil {
		klog.V(3).Infof("error marshal client response. error %v", err)
		resp.WriteHeader(http.StatusInternalServerError)
		return
	}

	_, err = resp.Write(b)
	if err != nil {
		klog.V(3).Infof("error write response. error %v", err)
		resp.WriteHeader(http.StatusInternalServerError)
		return
	}

	return
}

func (i *Installer) handleClientUnRegistration(resp http.ResponseWriter, req *http.Request) {
	klog.V(3).Infof("not implemented")
	resp.WriteHeader(http.StatusNotImplemented)
	return
}

func (i *Installer) NodeHandler(resp http.ResponseWriter, req *http.Request) {
	klog.V(3).Infof("handle /resource/nodes/. URL path: %s", req.URL.Path)

	switch req.Method {
	case http.MethodGet:
		regionName, rpName, nodeId := getNodeId(req)
		resp.WriteHeader(http.StatusOK)
		resp.Header().Set("Content-Type", "text/plain")

		region := location.GetRegionFromRegionName(regionName)
		resourceParition, err := location.GetPartitionFromPartitionName(rpName)
		if err != nil {
			resp.WriteHeader(http.StatusBadRequest)
			return
		}

		node, err := i.dist.GetNodeStatus(region, resourceParition, nodeId)
		if err == types.Error_ObjectNotFound {
			resp.WriteHeader(http.StatusNotFound)
		} else if err != nil {
			klog.Errorf("Error getting node status: region %v, rp %v, nodeId %s, error [%v]", regionName, rpName, nodeId, err)
			resp.WriteHeader(http.StatusInternalServerError)
		} else {
			ret := apiTypes.NodeResponse{Node: *node}
			b, err := json.Marshal(ret)
			if err != nil {
				klog.V(3).Infof("error marshal client response. error %v", err)
				resp.WriteHeader(http.StatusInternalServerError)
				return
			}

			_, err = resp.Write(b)
			if err != nil {
				klog.V(3).Infof("error write response. error %v", err)
				resp.WriteHeader(http.StatusInternalServerError)
				return
			}

			return
		}
	default:
		resp.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
}

func (i *Installer) ResourceHandler(resp http.ResponseWriter, req *http.Request) {
	klog.V(3).Infof("handle /resource. URL path: %s", req.URL.Path)

	switch req.Method {
	case http.MethodGet:
		clientId := getClientId(req)
		klog.Infof("Handle resource for client: %v", clientId)

		resp.WriteHeader(http.StatusOK)
		resp.Header().Set("Content-Type", "text/plain")

		nodes, crv, err := i.dist.ListNodesForClient(clientId)
		if err != nil {
			klog.V(3).Infof("error to get node list from distributor. error %v", err)
			resp.WriteHeader(http.StatusInternalServerError)
			return
		}
		limit := req.URL.Query().Get(ListLimitParameter)
		var chunkSize int
		if len(limit) > 0 {
			chunkSize, err = strconv.Atoi(limit)
			if err != nil {
				klog.Errorf("invalid limit value")
				resp.WriteHeader(http.StatusBadRequest)
				return
			}
		}

		i.handleResponseTrunked(resp, nodes, crv, chunkSize)
		return
	// hack: currently crv is used for watch watermark, this is up to 200 RPs which cannot fit as parameters or headers
	// unfortunately GET will return 405 with request body.
	// It's unlikely we can change to other solutions for now. so use POST to test verify the watch logic and flows for now.
	// TODO: switch to logical record or other means to set the water mark as query parameter
	case http.MethodPost:
		clientId := getClientId(req)
		klog.Infof("Handle resource for client: %v", clientId)

		if req.URL.Query().Get(WatchParameter) == WatchParameterTrue {
			i.serverWatch(resp, req, clientId)
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

// simple watch routine
// TODO: add timeout support
// TODO: with serialization options
// TODO: error code and string definition
//
func (i *Installer) serverWatch(resp http.ResponseWriter, req *http.Request, clientId string) {
	klog.V(3).Infof("Serving watch for client: %s", clientId)

	// For 630 distributor impl, watchChannel and stopChannel passed into the Watch routine from API layer
	watchCh := make(chan *event.NodeEvent, WatchChannelSize)
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
	klog.V(3).Infof("Start watching distributor for client: %v", clientId)
	err = i.dist.Watch(clientId, crvMap, watchCh, stopCh)
	if err != nil {
		klog.Errorf("unable to start the watch at store. Error %v", err)
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
	flushBatchSize := 10
	n := 0
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

			klog.V(6).Infof("Getting event from distributor, node Id: %v", record.Node.Id)

			if err := json.NewEncoder(resp).Encode(*record); err != nil {
				klog.V(3).Infof("encoding record failed. error %v", err)
				resp.WriteHeader(http.StatusInternalServerError)
				return
			}
			n++
			record.SetCheckpoint(metrics.Serializer_Encoded)
			if n == flushBatchSize || len(watchCh) == 0 {
				flusher.Flush()
				n = 0
			}
			record.SetCheckpoint(metrics.Serializer_Sent)
			event.AddLatencyMetricsAllCheckpoints(record)
		}
	}
}

// Helper functions
func stopWatch(stopCh chan struct{}) {
	stopCh <- struct{}{}
}

// get clientId from url path
func getClientId(req *http.Request) string {
	// urlpath is fixed: "/resource/clientid"
	clientId := strings.Split(req.URL.Path, "/")[2]
	// watch url path "/resource/clientid?watch=true"
	clientId = strings.Split(clientId, "?")[0]

	return clientId
}

// get nodeId from url path
func getNodeId(req *http.Request) (string, string, string) {
	// urlpath is fixed "/nodes?nodeId=&region=&resourcePartition="
	nodeId := req.URL.Query().Get("nodeId")
	regionName := req.URL.Query().Get("region")
	resourceParitionName := req.URL.Query().Get("resourcePartition")
	return regionName, resourceParitionName, nodeId
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

func (i *Installer) handleResponseTrunked(resp http.ResponseWriter, nodes []*types.LogicalNode, crv types.TransitResourceVersionMap, chunkSize int) {
	responseTrunkSize := DefaultResponseTrunkSize
	if responseTrunkSize < chunkSize {
		responseTrunkSize = chunkSize
	}
	klog.Infof("Serve with chunk size: %v", responseTrunkSize)
	var nodesLen = len(nodes)
	if nodesLen <= responseTrunkSize {
		listResp := apiTypes.ListNodeResponse{NodeList: nodes, ResourceVersions: crv}
		ret, err := json.Marshal(listResp)
		if err != nil {
			klog.Errorf("error read get node list. error %v", err)
			resp.WriteHeader(http.StatusInternalServerError)
			return
		}
		resp.Write(ret)
	} else {
		flusher, ok := resp.(http.Flusher)
		if !ok {
			klog.Errorf("expected http.ResponseWriter to be an http.Flusher")
		}

		resp.Header().Set("Connection", "Keep-Alive")
		resp.Header().Set("X-Content-Type-Options", "nosniff")
		//TODO: handle network disconnect or similar cases.
		var chunkedNodes []*types.LogicalNode
		start := 0
		for start < nodesLen {
			end := start + responseTrunkSize
			if end < nodesLen {
				chunkedNodes = nodes[start:end]
			} else {
				chunkedNodes = nodes[start:nodesLen]
			}

			// TODO: optimization: only sent the crv at the last chunck
			listResp := apiTypes.ListNodeResponse{NodeList: chunkedNodes, ResourceVersions: crv}
			ret, err := json.Marshal(listResp)
			if err != nil {
				klog.Errorf("error read get node list. error %v", err)
				resp.WriteHeader(http.StatusInternalServerError)
				return
			}

			klog.Infof("CRV: %v", listResp.ResourceVersions)
			resp.Write(ret)
			flusher.Flush()
			start = end
		}
	}
	return
}
