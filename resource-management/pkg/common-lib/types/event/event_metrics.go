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

package event

import (
	"k8s.io/klog/v2"
	"sync"

	"global-resource-service/resource-management/pkg/common-lib/metrics"
)

type LatencyMetricsAllCheckpoints struct {
	Aggregator_Received  *metrics.LatencyMetrics
	Distributor_Received *metrics.LatencyMetrics
	Distributor_Sending  *metrics.LatencyMetrics
	Distributor_Sent     *metrics.LatencyMetrics
	Serializer_Encoded   *metrics.LatencyMetrics
	Serializer_Sent      *metrics.LatencyMetrics
}

var latencyMetricsAllCheckpoints *LatencyMetricsAllCheckpoints
var latencyMetricsLock sync.RWMutex

func init() {
	latencyMetricsAllCheckpoints = new(LatencyMetricsAllCheckpoints)
	latencyMetricsAllCheckpoints.Aggregator_Received = metrics.NewLatencyMetrics(int(metrics.Aggregator_Received))
	latencyMetricsAllCheckpoints.Distributor_Received = metrics.NewLatencyMetrics(int(metrics.Distributor_Received))
	latencyMetricsAllCheckpoints.Distributor_Sending = metrics.NewLatencyMetrics(int(metrics.Distributor_Sending))
	latencyMetricsAllCheckpoints.Distributor_Sent = metrics.NewLatencyMetrics(int(metrics.Distributor_Sent))
	latencyMetricsAllCheckpoints.Serializer_Encoded = metrics.NewLatencyMetrics(int(metrics.Serializer_Encoded))
	latencyMetricsAllCheckpoints.Serializer_Sent = metrics.NewLatencyMetrics(int(metrics.Serializer_Sent))
}

func AddLatencyMetricsAllCheckpoints(e *NodeEvent) {
	if !metrics.ResourceManagementMeasurement_Enabled {
		return
	}
	if e == nil {
		klog.Error("Nil event")
	}
	checkpointsPerEvent := e.GetCheckpoints()
	if checkpointsPerEvent == nil {
		klog.Errorf("Event (%v, Id %s, RV %s) does not have checkpoint stamped", e.Type, e.Node.Id, e.Node.ResourceVersion)
	}
	lastUpdatedTime := e.Node.LastUpdatedTime

	agg_received_time := checkpointsPerEvent[metrics.Aggregator_Received]
	dis_received_time := checkpointsPerEvent[metrics.Distributor_Received]
	dis_sending_time := checkpointsPerEvent[metrics.Distributor_Sending]
	dis_sent_time := checkpointsPerEvent[metrics.Distributor_Sent]
	serializer_encoded_time := checkpointsPerEvent[metrics.Serializer_Encoded]
	serializer_sent_time := checkpointsPerEvent[metrics.Serializer_Sent]

	latencyMetricsLock.Lock()
	defer latencyMetricsLock.Unlock()
	if !agg_received_time.IsZero() {
		latencyMetricsAllCheckpoints.Aggregator_Received.AddLatencyMetrics(agg_received_time.Sub(lastUpdatedTime))
	} else {
		klog.Errorf("Event (%v, Id %s, RV %s) does not have %v stamped", e.Type, e.Node.Id, e.Node.ResourceVersion, metrics.Aggregator_Received)
	}
	if !dis_received_time.IsZero() {
		latencyMetricsAllCheckpoints.Distributor_Received.AddLatencyMetrics(dis_received_time.Sub(lastUpdatedTime))
	} else {
		klog.Errorf("Event (%v, Id %s, RV %s) does not have %v stamped", e.Type, e.Node.Id, e.Node.ResourceVersion, metrics.Distributor_Received)
	}
	if !dis_sending_time.IsZero() {
		latencyMetricsAllCheckpoints.Distributor_Sending.AddLatencyMetrics(dis_sending_time.Sub(lastUpdatedTime))
	} else {
		klog.Errorf("Event (%v, Id %s, RV %s) does not have %v stamped", e.Type, e.Node.Id, e.Node.ResourceVersion, metrics.Distributor_Sending)
	}
	if !dis_sent_time.IsZero() {
		latencyMetricsAllCheckpoints.Distributor_Sent.AddLatencyMetrics(dis_sent_time.Sub(lastUpdatedTime))
	} else {
		klog.Errorf("Event (%v, Id %s, RV %s) does not have %v stamped", e.Type, e.Node.Id, e.Node.ResourceVersion, metrics.Distributor_Sent)
	}
	if !serializer_encoded_time.IsZero() {
		latencyMetricsAllCheckpoints.Serializer_Encoded.AddLatencyMetrics(serializer_encoded_time.Sub(lastUpdatedTime))
	} else {
		klog.Errorf("Event (%v, Id %s, RV %s) does not have %v stamped", e.Type, e.Node.Id, e.Node.ResourceVersion, metrics.Serializer_Encoded)
	}
	if !serializer_sent_time.IsZero() {
		latencyMetricsAllCheckpoints.Serializer_Sent.AddLatencyMetrics(serializer_sent_time.Sub(lastUpdatedTime))
	} else {
		klog.Errorf("Event (%v, Id %s, RV %s) does not have %v stamped", e.Type, e.Node.Id, e.Node.ResourceVersion, metrics.Serializer_Sent)
	}
	klog.V(6).Infof("[Metrics][Detail] node %v RV %s: %s: %v, %s: %v, %s: %v, %s: %v, %s: %v, %s: %v",
		e.Node.Id, e.Node.ResourceVersion,
		metrics.Aggregator_Received_Name, agg_received_time.Sub(lastUpdatedTime),
		metrics.Distributor_Received_Name, dis_received_time.Sub(lastUpdatedTime),
		metrics.Distributor_Sending_Name, dis_sending_time.Sub(lastUpdatedTime),
		metrics.Distributor_Sent_Name, dis_sent_time.Sub(lastUpdatedTime),
		metrics.Serializer_Encoded_Name, serializer_encoded_time.Sub(lastUpdatedTime),
		metrics.Serializer_Sent_Name, serializer_sent_time.Sub(lastUpdatedTime))
}

func PrintLatencyReport() {
	latencyMetricsLock.RLock()
	agg_received_summary := latencyMetricsAllCheckpoints.Aggregator_Received.GetSummary()
	dis_received_summary := latencyMetricsAllCheckpoints.Distributor_Received.GetSummary()
	dis_sending_summary := latencyMetricsAllCheckpoints.Distributor_Sending.GetSummary()
	dis_sent_summary := latencyMetricsAllCheckpoints.Distributor_Sent.GetSummary()
	serializer_encoded_summary := latencyMetricsAllCheckpoints.Serializer_Encoded.GetSummary()
	serializer_sent_summary := latencyMetricsAllCheckpoints.Serializer_Sent.GetSummary()

	latencyMetricsLock.RUnlock()
	metrics_Message := "[Metrics][%s] perc50 %v, perc90 %v, perc99 %v. Total count %v"
	klog.Infof(metrics_Message, metrics.Aggregator_Received_Name, agg_received_summary.P50, agg_received_summary.P90, agg_received_summary.P99, agg_received_summary.TotalCount)
	klog.Infof(metrics_Message, metrics.Distributor_Received_Name, dis_received_summary.P50, dis_received_summary.P90, dis_received_summary.P99, dis_received_summary.TotalCount)
	klog.Infof(metrics_Message, metrics.Distributor_Sending_Name, dis_sending_summary.P50, dis_sending_summary.P90, dis_sending_summary.P99, dis_sending_summary.TotalCount)
	klog.Infof(metrics_Message, metrics.Distributor_Sent_Name, dis_sent_summary.P50, dis_sent_summary.P90, dis_sent_summary.P99, dis_sent_summary.TotalCount)
	klog.Infof(metrics_Message, metrics.Serializer_Encoded_Name, serializer_encoded_summary.P50, serializer_encoded_summary.P90, serializer_encoded_summary.P99, serializer_encoded_summary.TotalCount)
	klog.Infof(metrics_Message, metrics.Serializer_Sent_Name, serializer_sent_summary.P50, serializer_sent_summary.P90, serializer_sent_summary.P99, serializer_sent_summary.TotalCount)
}
