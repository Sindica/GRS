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

package cache

import (
	"errors"
	"fmt"
	"global-resource-service/resource-management/test/resourceRegionMgrSimulator/config"
	"k8s.io/klog/v2"
	"sort"
	"sync"

	"global-resource-service/resource-management/pkg/common-lib/types"
	"global-resource-service/resource-management/pkg/common-lib/types/event"
)

const LengthOfNodeEventQueue = 10000

type nodeEventQueueByRP struct {
	circularEventQueue []*event.NodeEvent
	// circular event queue start position and end position
	startPos int
	endPos   int

	// mutex for event queue operation
	eqLock sync.RWMutex
}

func newNodeQueueByRP() *nodeEventQueueByRP {
	return &nodeEventQueueByRP{
		circularEventQueue: make([]*event.NodeEvent, LengthOfNodeEventQueue),
		startPos:           0,
		endPos:             0,
		eqLock:             sync.RWMutex{},
	}
}

func (q *nodeEventQueueByRP) enqueueEvent(e *event.NodeEvent) {
	q.eqLock.Lock()
	defer q.eqLock.Unlock()

	if q.endPos == q.startPos+LengthOfNodeEventQueue {
		// cache is full - remove the oldest element
		q.startPos++
	}

	q.circularEventQueue[q.endPos%LengthOfNodeEventQueue] = e
	q.endPos++
}

func (q *nodeEventQueueByRP) getEventsFromIndex(startIndex int) ([]*event.NodeEvent, error) {
	q.eqLock.RLock()
	defer q.eqLock.RUnlock()

	if q.startPos == q.endPos || q.startPos > startIndex || startIndex > q.endPos { // queue is empty or out of range
		return nil, errors.New(fmt.Sprintf("Event queue start pos %d, end pos %d, invalid start index %d", q.startPos, q.endPos, startIndex))
	}

	length := q.endPos - startIndex
	result := make([]*event.NodeEvent, length)
	for i := 0; i < length; i++ {
		result[i] = q.circularEventQueue[(startIndex+i)%LengthOfNodeEventQueue]
	}

	return result, nil
}

func (q *nodeEventQueueByRP) getEventIndexSinceResourceVersion(resourceVersion uint64) (int, error) {
	q.eqLock.RLock()
	defer q.eqLock.RUnlock()
	if q.endPos-q.startPos == 0 {
		return -1, errors.New(fmt.Sprintf("Empty event queue"))
	}
	nodeEvent := q.circularEventQueue[q.startPos%LengthOfNodeEventQueue].Node

	oldestRV := nodeEvent.GetResourceVersionInt64()
	if oldestRV > resourceVersion {
		return -1, errors.New(fmt.Sprintf("Resource Partition %s events oldest resource Version %d is newer than requested resource version %d",
			nodeEvent.GeoInfo.ResourcePartition, oldestRV, resourceVersion))
	}

	index := sort.Search(q.endPos-q.startPos, func(i int) bool {
		return q.circularEventQueue[(q.startPos+i)%LengthOfNodeEventQueue].Node.GetResourceVersionInt64() > resourceVersion
	})
	index += q.startPos
	if index == q.endPos {
		return -1, types.Error_EndOfEventQueue
	}
	if index > q.endPos || index < q.startPos {
		return -1, errors.New(fmt.Sprintf("Event queue start pos %d, end pos %d, found invalid start index %d", q.startPos, q.endPos, index))
	}
	return index, nil
}

type NodeEventQueue struct {
	watchChan chan *event.NodeEvent

	// used to lock enqueue operation during snapshot
	enqueueLock sync.RWMutex

	eventQueueByRP []*nodeEventQueueByRP
}

func NewNodeEventQueue(resourcePartitionNum int) *NodeEventQueue {
	queue := &NodeEventQueue{
		eventQueueByRP: make([]*nodeEventQueueByRP, resourcePartitionNum),
		enqueueLock:    sync.RWMutex{},
	}

	for i := 0; i < resourcePartitionNum; i++ {
		queue.eventQueueByRP[i] = newNodeQueueByRP()
	}

	return queue
}

func (eq *NodeEventQueue) AcquireSnapshotRLock() {
	eq.enqueueLock.RLock()
}

func (eq *NodeEventQueue) ReleaseSnapshotRLock() {
	eq.enqueueLock.RUnlock()
}

func (eq *NodeEventQueue) EnqueueEvent(e *event.NodeEvent) {
	eq.enqueueLock.Lock()
	defer eq.enqueueLock.Unlock()
	if eq.watchChan != nil {
		go func() {
			eq.watchChan <- e
		}()
	}

	eq.eventQueueByRP[e.Node.GeoInfo.ResourcePartition].enqueueEvent(e)
}

func (eq *NodeEventQueue) Watch(rvs types.InternalResourceVersionMap, clientWatchChan chan *event.NodeEvent, stopCh chan struct{}) error {
	if eq.watchChan != nil {
		return errors.New("currently only support one watcher per node event queue")
	}

	// get events already in queues
	events, err := eq.getAllEventsSinceResourceVersion(rvs)
	if err != nil {
		return err
	}

	eq.watchChan = make(chan *event.NodeEvent)
	// writing event to channel
	go func(downstreamCh chan *event.NodeEvent, initEvents []*event.NodeEvent, stopCh chan struct{}, upstreamCh chan *event.NodeEvent) {
		if downstreamCh == nil {
			return
		}
		// send init events
		for i := 0; i < len(initEvents); i++ {
			klog.Infof("debug: init events")
			downstreamCh <- initEvents[i]
		}

		// continue to watch
		for {
			select {
			case <-stopCh:
				eq.watchChan = nil
				klog.V(3).Infof("Watch stopped due to client request")
				return
			case event, ok := <-upstreamCh:
				if !ok {
					break
				}
				klog.V(9).Infof("Sending event with node id %v", event.Node.Id)
				downstreamCh <- event
				klog.V(9).Infof("Event with node id %v sent", event.Node.Id)
			}
		}

	}(clientWatchChan, events, stopCh, eq.watchChan)

	return nil
}

func (eq *NodeEventQueue) getAllEventsSinceResourceVersion(rvs types.InternalResourceVersionMap) ([]*event.NodeEvent, error) {
	locStartPostitions := make([]int, config.RpNum)

	for loc, rv := range rvs {
		qByRP := eq.eventQueueByRP[loc.GetResourcePartition()]
		startIndex, err := qByRP.getEventIndexSinceResourceVersion(rv)
		if err != nil {
			if err == types.Error_EndOfEventQueue {
				return nil, nil
			}
			return nil, err
		}
		locStartPostitions[loc.GetResourcePartition()] = startIndex
	}

	nodeEvents := make([]*event.NodeEvent, 0)
	for rp, qByRP := range eq.eventQueueByRP {
		startIndex := locStartPostitions[rp]
		events, err := qByRP.getEventsFromIndex(startIndex)
		if err != nil {
			return nil, err
		}

		if len(events) > 0 {
			nodeEvents = append(nodeEvents, events...)
		}
	}

	return nodeEvents, nil
}
