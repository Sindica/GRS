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
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"global-resource-service/resource-management/pkg/common-lib/hash"
	"global-resource-service/resource-management/pkg/common-lib/types"
	"global-resource-service/resource-management/pkg/common-lib/types/location"
	"global-resource-service/resource-management/pkg/common-lib/types/runtime"
	"global-resource-service/resource-management/pkg/distributor/node"
	"math"
	"strconv"
	"sync"
	"testing"
)

func TestAdjustCapacity_1stSplit(t *testing.T) {
	for i := 25; i < 80; i++ {
		for j := 1; j < i; j++ {
			vs := generateInitVirtalNodes(i)
			lowerBoundOld := vs.lowerbound
			upperBoundOld := vs.upperbound
			vsPointerOld := vs

			vs.AdjustCapacity(vs, j)

			// check result
			assert.Nil(t, vs.parentVirtualNodeStore)
			assert.Equal(t, j, len(vs.nodeEventByHash))

			assert.Equal(t, 2, len(vs.splittVirtualNodeStores))
			assert.NotEqual(t, vs.splittVirtualNodeStores[0], vs.splittVirtualNodeStores[1])
			assert.Equal(t, i, len(vs.splittVirtualNodeStores[0].nodeEventByHash)+len(vs.splittVirtualNodeStores[1].nodeEventByHash))

			assert.True(t, vs.splittVirtualNodeStores[0].adjustedLowerBound < vs.splittVirtualNodeStores[1].adjustedLowerBound)
			assert.True(t, vs.splittVirtualNodeStores[0].adjustedUpperBound < vs.splittVirtualNodeStores[1].adjustedUpperBound)
			assert.Equal(t, vs.splittVirtualNodeStores[0].adjustedUpperBound, vs.splittVirtualNodeStores[1].adjustedLowerBound)

			// init vs has immutable lower & upper bound
			assert.Equal(t, lowerBoundOld, vs.lowerbound)
			assert.Equal(t, upperBoundOld, vs.upperbound)
			assert.Equal(t, vs.lowerbound, vs.splittVirtualNodeStores[0].adjustedLowerBound)
			assert.Equal(t, vs.upperbound, vs.splittVirtualNodeStores[1].adjustedUpperBound)
			vs.mu.Lock()
			vs.mu.Unlock()

			// has one and only one store as parent
			assert.Equal(t, vsPointerOld, vs)
			assert.Nil(t, vs.splittVirtualNodeStores[0].parentVirtualNodeStore)
			assert.Equal(t, vs, vs.splittVirtualNodeStores[1].parentVirtualNodeStore)

			assert.Equal(t, len(vs.nodeEventByHash), vs.GetHostNum())
		}
	}
}

func TestAdjustCapacity_Merge(t *testing.T) {
	// one
}

func generateInitVirtalNodes(hostNum int) *VirtualNodeStore {
	loc := location.NewLocation(location.Beijing, location.ResourcePartition2)
	vs := &VirtualNodeStore{
		mu:              sync.RWMutex{},
		nodeEventByHash: make(map[float64]*node.ManagedNodeEvent, hostNum),
		location:        *loc,
	}

	lowerBound := float64(math.MaxUint64)
	upperBound := float64(-1)
	for i := 0; i < hostNum; i++ {
		n := createRandomNode(i, loc)
		ne := runtime.NewNodeEvent(n, runtime.Added)
		mgmtNE := node.NewManagedNodeEvent(ne, loc)
		hashValue := float64(hash.HashStrToUInt64(mgmtNE.GetId())) / math.MaxUint64 * 360
		vs.nodeEventByHash[hashValue] = mgmtNE

		if lowerBound > hashValue {
			lowerBound = hashValue
		}
		if upperBound < hashValue {
			upperBound = hashValue
		}
	}

	vs.lowerbound = lowerBound
	// upperBound is exclusive, increase a bit
	vs.upperbound = upperBound + 1

	return vs
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
