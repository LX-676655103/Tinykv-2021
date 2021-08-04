// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package schedulers

import (
	"github.com/pingcap-incubator/tinykv/scheduler/server/core"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/operator"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/opt"
	"sort"
)

func init() {
	schedule.RegisterSliceDecoderBuilder("balance-region", func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			return nil
		}
	})
	schedule.RegisterScheduler("balance-region", func(opController *schedule.OperatorController, storage *core.Storage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		return newBalanceRegionScheduler(opController), nil
	})
}

const (
	// balanceRegionRetryLimit is the limit to retry schedule for selected store.
	balanceRegionRetryLimit = 10
	balanceRegionName       = "balance-region-scheduler"
)

type balanceRegionScheduler struct {
	*baseScheduler
	name         string
	opController *schedule.OperatorController
}

// newBalanceRegionScheduler creates a scheduler that tends to keep regions on
// each store balanced.
func newBalanceRegionScheduler(opController *schedule.OperatorController, opts ...BalanceRegionCreateOption) schedule.Scheduler {
	base := newBaseScheduler(opController)
	s := &balanceRegionScheduler{
		baseScheduler: base,
		opController:  opController,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// BalanceRegionCreateOption is used to create a scheduler with an option.
type BalanceRegionCreateOption func(s *balanceRegionScheduler)

func (s *balanceRegionScheduler) GetName() string {
	if s.name != "" {
		return s.name
	}
	return balanceRegionName
}

func (s *balanceRegionScheduler) GetType() string {
	return "balance-region"
}

func (s *balanceRegionScheduler) IsScheduleAllowed(cluster opt.Cluster) bool {
	return s.opController.OperatorCount(operator.OpRegion) < cluster.GetRegionScheduleLimit()
}

func (s *balanceRegionScheduler) Schedule(cluster opt.Cluster) *operator.Operator {
	// Your Code Here (3C).
	// select proper store & sort stores
	tempStores := cluster.GetStores()
	stores := make([]*core.StoreInfo, 0)
	for _, store := range tempStores {
		// should be up and the down time cannot be longer than MaxStoreDownTime of the cluster
		if store.IsUp() && store.DownTime() <= cluster.GetMaxStoreDownTime() {
			stores = append(stores, store)
		}
	}
	// cannot move
	if len(stores) <= 1 {
		return nil
	}
	sort.Slice(stores, func(i, j int) bool {
		return stores[i].GetRegionSize() > stores[j].GetRegionSize()
	})
	// Scheduler tries to find regions to move from the store with the biggest region size.
	var regionInfo *core.RegionInfo
	source := stores[0]
	for i := 0; i < len(stores)-1; i++ {
		source = stores[i]
		// try to select a pending region
		cluster.GetPendingRegionsWithLock(stores[i].GetID(), func(regions core.RegionsContainer) {
			regionInfo = regions.RandomRegion(nil, nil)
		})
		if regionInfo != nil {
			break
		}
		// If there isn’t a pending region, it will try to find a follower region.
		cluster.GetFollowersWithLock(stores[i].GetID(), func(regions core.RegionsContainer) {
			regionInfo = regions.RandomRegion(nil, nil)
		})
		if regionInfo != nil {
			break
		}
		// If it still cannot pick out one region, it will try to pick leader regions.
		cluster.GetLeadersWithLock(stores[i].GetID(), func(regions core.RegionsContainer) {
			regionInfo = regions.RandomRegion(nil, nil)
		})
		if regionInfo != nil {
			break
		}
		// If it still cannot make it, Scheduler will try the next store which has a smaller region size
		// until all stores will have been tried.
	}
	if regionInfo == nil {
		return nil
	}
	storeIds := regionInfo.GetStoreIds()
	if len(storeIds) < cluster.GetMaxReplicas() {
		return nil
	}
	// select the store with the smallest region size
	regionStores := cluster.GetRegionStores(regionInfo)
	targets := make([]*core.StoreInfo, 0)
	for _, store := range stores {
		isInRegionStores := false
		for _, local := range regionStores {
			if local.GetID() == store.GetID() {
				isInRegionStores = true
				break
			}
		}
		if !isInRegionStores {
			targets = append(targets, store)
		}
	}
	if len(targets) == 0 {
		return nil
	}
	sort.Slice(targets, func(i, j int) bool {
		return targets[i].GetRegionSize() < targets[j].GetRegionSize()
	})
	target := targets[0]
	// judge whether this movement is valuable
	// the difference of region size has to be bigger than
	// two times the approximate size of the region
	difference := source.GetRegionSize() - target.GetRegionSize()
	if difference <= 2*regionInfo.GetApproximateSize() {
		return nil
	}
	// allocate a new peer on the target store
	newPeer, err := cluster.AllocPeer(target.GetID())
	if err != nil {
		return nil
	}
	// create a move peer operator
	peerOperator, err := operator.CreateMovePeerOperator("balance-region", cluster,
		regionInfo, operator.OpBalance, source.GetID(), target.GetID(), newPeer.GetId())
	if err != nil {
		return nil
	}
	return peerOperator
}
