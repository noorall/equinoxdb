/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package store

import (
	"context"
	"equinox/storage/codec"
	"equinox/storage/config"
	"equinox/storage/metric"
	"equinox/storage/types"
	"fmt"
	"sync"
)

type VFileRegionManager struct {
	vFileManagers map[int]*VFileManager

	option config.Option

	mu sync.RWMutex

	Stats *metric.ValueFileMetrics
}

func NewVFileRegionManager(option config.Option) *VFileRegionManager {
	return &VFileRegionManager{
		vFileManagers: make(map[int]*VFileManager),
		option:        option,
		Stats:         metric.NewValueFileMetrics(metric.GetEngineLabs(option)),
	}
}

func (r *VFileRegionManager) RegisterRegion(lifeCycle int) error {
	switch types.LifeCycle(lifeCycle) {
	case types.LifeCycleDefault, types.LifeCycleOneMouth, types.LifeCycleOneYear, types.LifeCycleSixMouth, types.LifeCycleFourteenDays, types.LifeCycleSevenDays:
	default:
		return fmt.Errorf("illegal lifeCycle")
	}
	if _, ok := r.vFileManagers[lifeCycle]; ok {
		return nil
	}
	vm, err := NewVFileManager(r.option, lifeCycle, r.Stats)
	if err != nil {
		return err
	}
	r.vFileManagers[lifeCycle] = vm
	return nil
}

func (r *VFileRegionManager) GetVFileManager(lifeCycle int) (*VFileManager, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if vm, ok := r.vFileManagers[lifeCycle]; ok {
		return vm, nil
	}
	return nil, fmt.Errorf("vfile not exist for lifeCycle %d", lifeCycle)
}

func (r *VFileRegionManager) GetAllVFileManager() map[int]*VFileManager {
	r.mu.Lock()
	defer r.mu.Unlock()
	var vms map[int]*VFileManager
	for k, v := range r.vFileManagers {
		vms[k] = v
	}
	return vms
}

func (r *VFileRegionManager) GetOrCreateVFileManager(lifeCycle int) (*VFileManager, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	err := r.RegisterRegion(lifeCycle)
	if err != nil {
		return nil, err
	}
	if vm, ok := r.vFileManagers[lifeCycle]; ok {
		return vm, nil
	}
	return nil, fmt.Errorf("vfile not exist for lifeCycle %d", lifeCycle)
}

func (r *VFileRegionManager) ReadDataBlocks(block []byte, markAsDelete bool, key []byte) ([][]byte, error) {
	if codec.IsPtrBlock(block) {
		vPtrs, err := DecodeValuePtrs(block[1:])
		if err != nil {
			return nil, err
		}
		wg := sync.WaitGroup{}
		dataBlocks := make([][]byte, len(vPtrs))
		errCount := 0
		// Read vFile parallelism
		for i, ptr := range vPtrs {
			wg.Add(1)
			go func(ptr *ValuePtr, idx int) {
				defer wg.Done()
				if err := r.option.ValueFileParallelismLimiter.Take(context.Background()); err != nil {
					errCount++
					return
				}
				defer r.option.ValueFileParallelismLimiter.Release()

				vm, e := r.GetVFileManager(ptr.LifeCycle)
				if e != nil {
					errCount++
					return
				}
				dataBlocks[i], e = vm.Read(ptr)
				if e != nil {
					errCount++
				} else if markAsDelete {
					_ = vm.MarkAsDelete(key, ptr)
				}
			}(ptr, i)
		}
		wg.Wait()
		if errCount != 0 {
			return nil, fmt.Errorf("error while read vfile")
		}
		return dataBlocks, nil
	}
	return [][]byte{block}, nil
}

func (r *VFileRegionManager) Close() error {
	var err error
	for _, v := range r.vFileManagers {
		err = v.Close()
	}
	return err
}
