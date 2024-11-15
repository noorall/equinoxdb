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

package storage

import (
	"equinox/storage/store"
	"equinox/storage/types"
	"sync"
	"time"
)

func (e *Engine) enableValueFileGc() {
	e.mu.RLock()
	if e.vFileGcDone != nil {
		e.mu.RUnlock()
		return
	}
	e.mu.RUnlock()

	e.mu.Lock()
	if e.vFileGcDone != nil {
		e.mu.Unlock()
		return
	}
	e.vFileGcDone = make(chan struct{})
	wg := new(sync.WaitGroup)
	wg.Add(1)
	e.vFileGcWG = wg
	e.mu.Unlock()
	go func() {
		defer wg.Done()
		e.runValueFileGC()
	}()
}

func (e *Engine) disableValueFileGc() {
	e.mu.Lock()
	if e.vFileGcDone == nil {
		e.mu.Unlock()
		return
	}

	// We may be in the process of stopping snapshots.  See if the channel
	// was closed.
	select {
	case <-e.vFileGcDone:
		e.mu.Unlock()
		return
	default:
	}

	close(e.vFileGcDone)
	wg := e.vFileGcWG
	e.mu.Unlock()

	// Wait for the snapshot goroutine to exit.
	wg.Wait()

	// Signal that the goroutines are exit and everything is stopped by setting
	// snapDone to nil.
	e.mu.Lock()
	e.vFileGcDone = nil
	e.mu.Unlock()
}

func (e *Engine) runValueFileGC() {
	t := time.NewTicker(12 * time.Hour)
	defer t.Stop()

	for {
		e.mu.RLock()
		quit := e.vFileGcDone
		e.mu.RUnlock()
		select {
		case <-quit:
			return
		case <-t.C:
			e.runValueFileGCOnce()
		}
	}
}

func (e *Engine) runValueFileGCOnce() {
	vms := e.vFileRegionManager.GetAllVFileManager()
	for lifeCycle, vm := range vms {
		gfs := vm.GetGarbageFiles()
		for _, gf := range gfs {
			e.gcValueFile(gf, lifeCycle)
		}
	}
}

func (e *Engine) gcValueFile(vf *store.ValueFile, lifeCycle int) {
	vm, err := e.vFileRegionManager.GetVFileManager(lifeCycle)
	if err != nil {
		return
	}
	values := make(map[string][]types.Value)
	it := store.NewValueFileIterator(vf)
	for it.Next() == nil {
		key := it.ReadKey()
		keyStr := string(key)
		header := it.ReadHeader()
		var v []byte
		v, err = vm.WriteBlock(key, header.MinTime, header.MaxTime, it.ReadValueBlock())
		if err != nil {
			return
		}
		vPtr := types.NewPtrValueValue(header.MinTime, header.MaxTime, v)
		values[keyStr] = append(values[keyStr], vPtr)
	}
	err = e.mm.WriteMulti(values, e.syncWrite)
	if err != nil {
		return
	}
}
