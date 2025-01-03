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

package memory

import (
	"equinox/storage/config"
	"equinox/storage/errs"
	"equinox/storage/metric"
	"equinox/storage/store"
	"equinox/storage/types"
	"errors"
	"go.uber.org/zap"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
)

var (
	ErrNoRoom = errors.New("no room for write")
)

type MemManager struct {
	sync.RWMutex

	mem *MemTable
	imm []*MemTable

	nextMemFid int

	flushCh chan *MemTable
	wg      sync.WaitGroup

	stats *metric.MemoryMetrics

	option config.Option

	logger *zap.Logger
}

func NewMemManager(opt config.Option, logger *zap.Logger) (*MemManager, error) {
	mm := &MemManager{
		imm:     make([]*MemTable, 0, opt.NumMemTables),
		flushCh: make(chan *MemTable, opt.NumMemTables),
		option:  opt,
		logger:  logger,
		stats:   metric.NewCacheMetrics(metric.GetEngineLabs(opt)),
	}
	err := mm.restoreMemTables()
	if err != nil {
		return nil, errs.Errorf(err, "while restore memtables")
	}
	if mm.mem, err = mm.newMemTable(); err != nil {
		return nil, errs.Errorf(err, "cannot create empty memtable")
	}
	mm.stats.LastSnapshot.SetToCurrentTime()
	return mm, nil
}

func (mm *MemManager) Close() {
	mm.wg.Wait()
	mm.Lock()
	defer mm.Unlock()
	_ = mm.mem.wal.CloseWithTruncate(int64(mm.mem.wal.Pos))
	close(mm.flushCh)
}

func (mm *MemManager) WriteMulti(values map[string][]types.Value, sync bool) error {
	mm.Lock()
	defer mm.Unlock()
	mm.stats.Writes.Inc()
	err := mm.mem.WriteMulti(values)
	if err != nil {
		mm.stats.WriteErr.Inc()
		return err
	}
	mm.stats.MemBytes.Set(float64(mm.mem.Cache.Size()))
	if sync {
		return mm.mem.SyncWAL()
	}
	return nil
}

func (mm *MemManager) DeleteRange(keys [][]byte, min, max int64) {
	mm.Lock()
	defer mm.Unlock()
	mm.mem.DeleteRange(keys, min, max)
	for _, m := range mm.imm {
		m.DeleteRange(keys, min, max)
	}
	mm.stats.MemBytes.Set(float64(mm.mem.Cache.Size()))
}

func (mm *MemManager) Values(key []byte) types.Values {
	mm.RLock()
	defer mm.RUnlock()

	var sz int
	var entries []*types.Entry

	e := mm.mem.Cache.Get(key)
	sz += e.Size()
	e.Deduplicate()
	entries = append(entries, e)

	for _, m := range mm.imm {
		ie := m.Cache.Get(key)
		if ie != nil {
			ie.Deduplicate()
			sz += ie.Size()
			entries = append(entries, ie)
		}
	}

	values := make(types.Values, sz)
	n := 0
	for _, entry := range entries {
		entry.RLock()
		n += copy(values[n:], entry.Values())
		entry.RUnlock()
	}
	values = values[:n]
	values = values.Deduplicate()
	return values
}

func (mm *MemManager) EnsureMemForWrite() error {
	mm.Lock()
	defer mm.Unlock()

	if !mm.mem.IsFull() {
		return nil
	}

	var err error
	select {
	case mm.flushCh <- mm.mem:
		mm.logger.Debug("Flushing memTable ", zap.Uint32("size", mm.mem.Cache.Size()), zap.Int("flushCh", len(mm.flushCh)))
		mm.wg.Add(1)
		mm.imm = append(mm.imm, mm.mem)
		mm.mem, err = mm.newMemTable()
		if err != nil {
			return errs.Error(err, "cannot create new mem table")
		}

		return nil
	default:
		return ErrNoRoom
	}
}

func (mm *MemManager) TakeFlushMemTable() *MemTable {
	mt, ok := <-mm.flushCh
	if ok {
		return mt
	}
	return nil
}

func (mm *MemManager) OnMemTableFlushed(mt *MemTable) {
	mm.Lock()
	defer mm.Unlock()
	if mm.imm[0] != mt {
		return
	}
	mm.imm = mm.imm[1:]
	mm.wg.Done()
	mt.DecrRef()
	mm.stats.LastSnapshot.SetToCurrentTime()
}

func (mm *MemManager) newMemTable() (*MemTable, error) {
	mem, err := NewMemTable(mm.nextMemFid, mm.option)
	if err != nil {
		return nil, err
	}
	mm.stats.MemBytes.Set(float64(mem.Cache.Size()))
	mm.nextMemFid++
	return mem, nil
}

func (mm *MemManager) restoreMemTables() error {
	files, err := os.ReadDir(mm.option.Dir)
	if err != nil {
		return errs.Errorf(err, "Unable to open mem dir: %q", mm.option.Dir)
	}

	var fids []int
	for _, file := range files {
		if !strings.HasSuffix(file.Name(), store.WALFileExtension) {
			continue
		}
		fsz := len(file.Name())
		var fid int64
		fid, err = strconv.ParseInt(file.Name()[:fsz-len(store.WALFileExtension)], 10, 64)
		if err != nil {
			return errs.Errorf(err, "Unable to parse log id. file: %s", file.Name())
		}

		fids = append(fids, int(fid))
	}

	// Sort by fid in ascending order (aka. by table files' created time).
	sort.Slice(fids, func(i, j int) bool {
		return fids[i] < fids[j]
	})
	for _, fid := range fids {
		flags := os.O_RDWR
		var mt *MemTable
		mt, err = OpenMemTable(fid, flags, mm.option)
		if err != nil {
			return errs.Errorf(err, "while opening fid: %d", fid)
		}
		// If this memTable is empty we don't need to add it. This is a
		// memTable that was completely truncated.
		if mt.Cache.Empty() {
			mt.DecrRef()
			continue
		}
		// These should no longer be written to. So, make them part of the imm.
		mm.imm = append(mm.imm, mt)
	}
	if len(fids) != 0 {
		mm.nextMemFid = fids[len(fids)-1]
	}

	mm.nextMemFid++
	return nil
}
