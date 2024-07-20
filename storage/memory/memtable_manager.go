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

	option config.Option

	logger *zap.Logger
}

func NewMemManager(opt config.Option, logger *zap.Logger) (*MemManager, error) {
	mm := &MemManager{
		imm:     make([]*MemTable, 0, opt.NumMemTables),
		flushCh: make(chan *MemTable, opt.NumMemTables),
		option:  opt,
		logger:  logger,
	}
	err := mm.restoreMemTables()
	if err != nil {
		return nil, errs.Errorf(err, "while restore memtables")
	}
	if mm.mem, err = mm.newMemTable(); err != nil {
		return nil, errs.Errorf(err, "cannot create empty memtable")
	}
	return mm, nil
}

func (mm *MemManager) Close() {
	mm.Lock()
	defer mm.Unlock()

	close(mm.flushCh)
}

func (mm *MemManager) WriteMulti(values map[string][]types.Value, sync bool) error {
	mm.Lock()
	defer mm.Unlock()

	err := mm.mem.WriteMulti(values)
	if err != nil {
		return err
	}
	if sync {
		return mm.mem.wal.Sync()
	}
	return nil
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
	mt.DecrRef()
}

func (mm *MemManager) newMemTable() (*MemTable, error) {
	mem, err := NewMemTable(mm.nextMemFid, mm.option)
	if err != nil {
		return nil, err
	}

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
