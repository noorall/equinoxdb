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
	"bytes"
	"equinox/storage/errs"
	"equinox/storage/store"
	"equinox/storage/types"
	equinox "equinox/types"
	"fmt"
	"log"
	"math"
	"os"
)

type MemTable struct {
	cache *Cache
	wal   *store.WalFile
	buf   *bytes.Buffer

	option     equinox.Options
	maxVersion uint64
	NewTable   bool
}

func NewMemTable(id int, option equinox.Options) (*MemTable, error) {
	mt, err := OpenMemTable(id, os.O_CREATE|os.O_RDWR, option)
	if err == nil && mt.NewTable {
		return mt, nil
	}

	if err != nil {
		return nil, errs.Errorf(err, "create a new memtable: %s", mt.wal.Fd.Name())
	}

	return nil, fmt.Errorf("file %s already exists", mt.wal.Fd.Name())
}

func OpenMemTable(fid, flags int, option equinox.Options) (*MemTable, error) {
	cache := NewCache(option.Comparator)

	mt := &MemTable{
		cache:  cache,
		option: option,
		buf:    &bytes.Buffer{},
	}

	mt.wal = store.NewWalFile(fid, option.Dir)

	err := mt.wal.Open(flags, 2*option.MemTableSize)
	if err != nil {
		return nil, errs.Errorf(err, "while opening memtable: %d", fid)
	}

	cache.Handler = func() {
		if err = mt.wal.Delete(); err != nil {
			option.Logger.Errorf("while deleting file: %d, errs: %v", fid, err)
		}
	}

	if mt.wal.MMapFile.NewFile {
		mt.NewTable = true
		return mt, nil
	}

	// restore from wal
	err = mt.restoreFromWAL()

	if err != nil {
		return nil, errs.Errorf(err, "while restoring memtable: %d", fid)
	}

	return mt, nil
}

func (m *MemTable) WriteMulti(values map[string][]types.Value) error {
	var addedSize uint64
	for _, v := range values {
		addedSize += uint64(types.Values(v).Size())
	}

	if err := m.wal.WriteMulti(values); err != nil {
		return errs.Errorf(err, "while writing values to wal")
	}

	for k, v := range values {
		err := m.cache.Put([]byte(k), v)
		if err != nil {
			return errs.Errorf(err, "while writing values to cache")
		}
	}
	return nil
}

func (m *MemTable) Delete(keys [][]byte) {
	m.DeleteRange(keys, math.MinInt64, math.MaxInt64)
}

func (m *MemTable) DeleteRange(keys [][]byte, min, max int64) {
	for _, k := range keys {
		// Make sure key exist in the cache, skip if it does not
		e := m.cache.Get(k)
		if e == nil {
			continue
		}
		origSize := uint32(e.Size())
		if min == math.MinInt64 && max == math.MaxInt64 {
			e.Clean()
		}
		e.Filter(min, max)
		m.cache.DecreaseSize(origSize - uint32(e.Size()))
	}
}

func (m *MemTable) IsFull() bool {
	if m.cache.Size() >= uint32(m.option.MemTableSize) {
		return true
	}

	return m.wal.Pos >= uint32(m.option.MemTableSize)
}

func (m *MemTable) SyncWAL() error {
	return m.wal.Sync()
}

func (m *MemTable) IncrRef() {
	m.cache.Ref()
}

func (m *MemTable) DecrRef() {
	m.cache.Deref()
}

func (m *MemTable) restoreFromWAL() error {
	if m.wal == nil || m.cache == nil {
		return nil
	}
	r := store.NewWALReader(m.wal.NewReader(0))

	for r.Next() {
		entry, err := r.Read()
		if err != nil {
			n := r.Count()
			log.Printf("file corrupt, errs: %v", err)
			if err = m.wal.Truncate(n); err != nil {
				return err
			}
			break
		}

		switch t := entry.(type) {
		case *store.WriteWALEntry:
			if err = m.WriteMulti(t.Values); err != nil {
				return err
			}
		case *store.DeleteRangeWALEntry:
			m.DeleteRange(t.Keys, t.Min, t.Max)
		case *store.DeleteWALEntry:
			m.Delete(t.Keys)
		}
	}

	return m.wal.Truncate(r.Count())
}
