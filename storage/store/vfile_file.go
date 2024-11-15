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
	"bytes"
	"encoding/binary"
	"equinox/storage/errs"
	"equinox/storage/types"
	"fmt"
	"github.com/cespare/xxhash/v2"
	"hash/crc32"
	"io"
	"sync"
	"sync/atomic"
	"time"
)

const DefaultValidationCacheSize = 1024 * 1024

type ValueFile struct {
	fid  uint32
	size uint32 // the max size of a log file will not exceed 4 GB
	pos  uint32

	path string
	lock sync.RWMutex

	tombstoner *Tombstoner
	cache      *dataValidationCache

	lifeCycle int

	*MMapFile
}

func (f *ValueFile) Open(path string, flags int, size int) error {
	mmf, err := OpenMmapFile(path, flags, size)

	if err != nil {
		return errs.Errorf(err, "while opening file: %s", path)
	}

	f.MMapFile = mmf
	f.size = uint32(len(f.Data))
	if mmf.NewFile {
		f.size = 0
		f.clearEntryHeader()
	}

	f.tombstoner = NewTombstoner(path, nil)
	f.cache = newDataValidationCache(DefaultValidationCacheSize)

	return nil
}

func (f *ValueFile) Truncate(end int64) error {
	if fs, err := f.Fd.Stat(); err != nil {
		return errs.Errorf(err, "while get stat from file: %s", f.path)
	} else if fs.Size() == end {
		return nil
	}

	f.size = uint32(end)
	return f.MMapFile.Truncate(end)
}

func (f *ValueFile) WriteBlock(key []byte, minTime, maxTime int64, block []byte) (uint32, error) {
	n := len(key) + 4 + len(block) + VFileHeaderSize

	newPos := atomic.AddUint32(&f.pos, uint32(n))
	if int(newPos) >= len(f.Data) {
		if err := f.Truncate(int64(newPos)); err != nil {
			return 0, err
		}
	}

	start := int(newPos) - n

	checksum := make([]byte, crc32.Size)
	binary.BigEndian.PutUint32(checksum[:], crc32.ChecksumIEEE(block))

	header := &VFileHeader{
		KeyLen:  uint16(len(key)),
		DataLen: uint32(len(block)),
		KeyHash: xxhash.Sum64(key),
		MinTime: minTime,
		MaxTime: maxTime,
	}

	headerBytes, _ := header.MarshalBinary()

	start += copy(f.Data[start:], headerBytes)

	start += copy(f.Data[start:], key)

	start += copy(f.Data[start:], checksum)

	start += copy(f.Data[start:], block)

	if uint32(start) != newPos {
		return 0, fmt.Errorf("error while write value file")
	}

	atomic.AddUint32(&f.size, uint32(n))

	return newPos, nil
}

func (f *ValueFile) MarkAsDelete(key []byte, minTime, maxTime int64, sync bool) error {
	f.cache.AddMulti(string(key), []TimeRange{{minTime, maxTime}})
	if sync {
		return f.tombstoner.writeTombstone([]Tombstone{{Max: maxTime, Min: minTime, Key: key}})
	} else {
		go f.tombstoner.writeTombstone([]Tombstone{{Max: maxTime, Min: minTime, Key: key}})
		return nil
	}
}

func (f *ValueFile) IsValidateValue(key []byte, minTime, maxTime int64) bool {
	// TODO optimize this part
	l := types.LifeCycleToUnixNano(f.lifeCycle)
	t := time.Now()
	if l != -1 && maxTime < t.UnixNano()-l {
		return false
	}
	deletedTimes := f.cache.Get(string(key))
	if deletedTimes != nil {
		return !IsOverlapAny(TimeRange{minTime, maxTime}, deletedTimes)
	}
	validated := true
	err := f.tombstoner.Walk(func(t Tombstone) error {
		if bytes.Equal(key, t.Key) && t.Min <= minTime && t.Max >= maxTime {
			validated = false
			return fmt.Errorf("find")
		}
		return nil
	})
	if err != nil {
		return false
	}
	return validated
}

func (f *ValueFile) readWithValPtr(p *ValuePtr) (buf []byte, err error) {
	size := int64(len(f.Data))

	offset := p.Offset
	dataSize := int64(p.Size)

	fSize := int64(atomic.LoadUint32(&f.size))

	if offset >= size || offset+dataSize > size ||
		offset+dataSize > fSize {
		err = io.EOF
	} else {
		buf = f.Data[offset : offset+dataSize]
	}

	return buf, err
}

func (f *ValueFile) Flush(offset uint32) error {
	if err := f.Sync(); err != nil {
		return errs.Errorf(err, "sync file: %s", f.path)
	}

	f.lock.Lock()
	defer f.lock.Unlock()

	if err := f.Truncate(int64(offset)); err != nil {
		return err
	}

	return nil
}

func (f *ValueFile) clearEntryHeader() {
	start, end := f.pos, f.pos+VFileHeaderSize
	if end > f.size {
		return
	}

	if end > f.size {
		end = f.size
	}

	if end <= start {
		return
	}

	h := f.Data[start:end]

	for i := range h {
		h[i] = 0x0
	}
}
