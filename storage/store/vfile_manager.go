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
	"equinox/storage/codec"
	"equinox/storage/config"
	"equinox/storage/errs"
	"equinox/storage/metric"
	"fmt"
	"go.uber.org/zap"
	"hash/crc32"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

const ValueFileExt = ".vfile"

type VFileManager struct {
	dirPath string

	// guards our view of which files exist, which to be deleted, how many active iterators
	filesLock        sync.RWMutex
	filesMap         map[uint32]*ValueFile
	maxFid           uint32
	filesToBeDeleted []uint32
	// A refcount of iterators -- when this hits zero, we can delete the filesToBeDeleted.
	numActiveIterators int32

	writableLogOffset         uint32 // read by read, written by write. Must access via atomics.
	numEntriesWritten         uint32
	valueFileMaxSize          uint32
	valueFileMaxEntries       uint32
	valueFileGCThresholdRadio float64
	valueFileGCMaxFiles       int

	garbageCh chan struct{}
	discard   *discard
	lifeCycle int

	stats  *metric.ValueFileMetrics
	logger *zap.Logger

	fileWrite sync.RWMutex
}

func NewVFileManager(option config.Option, lifeCycle int, stats *metric.ValueFileMetrics) (*VFileManager, error) {
	valueFileDir := filepath.Join(option.Dir, strconv.Itoa(lifeCycle))
	if err := os.MkdirAll(valueFileDir, 0777); err != nil {
		return nil, err
	}
	v := &VFileManager{
		dirPath:             valueFileDir,
		valueFileMaxSize:    uint32(option.ValueFileMaxSize),
		valueFileMaxEntries: uint32(option.ValueFileMaxEntries),
		logger:              option.Logger,
		garbageCh:           make(chan struct{}, 1),
		lifeCycle:           lifeCycle,
		stats:               stats,
	}

	if err := v.loadFiles(); err != nil {
		return nil, err
	}

	d, err := newDiscard(valueFileDir)
	if err != nil {
		return nil, err
	}
	v.discard = d

	// first open
	if len(v.filesMap) == 0 {
		_, err := v.createValueFile()
		if err != nil {
			return nil, errs.Errorf(err, "Create a new log file failed while open value log")
		}

		return v, nil
	}

	// filter out the deleting log file
	fids := v.sortAndFilterFids()
	for _, fid := range fids {
		vf, ok := v.filesMap[fid]

		if !ok {
			return nil, fmt.Errorf("failed to open fmap")
		}

		if err := vf.Open(valueFilePath(v.dirPath, fid), os.O_RDWR,
			2*option.ValueFileMaxSize); err != nil {
			return nil, errs.Errorf(err, "Open existing file: %s", vf.path)
		}

		if vf.size == 0 && fid != v.maxFid {
			v.logger.Info("Deleting empty file ", zap.String("path", vf.path))
			if err := vf.Delete(); err != nil {
				return nil, errs.Errorf(err, "While deleting a empty log file: %s", vf.path)
			}
			delete(v.filesMap, fid)
		}

	}
	v.stats.AddFiles(int64(len(fids)))
	last, ok := v.filesMap[v.maxFid]
	if !ok {
		return nil, fmt.Errorf("failed to get lastest fmap")
	}

	offset := last.size
	v.stats.AddSize(int64(last.size))

	// the last value file is not empty, truncate it and create a new one.
	if offset > 0 {
		if err := last.Truncate(int64(offset)); err != nil {
			return nil, errs.Errorf(err, "While truncating the last value file: %s", last.path)
		}

		if _, err := v.createValueFile(); err != nil {
			return nil, errs.Errorf(err, "While creating a new last value file")
		}
	}

	// the last value file is empty, reuse it.
	return v, nil
}

func (v *VFileManager) WriteBlock(key []byte, minTime, maxTime int64, block []byte) ([]byte, error) {
	v.filesLock.RLock()
	maxFid := v.maxFid
	curValueFile := v.filesMap[maxFid]
	v.filesLock.RUnlock()

	dataSize := uint32(VFileHeaderSize + len(key) + crc32.Size + len(block))

	if err := v.validateWriteSize(uint64(dataSize)); err != nil {
		// optimize this part
		curValueFile, err = v.flush(curValueFile)
		if err != nil {
			return block, err
		}
	}

	end, err := curValueFile.WriteBlock(key, minTime, maxTime, block)

	if err != nil {
		return block, err
	}

	atomic.AddUint32(&v.writableLogOffset, uint32(len(key)+4+len(block)+VFileHeaderSize))
	v.stats.AddTotalWritten(int64(uint32(len(key) + 4 + len(block) + VFileHeaderSize)))

	buf := make([]byte, ValuePtrSize+1)
	buf[0] = codec.WithPtrFlag(block[0])

	p := ValuePtr{}
	p.MinTime = minTime
	p.MaxTime = maxTime
	p.FileNo = curValueFile.fid
	p.Offset = int64(end - uint32(len(block)))
	p.Size = uint32(len(block))
	v.numEntriesWritten++
	p.AppendTo(buf[1:])
	return buf, nil
}

func (v *VFileManager) Read(vp *ValuePtr) ([]byte, error) {
	vf, err := v.getValueFile(vp)
	// has been gc
	if err != nil {
		return []byte{}, nil
	}
	defer vf.lock.RUnlock()

	return vf.readWithValPtr(vp)
}

func (v *VFileManager) MarkAsDelete(key []byte, vp *ValuePtr) error {
	vf, err := v.getValueFile(vp)
	if err != nil {
		return err
	}
	defer vf.lock.RUnlock()
	v.discard.Update(vf.fid, uint64(int(vp.Size)+len(key)+crc32.Size+VFileHeaderSize))
	return vf.MarkAsDelete(key, vp.MinTime, vp.MaxTime, false)
}

func (v *VFileManager) GetGarbageFiles() []*ValueFile {
	files := v.getGarbageFiles()
	var gf []*ValueFile
	v.filesLock.RLock()
	defer v.filesLock.RUnlock()
	for _, f := range files {
		maxFid := v.maxFid
		if f.fid >= maxFid {
			v.logger.Error("The value log id equal or greater than maxFid.",
				zap.Int("fid", int(f.fid)), zap.Int("maxFid", int(maxFid)))
			continue
		}
		canGc := true
		for _, fid := range v.filesToBeDeleted {
			if f.fid == fid {
				canGc = false
				break
			}
		}
		if canGc {
			gf = append(gf, f)
		}
	}
	return gf
}

func (v *VFileManager) DeleteValueFile(vf *ValueFile) {
	v.filesLock.Lock()
	defer v.filesLock.Lock()

	err := v.deleteValueFile(vf)
	if err != nil {
		v.logger.Warn("failed to delete value file, try later", zap.Int("fid", int(vf.fid)))
		v.filesToBeDeleted = append(v.filesToBeDeleted, vf.fid)
	} else {
		delete(v.filesMap, vf.fid)
	}
}

func (v *VFileManager) Close() error {
	if v == nil {
		return nil
	}

	v.logger.Info("Closing value log and stopping garbage collection.")
	var err error
	for id, vf := range v.filesMap {
		vf.lock.Lock() // We won’t release the lock.
		offset := int64(-1)

		if id == v.maxFid {
			offset = int64(v.writeOffset())
		}
		if terr := vf.CloseWithTruncate(offset); terr != nil && err == nil {
			err = terr
		}
	}

	if terr := v.discard.Close(); err == nil && terr != nil {
		err = terr
	}

	for _, fid := range v.filesToBeDeleted {
		v.DeleteValueFile(v.filesMap[fid])
	}

	return err
}

func (v *VFileManager) Sync() error {
	v.filesLock.RLock()
	maxFid := v.maxFid
	currValueFile := v.filesMap[maxFid]
	if currValueFile == nil {
		v.filesLock.RUnlock()
		return nil
	}

	currValueFile.lock.RLock()
	v.filesLock.RUnlock()
	err := currValueFile.Sync()
	currValueFile.lock.RUnlock()

	return err
}

func (v *VFileManager) writeOffset() uint32 {
	return atomic.LoadUint32(&v.writableLogOffset)
}

func (v *VFileManager) createValueFile() (*ValueFile, error) {
	fid := v.maxFid + 1
	fpath := valueFilePath(v.dirPath, fid)
	vf := &ValueFile{
		fid:       fid,
		path:      fpath,
		lifeCycle: v.lifeCycle,
		pos:       0,
	}

	err := vf.Open(fpath, os.O_RDWR|os.O_CREATE|os.O_EXCL, 2*int(v.valueFileMaxSize))
	if err != nil {
		return nil, err
	}

	v.filesLock.Lock()
	defer v.filesLock.Unlock()
	v.filesMap[fid] = vf
	v.maxFid = fid
	atomic.StoreUint32(&v.writableLogOffset, 0)
	v.numEntriesWritten = 0

	return vf, nil
}

func (v *VFileManager) deleteValueFile(vf *ValueFile) error {
	if vf == nil {
		return nil
	}

	vf.lock.Lock()
	defer vf.lock.Unlock()

	err := vf.Delete()
	if err == nil {
		v.stats.DecFiles()
	}
	return err
}

func (v *VFileManager) loadFiles() error {
	v.filesMap = make(map[uint32]*ValueFile)

	files, err := os.ReadDir(v.dirPath)
	if err != nil {
		return errs.Errorf(err, "Unable to open value file dir. Path: %s", v.dirPath)
	}

	// some file may duplicated, so we need a Set to found these.
	exist := make(map[uint64]struct{})
	for _, file := range files {
		fname := file.Name()
		if !strings.HasSuffix(fname, ValueFileExt) {
			continue
		}

		fid, err := getFileIdFromName(fname)
		if err != nil {
			return errs.Errorf(err, "Unable to parse value file id. File: %s", fname)
		}

		if _, ok := exist[fid]; ok {
			return errs.Errorf(err, "Duplicate file found. Please delete one manually. File: %s", fname)
		}

		exist[fid] = struct{}{}

		vf := &ValueFile{
			fid:  uint32(fid),
			path: filepath.Join(v.dirPath, fname),
		}

		v.filesMap[vf.fid] = vf

		if v.maxFid < vf.fid {
			v.maxFid = vf.fid
		}
	}

	return nil
}

func (v *VFileManager) sortAndFilterFids() []uint32 {
	deleted := make(map[uint32]struct{})
	for _, fid := range v.filesToBeDeleted {
		deleted[fid] = struct{}{}
	}

	res := make([]uint32, 0, len(v.filesMap))
	for fid := range v.filesMap {
		if _, ok := deleted[fid]; !ok {
			res = append(res, fid)
		}
	}

	sort.Slice(res, func(i, j int) bool {
		return res[i] < res[j]
	})
	return res
}

func (v *VFileManager) validateWriteSize(dataSize uint64) error {
	offset := uint64(v.writeOffset())
	estimatedVlogOffset := dataSize + offset

	if estimatedVlogOffset > uint64(v.valueFileMaxSize) {
		return fmt.Errorf("batch size offset %d is bigger than maximum offset %d",
			estimatedVlogOffset, v.valueFileMaxSize)
	}

	return nil
}

func (v *VFileManager) flush(vf *ValueFile) (*ValueFile, error) {
	if v.writeOffset() <= v.valueFileMaxSize && v.numEntriesWritten <= v.valueFileMaxEntries {
		return vf, nil
	}

	if err := vf.Flush(v.writeOffset()); err != nil {
		return nil, err
	}

	newvf, err := v.createValueFile()
	if err != nil {
		return nil, err
	}

	return newvf, nil
}

func (v *VFileManager) getValueFile(vp *ValuePtr) (*ValueFile, error) {
	v.filesLock.RLock()
	defer v.filesLock.RUnlock()

	vf, ok := v.filesMap[vp.FileNo]
	if !ok {
		return nil, fmt.Errorf("file id: %d not found", vp.FileNo)
	}

	maxFid := v.maxFid
	if vp.FileNo == maxFid {
		offset := v.writeOffset()
		if uint32(vp.Offset) >= offset {
			return nil, fmt.Errorf(
				"invalid read offset: %d greater than current offset: %d", vp.Offset, offset)
		}
	}

	// may be the gc goroutine read this file concurrently
	vf.lock.RLock()

	return vf, nil
}

func (v *VFileManager) updateDiscard(stats map[uint32]uint64) {
	for fid, count := range stats {
		v.discard.Update(fid, count)
	}
}

func valueFilePath(dir string, fid uint32) string {
	return filepath.Join(dir, fmt.Sprintf("%06d%s", fid, ValueFileExt))
}

func getFileIdFromName(fileName string) (uint64, error) {
	return strconv.ParseUint(fileName[:len(fileName)-len(ValueFileExt)], 10, 32)
}
