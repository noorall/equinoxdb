package equinox

import (
	"bytes"
	"encoding/binary"
	"equinox/internel"
	"fmt"
	"hash/crc32"
	"io/ioutil"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

const ValueFileExt = ".vlog"
const maxVlogFileSize = math.MaxUint32

type valueLog struct {
	dirPath string

	// guards our view of which files exist, which to be deleted, how many active iterators
	filesLock        sync.RWMutex
	filesMap         map[uint32]*logFile
	maxFid           uint32
	filesToBeDeleted []uint32
	// A refcount of iterators -- when this hits zero, we can delete the filesToBeDeleted.
	numActiveIterators int32

	writableLogOffset  uint32 // read by read, written by write. Must access via atomics.
	numEntriesWritten  uint32
	valueLogFileSize   uint32
	valueLogMaxEntries uint32
	valueThreshold     uint32
	garbageCh          chan struct{}
	discard            *discard
	logger             internel.Logger
	separate           bool
}

func OpenValueLog(option Options) (*valueLog, error) {
	v := &valueLog{
		dirPath:            option.ValueLogDir,
		valueLogFileSize:   uint32(option.ValueLogFileSize),
		valueLogMaxEntries: uint32(option.ValueLogMaxEntries),
		valueThreshold:     uint32(option.ValueThreshold),
		garbageCh:          make(chan struct{}, 1),
		logger:             option.Logger,
		separate:           option.Separate,
	}

	if err := v.loadFiles(); err != nil {
		return nil, err
	}

	d, err := NewDiscard(option)
	if err != nil {
		return nil, err
	}
	v.discard = d

	// first open
	if len(v.filesMap) == 0 {
		_, err := v.createValueLogFile()
		if err != nil {
			return nil, Wrapf(err, "Create a new log file failed while open value log")
		}

		return v, nil
	}

	// filter out the deleting log file
	fids := v.sortAndFilterFids()
	for _, fid := range fids {
		lf, ok := v.filesMap[fid]
		AssertTrue(ok)

		if err := lf.Open(valueLogFilePath(v.dirPath, fid), os.O_RDWR,
			2*option.ValueLogFileSize); err != nil {
			return nil, Wrapf(err, "Open existing file: %s", lf.path)
		}

		if lf.size == 0 && fid != v.maxFid {
			v.logger.Infof("Deleting empty file: %s", lf.path)
			if err := lf.Delete(); err != nil {
				return nil, Wrapf(err, "While deleting a empty log file: %s", lf.path)
			}

			delete(v.filesMap, fid)
		}

	}

	last, ok := v.filesMap[v.maxFid]
	AssertTrue(ok)

	offset, err := last.iterate(0, func(_ *entry, vp valPtr) error {
		return nil
	})

	if err != nil {
		return nil, Wrapf(err, "While iteratring the last value log file: %s", last.path)
	}

	// the last value log file is not empty, truncate it and create a new one.
	if offset > 0 {
		if err := last.Truncate(int64(offset)); err != nil {
			return nil, Wrapf(err, "While truncating the last value log file: %s", last.path)
		}

		if _, err := v.createValueLogFile(); err != nil {
			return nil, Wrapf(err, "While creating a new last value log file")
		}
	}

	// the last value log file is empty, reuse it.
	return v, nil
}

func (v *valueLog) Write(batch *writeBatchInternel) error {
	if err := v.validate(batch); err != nil {
		return err
	}

	v.filesLock.RLock()
	maxFid := v.maxFid
	curLogFile := v.filesMap[maxFid]
	v.filesLock.RUnlock()

	defer func() {
		if batch.sync {
			if err := curLogFile.Sync(); err != nil {
				v.logger.Errorf("Error Sync value log(%s): %+v", curLogFile.path, err)
			}
		}
	}()

	// clear
	batch.ptrs = batch.ptrs[:0]
	written := 0
	// write every entry
	buf := &bytes.Buffer{}
	for _, e := range batch.entries {
		if !v.separate || e.checkWithThreshold(v.valueThreshold) {
			batch.ptrs = append(batch.ptrs, valPtr{})
			continue
		}

		p := valPtr{}
		p.fid = curLogFile.fid
		p.offset = v.writeOffset()
		plen, err := curLogFile.EncodeEntryTo(e, buf)
		if err != nil {
			return err
		}

		// atomic update the offset, allow the concurrently write entry to the same log file
		atomic.AddUint32(&v.writableLogOffset, plen)
		curLogFile.WriteEntryFrom(buf)

		p.len = plen
		batch.ptrs = append(batch.ptrs, p)
		written++
	}

	v.numEntriesWritten += uint32(written)
	// flush
	if _, err := v.flush(curLogFile); err != nil {
		return err
	}

	return nil
}

func (v *valueLog) Read(option *ReadOptions, vp valPtr) (*entry, error) {
	lf, err := v.getLogFile(vp)
	if err != nil {
		return nil, err
	}

	defer lf.lock.RUnlock()
	buf, err := lf.readWithValPtr(vp)
	if err != nil {
		return nil, err
	}

	if option.VerifyCheckSum {
		hash := crc32.New(internel.CastagnoliCrcTable)
		if _, err := hash.Write(buf[:len(buf)-crc32.Size]); err != nil {
			return nil, Wrapf(err, "Failed compute the crc32 of value ptr: %+v", vp)
		}

		checksum := buf[len(buf)-crc32.Size:]
		if hash.Sum32() != binary.BigEndian.Uint32(checksum) {
			return nil, Wrapf(ErrChecksumMismatch, "Corrupted value at: %+v", vp)
		}
	}

	var h header
	hlen := h.Decode(buf)
	kv := buf[hlen:]
	if len(kv) < int(h.klen+h.vlen) {
		return nil, fmt.Errorf("Invalid value ptr: %+v, need len: %d, but has len: %d",
			vp, h.klen+h.vlen, len(kv))
	}

	return &entry{
		key:    kv[:h.klen],
		value:  kv[h.klen : h.klen+h.vlen],
		offset: vp.offset,
		hlen:   uint16(hlen),
		rtype:  h.recordType,
	}, nil
}

func (v *valueLog) Close() error {
	if v == nil {
		return nil
	}

	v.logger.Debugf("Closing value log and stopping garbage collection.")
	var err error
	for id, lf := range v.filesMap {
		lf.lock.Lock() // We won’t release the lock.
		offset := int64(-1)

		if id == v.maxFid {
			offset = int64(v.writeOffset())
		}
		if terr := lf.CloseWithTruncate(offset); terr != nil && err == nil {
			err = terr
		}
	}

	if terr := v.discard.Close(); err == nil && terr != nil {
		err = terr
	}

	return err
}

// sync function syncs content of latest value log file to disk.
func (v *valueLog) Sync() error {
	v.filesLock.RLock()
	maxFid := v.maxFid
	currLogFile := v.filesMap[maxFid]
	if currLogFile == nil {
		v.filesLock.RUnlock()
		return nil
	}

	currLogFile.lock.RLock()
	v.filesLock.RUnlock()
	err := currLogFile.Sync()
	currLogFile.lock.RUnlock()

	return err
}

func (v *valueLog) writeOffset() uint32 {
	return atomic.LoadUint32(&v.writableLogOffset)
}

func (v *valueLog) createValueLogFile() (*logFile, error) {
	fid := v.maxFid + 1
	fpath := valueLogFilePath(v.dirPath, fid)
	lf := &logFile{
		fid:  fid,
		path: fpath,
		pos:  0,
	}

	err := lf.Open(fpath, os.O_RDWR|os.O_CREATE|os.O_EXCL, 2*int(v.valueLogFileSize))
	if err != nil {
		return nil, err
	}

	v.filesLock.Lock()
	defer v.filesLock.Unlock()
	v.filesMap[fid] = lf
	v.maxFid = fid
	atomic.StoreUint32(&v.writableLogOffset, 0)
	v.numEntriesWritten = 0

	return lf, nil
}

func (v *valueLog) deleteValueLogFile(lf *logFile) error {
	if lf == nil {
		return nil
	}

	lf.lock.Lock()
	defer lf.lock.Unlock()

	return lf.Delete()
}

func (v *valueLog) loadFiles() error {
	v.filesMap = make(map[uint32]*logFile)

	files, err := ioutil.ReadDir(v.dirPath)
	if err != nil {
		Wrapf(err, "Unable to open value log dir. Path: %s", v.dirPath)
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
			return Wrapf(err, "Unable to parse value log file id. File: %s", fname)
		}

		if _, ok := exist[fid]; ok {
			return Wrapf(err, "Duplicate file found. Please delete one manually. File: %s", fname)
		}

		exist[fid] = struct{}{}

		lf := &logFile{
			fid:  uint32(fid),
			path: filepath.Join(v.dirPath, fname),
		}

		v.filesMap[lf.fid] = lf

		if v.maxFid < lf.fid {
			v.maxFid = lf.fid
		}
	}

	return nil
}

func (v *valueLog) sortAndFilterFids() []uint32 {
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

func (v *valueLog) validate(batch *writeBatchInternel) error {
	offset := uint64(v.writeOffset())
	estimatedVlogOffset := batch.ApproximateSize() + offset

	if estimatedVlogOffset > uint64(maxVlogFileSize) {
		return fmt.Errorf("Batch size offset %d is bigger than maximum offset %d",
			estimatedVlogOffset, maxVlogFileSize)
	}

	return nil
}

func (v *valueLog) flush(lf *logFile) (*logFile, error) {
	if v.writeOffset() <= v.valueLogFileSize && v.numEntriesWritten <= v.valueLogMaxEntries {
		return lf, nil
	}

	if err := lf.Flush(v.writeOffset()); err != nil {
		return nil, err
	}

	newlf, err := v.createValueLogFile()
	if err != nil {
		return nil, err
	}

	return newlf, nil
}

func (v *valueLog) getLogFile(vp valPtr) (*logFile, error) {
	v.filesLock.RLock()
	defer v.filesLock.RUnlock()

	lf, ok := v.filesMap[vp.fid]
	if !ok {
		return nil, fmt.Errorf("File id: %d not found.", vp.fid)
	}

	maxFid := v.maxFid
	if vp.fid == maxFid {
		offset := v.writeOffset()
		if vp.offset >= offset {
			return nil, fmt.Errorf(
				"Invalid read offset: %d greater than current offset: %d", vp.offset, offset)
		}
	}

	// may be the gc goroutine read this file concurrently
	lf.lock.RLock()

	return lf, nil
}

func (v *valueLog) updateDiscard(stats map[uint32]uint64) {
	for fid, count := range stats {
		v.discard.Update(fid, count)
	}
}

func (v *valueLog) runGC(discardRatio float64, db DB) error {
	select {
	case v.garbageCh <- struct{}{}:
		defer func() {
			<-v.garbageCh
		}()

		lf := v.selectGCFile(discardRatio)
		if lf == nil {
			return ErrNoRewrite
		}

		if err := v.gcLogFile(lf, db); err != nil {
			return err
		}

		v.discard.Reset(lf.fid)
		return nil

	default:
		return ErrRejected
	}
}

func (v *valueLog) waitGC(closer *internel.Closer) {
	defer closer.Done()
	<-closer.HasBeenClosed()

	// Block any GC in progress to finish, and don't allow any more writes to runGC by filling up
	// the channel of size 1.
	v.garbageCh <- struct{}{}
}

var writeOption = &WriteOptions{Sync: true}

func (v *valueLog) selectGCFile(ratio float64) *logFile {
	v.filesLock.RLock()
	defer v.filesLock.RUnlock()

	var fid uint32
	var count uint64
	var lf *logFile

	for {
		fid, count = v.discard.Max()
		if fid == 0 {
			v.logger.Debugf("No file need to garbage collection.")
			return nil
		}

		logFile, ok := v.filesMap[fid]
		if !ok {
			v.discard.Reset(fid)
		} else {
			lf = logFile
			break
		}
	}

	f, err := lf.Fd.Stat()
	if err != nil {
		v.logger.Errorf("Unable to get stats for value log fid: %d err: %+v", f, err)
		return nil
	}

	// the max discard count of the selected file doesn't reach the ratio
	if thresold := ratio * float64(f.Size()); thresold > float64(count) {
		v.logger.Debugf("Discard: %d less than threshold: %.0f for file: %s",
			count, thresold, f.Name())
		return nil
	}

	maxFid := atomic.LoadUint32(&v.maxFid)
	if fid < maxFid {
		v.logger.Infof("Found value log max discard fid: %d discard: %d", maxFid, count)
		return lf
	}

	return nil
}

func (v *valueLog) gcLogFile(lf *logFile, db DB) error {
	v.filesLock.RLock()

	for _, fid := range v.filesToBeDeleted {
		if lf.fid == fid {
			v.filesLock.Unlock()
			return fmt.Errorf("The value log has been marked for deletion. fid: %d", fid)
		}
	}

	maxFid := v.maxFid
	if lf.fid >= maxFid {
		return fmt.Errorf("The value log id equal or greater than maxFid. fid: %d, maxFid: %d",
			lf.fid, maxFid)
	}
	v.filesLock.RUnlock()

	v.logger.Infof("Gc value log: %d, path:%d", lf.fid, lf.path)
	option := db.GetOption()
	wb := NewWriteBatch(db)

	var size uint32
	var count, moved int
	ropt := &ReadOptions{VerifyCheckSum: true}
	filter := func(e *entry, _ valPtr) error {
		count++

		if count%100000 == 0 {
			v.logger.Debugf("Processing entry %d", count)
		}
		ev, err := db.GetExtend(ropt, e.key)
		if err != nil {
			return err
		}

		// Version not found. Discard.
		if ev.version != ParseVersion(e.key) {
			return nil
		}

		// lsm store the key with value. Discard.
		if ev.Meta&ValPtr == 0 {
			return nil
		}

		// entry has been deleted. Discard.
		if ev.Meta&Delete == Delete {
			return nil
		}

		// Value isn't in the lsm, but still present in value log.
		if len(ev.Value) == 0 {
			return fmt.Errorf("Empty value: %+v", ev)
		}

		var vp valPtr
		vp.Decode(ev.Value)

		if vp.fid == lf.fid && vp.offset == e.offset {
			moved++
			ne := entry{}

			// deep copy
			ne.key = append(ne.key, e.key...)
			ne.value = append(ne.value, e.value...)
			ne.rtype = e.rtype &^ ValPtr
			es := ne.estimateSize(uint32(option.ValueThreshold)) + uint32(len(ne.value))

			if wb.Count()+1 >= option.maxBatchCount || size+es >= uint32(option.maxBatchSize) {
				if err := db.Write(writeOption, wb); err != nil {
					return err
				}

				wb.Clear()
			}

			wb.Put(e.key, e.value)
			size += es
		}

		return nil
	}

	_, err := lf.iterate(0, filter)

	if err != nil {
		return err
	}

	if err := db.Write(writeOption, wb); err != nil {
		return err
	}

	v.logger.Infof("Total entries: %d. Moved: %d", count, moved)
	v.logger.Infof("Removing file: %d, path: %s", lf.fid, lf.path)

	canDeleteLogFile := false

	v.filesLock.Lock()
	if _, ok := v.filesMap[lf.fid]; !ok {
		v.filesLock.Unlock()
		return fmt.Errorf("Not found the file need to delete: %d", lf.fid)
	}

	if v.iteratorCount() == 0 {
		delete(v.filesMap, lf.fid)
		canDeleteLogFile = true
	} else {
		v.filesToBeDeleted = append(v.filesToBeDeleted, lf.fid)
	}
	v.filesLock.Unlock()

	if canDeleteLogFile {
		if err := v.deleteValueLogFile(lf); err != nil {
			return err
		}
	}

	return nil
}

func (v *valueLog) iteratorCount() int {
	return int(atomic.LoadInt32(&v.numActiveIterators))
}

func (v *valueLog) incrIteratorCount() {
	atomic.AddInt32(&v.numActiveIterators, 1)
}

func (v *valueLog) decrIteratorCount() error {
	num := atomic.AddInt32(&v.numActiveIterators, -1)
	if num != 0 {
		return nil
	}

	v.filesLock.Lock()
	lfs := make([]*logFile, 0, len(v.filesToBeDeleted))
	for _, id := range v.filesToBeDeleted {
		lfs = append(lfs, v.filesMap[id])
		delete(v.filesMap, id)
	}
	v.filesToBeDeleted = nil
	v.filesLock.Unlock()

	for _, lf := range lfs {
		if err := v.deleteValueLogFile(lf); err != nil {
			return err
		}
	}

	return nil
}

func (v *valueLog) dropAll() (int, error) {
	var count int
	deleteAll := func() error {
		v.filesLock.Lock()
		defer v.filesLock.Unlock()
		for _, lf := range v.filesMap {
			if err := v.deleteValueLogFile(lf); err != nil {
				return err
			}
			count++
		}

		v.filesMap = make(map[uint32]*logFile)
		v.maxFid = 0
		return nil
	}
	if err := deleteAll(); err != nil {
		return count, err
	}

	if _, err := v.createValueLogFile(); err != nil {
		return count, err
	}

	return count, nil
}

func valueLogFilePath(dir string, fid uint32) string {
	return filepath.Join(dir, fmt.Sprintf("%06d%s", fid, ValueFileExt))
}

func getFileIdFromName(fileName string) (uint64, error) {
	return strconv.ParseUint(fileName[:len(fileName)-len(ValueFileExt)], 10, 32)
}
