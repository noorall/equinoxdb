package equinox

import (
	"bytes"
	"context"
	"equinox/internel"
	"equinox/types"
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"errors"
)

const threshold = 1024 * 1024

// DB interface is useful when test
type DB interface {
	Put(options *WriteOptions, key, value []byte) error
	Delete(options *WriteOptions, key []byte) error
	Write(options *WriteOptions, batch *WriteBatch) error
	Get(options *ReadOptions, key []byte) (value []byte, err error)
	NewIterator(options *ReadOptions) (iterator Iterator, err error)
	GetSnapshot() *Snapshot
	ReleaseSnapshot(snapshot *Snapshot)
	GetExtend(options *ReadOptions, key []byte) (value *EValue, err error)
	GetOption() Options
	Close() error
	TSWrite(options *WriteOptions, points []types.TSEntry) error
}

// closers for those goroutines that need run backgroud when DB opened.
type closers struct {
	compact  *internel.Closer
	memtable *internel.Closer
	writes   *internel.Closer
	valueGC  *internel.Closer
}

type DBImpl struct {
	sync.RWMutex // Guards list of inmemory tables, not individual reads and writes.
	closers      closers

	dirLockGuard  *internel.DirLockGuard // Ensure that one and only one database is opened
	valueDirGuard *internel.DirLockGuard

	mem        *memTable
	imm        []*memTable
	nextMemFid int

	option    *Options
	snapshots *snapshotList
	manifest  *manifestFile
	ls        *levels
	vlog      *valueLog
	vlogs     []*valueLog
	writeCh   chan *writeBatchInternel
	flushCh   chan *memTable

	blockWrites  int32
	isClosed     uint32    // atomic
	closeOnce    sync.Once // for close db just once
	tsEntryCache map[string][]byte
	keyIndex     map[string]int
	walTime      int
	rwMu         sync.RWMutex
	idx          atomic.Uint64
}

const (
	lockFile        = "LOCK"
	writeChCapacity = 1000
)

func Open(options Options) (DB, error) {
	if err := checkOptions(&options); err != nil {
		return nil, err
	}

	dir := []string{options.Dir, filepath.Join(options.ValueLogDir, strconv.Itoa(0))}

	if options.VFileWriteParallelism > 1 {
		for i := 1; i < options.VFileWriteParallelism; i++ {
			dir = append(dir, filepath.Join(options.ValueLogDir, strconv.Itoa(i)))
		}
	}

	if err := createDirs(dir); err != nil {
		return nil, err
	}

	dirLockGuard, err := internel.AcquireDirLock(options.Dir, lockFile)
	if err != nil {
		return nil, Wrapf(err, "acquire dir(%q) lock failed, when open db.", options.Dir)
	}

	// if any errs found during db open, release the lock.
	defer func() {
		if dirLockGuard != nil {
			_ = dirLockGuard.Release()
		}
	}()

	absDir, err := filepath.Abs(options.Dir)
	if err != nil {
		return nil, err
	}
	absValueLogDir, err := filepath.Abs(options.ValueLogDir)
	if err != nil {
		return nil, err
	}

	var valueDirLockGuard *internel.DirLockGuard
	// value log file isn't storage with sst tables.
	// so lock value log dir too.
	if absDir != absValueLogDir {
		valueDirLockGuard, err = internel.AcquireDirLock(options.ValueLogDir, lockFile)
		if err != nil {
			return nil, err
		}

		// if any errs found during db open, release the lock.
		defer func() {
			if valueDirLockGuard != nil {
				_ = valueDirLockGuard.Release()
			}
		}()
	}

	options.Logger.Infof("Opening manifest file: %q", filepath.Join(options.Dir, ManifestFilename))
	manifestFile, manifest, err := OpenManifestFile(options.Dir)
	if err != nil {
		return nil, err
	}
	// if any errs found during db open, close the manifest file.
	defer func() {
		if manifestFile != nil {
			_ = manifestFile.Close()
		}
	}()

	db := &DBImpl{
		imm:           make([]*memTable, 0, options.NumMemtables),
		flushCh:       make(chan *memTable, options.NumMemtables),
		writeCh:       make(chan *writeBatchInternel, writeChCapacity),
		option:        &options,
		manifest:      manifestFile,
		dirLockGuard:  dirLockGuard,
		valueDirGuard: valueDirLockGuard,
		snapshots:     newSnapshotList(),
		tsEntryCache:  make(map[string][]byte),
		keyIndex:      make(map[string]int),
		rwMu:          sync.RWMutex{},
		idx:           atomic.Uint64{},
		vlogs:         make([]*valueLog, 0),
	}

	// Cleanup all the goroutines started by badger in case of an errs.
	defer func() {
		if err != nil {
			options.Logger.Errorf("while opening db, got an errs: %v. Cleaning up...", err)
			db.cleanup()
			db = nil
		}
	}()

	// load old memtables if exists, just read only
	db.option.Logger.Infof("Opening old memtables")
	if err := db.openMemTables(); err != nil {
		return nil, Wrapf(err, "while opening memtables")
	}

	// create new empty memtable for write
	db.option.Logger.Infof("Creating new memtables")
	if db.mem, err = db.newMemTable(); err != nil {
		return nil, Wrapf(err, "cannot create memtable")
	}

	// init value log
	db.option.Logger.Infof("Opening value log")
	db.vlog, err = OpenValueLog(*db.option, 0)

	if db.option.VFileWriteParallelism > 1 {
		db.option.Logger.Infof("Opening value log for write parallelism")
		for i := 1; i < db.option.VFileWriteParallelism; i++ {
			vlog, err := OpenValueLog(*db.option, i)
			if err != nil {
				return db, Wrapf(err, "During value log open")
			}
			db.vlogs = append(db.vlogs, vlog)
		}
	}

	db.vlogs = append(db.vlogs, db.vlog)

	if err != nil {
		return db, Wrapf(err, "During value log open")
	}

	// init levels
	db.option.Logger.Infof("Initing tables")
	if db.ls, err = NewLevels(db.option, &manifest, db.vlog, db.manifest, db.snapshots); err != nil {
		return db, err
	}

	// init maxVersion,
	maxVersion := db.initMaxVersion()
	db.ls.updateMaxVersion(maxVersion)
	db.option.Logger.Infof("Set maxVersion to %d", maxVersion)

	// start compact goroutines
	db.closers.compact = internel.NewCloser(1)
	db.option.Logger.Infof("Starting compact(%d) goroutines", db.option.NumCompactors)
	db.ls.StartCompact(db.closers.compact)

	// start a goroutine for flush old memtables
	db.closers.memtable = internel.NewCloser(1)
	db.option.Logger.Infof("Starting memtable flush")
	go func() {
		_ = db.flushMemTable(db.closers.memtable)
	}()
	for _, mt := range db.imm {
		db.flushCh <- mt
	}

	// start write goroutines
	db.closers.writes = internel.NewCloser(1)
	db.option.Logger.Infof("Starting write goroutine")
	go db.doWrites(db.closers.writes)

	// start value gc goroutine
	db.closers.valueGC = internel.NewCloser(1)
	db.option.Logger.Infof("Starting value log gc goroutine")
	go db.vlog.waitGC(db.closers.valueGC)

	valueDirLockGuard = nil
	dirLockGuard = nil
	manifestFile = nil
	return db, nil
}

func (db *DBImpl) initMaxVersion() uint64 {
	var maxVersion uint64
	set := func(v uint64) {
		if v > maxVersion {
			maxVersion = v
		}
	}

	db.Lock()

	// get version from current memtable first
	set(db.mem.maxVersion)

	// then get version from old immutable memtables
	for _, mt := range db.imm {
		set(mt.maxVersion)
	}

	db.Unlock()

	// final, get version from levels
	for _, t := range db.ls.GetTableMeta() {
		set(t.MaxVersion)
	}

	return maxVersion
}

func (db *DBImpl) getMaxVersion() uint64 {
	return db.ls.MaxVersion()
}

// Close closes a DB. Calling Close(db) multiple times would still only close the DB once.
func Close(db DB) error {
	if err := db.Close(); err != nil {
		return Wrap(err, "close db failed.")
	}

	return nil
}

// RunValueLogGC triggers a value log garbage collection.
//
// It picks value log files to perform GC based on statistics that are collected
// during compactions.  If no such statistics are available, then log files are
// picked in random order. The process stops as soon as the first log file is
// encountered which does not result in garbage collection.
//
// When a log file is picked, it is first sampled. If the sample shows that we
// can discard at least discardRatio space of that file, it would be rewritten.
//
// If a call to RunValueLogGC results in no rewrites, then an ErrNoRewrite is
// thrown indicating that the call resulted in no file rewrites.
//
// We recommend setting discardRatio to 0.5, thus indicating that a file be
// rewritten if half the space can be discarded.  This results in a lifetime
// value log write amplification of 2 (1 from original write + 0.5 rewrite +
// 0.25 + 0.125 + ... = 2). Setting it to higher value would result in fewer
// space reclaims, while setting it to a lower value would result in more space
// reclaims at the cost of increased activity on the LSM tree. discardRatio
// must be in the range (0.0, 1.0), both endpoints excluded, otherwise an
// errs is returned.
//
// Only one GC is allowed at a time. If another value log GC is running, or DB
// has been closed, this would return an ErrRejected.
//
// Note: Every time GC is run, it would produce a spike of activity on the LSM
// tree.
func RunValueLogGC(db DB, discardRatio float64) error {
	if discardRatio >= 1.0 || discardRatio <= 0.0 {
		return fmt.Errorf("discardRatio set to a invalid value(%.2f), it should in (0.0, 1.0).",
			discardRatio)
	}

	dbImpl, _ := db.(*DBImpl)

	return dbImpl.vlog.runGC(discardRatio, db)
}

// Put write the key and value into the db with options
func (db *DBImpl) Put(options *WriteOptions, key, value []byte) error {
	if len(key) == 0 {
		return ErrEmptyKey
	}

	wb := NewWriteBatch(db)
	wb.Put(key, value)

	return db.Write(options, wb)
}

// Delete delete value of the key from db with options
func (db *DBImpl) Delete(options *WriteOptions, key []byte) error {
	if len(key) == 0 {
		return ErrEmptyKey
	}

	wb := NewWriteBatch(db)
	wb.Delete(key)

	return db.Write(options, wb)
}

// Write writes the record in batch into db with write options.
// ATTENTION: write synchronous, if there is no room for write in memtable, it will be blocked.
func (db *DBImpl) Write(options *WriteOptions, batch *WriteBatch) error {
	if err := batch.Validate(); err != nil {
		return err
	}

	batch.sync = options.Sync
	wb, err := db.doWrite(batch)
	if err != nil {
		return err
	}

	if options.Sync {
		return wb.Wait()
	}

	return nil
}

func (db *DBImpl) TSWrite(options *WriteOptions, points []types.TSEntry) error {
	if len(points) == 0 {
		return ErrEmptyKey
	}

	wb := NewWriteBatch(db)

	for _, point := range points {
		//key := point.KeyStr()
		//if options.Separate {
		//	if _, exists := db.tsEntryCache[key]; !exists {
		//		db.rwMu.Lock()
		//		db.tsEntryCache[key] = []byte{}
		//		db.rwMu.Unlock()
		//	}
		//	db.rwMu.Lock()
		//	db.tsEntryCache[key] = append(db.tsEntryCache[key], point.GetBytes()...)
		//	db.rwMu.Unlock()
		//	if len(db.tsEntryCache[key]) > threshold {
		//		intBytes := make([]byte, 8)
		//		binary.BigEndian.PutUint64(intBytes, uint64(db.keyIndex[key]))
		//		db.rwMu.Lock()
		//		db.keyIndex[key]++
		//		db.rwMu.Unlock()
		//		wb.Put(append(point.Key(), intBytes...), db.tsEntryCache[key])
		//		db.tsEntryCache[key] = db.tsEntryCache[key][:0]
		//	}
		//} else {
		//	db.keyIndex[key]++
		//	wb.Put(append(point.Key(), []byte(strconv.Itoa(db.keyIndex[key]))...), point.GetBytes())
		//}
		// db.keyIndex[key]++
		db.idx.Add(1)
		wb.Put(point.Key(), point.GetBytes())
	}
	if wb.sz != 0 {
		return db.Write(options, wb)
	}
	return nil
}

func (db *DBImpl) doWrite(batch *WriteBatch) (*writeBatchInternel, error) {
	if atomic.LoadInt32(&db.blockWrites) == 1 {
		return nil, ErrBlockedWrites
	}

	wb := batchPool.Get().(*writeBatchInternel)
	count := uint64(len(batch.entries))
	version := db.ls.updateMaxVersion(count) + 1
	wb.Reset()
	wb.setVersion(version)
	wb.Fill(batch)
	wb.wg.Add(1)
	wb.IncrRef()

	db.writeCh <- wb

	return wb, nil
}

// Get read newest value of the key if not any snapshot set in options,
// otherwise it will read the version (or below it if there is no such version) in snapshot.
func (db *DBImpl) Get(options *ReadOptions, key []byte) (value []byte, err error) {
	vs, err := db.GetExtend(options, key)
	if err != nil {
		return nil, err
	}

	return vs.Value, nil
}

// NewIterator returns a iterator for the db.
// Iterators have the nuance of being a snapshot of the writes for the transaction at the time
// iterator was created. If writes are performed after an iterator is created, then that iterator
// will not be able to see those writes. Only writes performed before an iterator was created can be
// viewed.
// CAUTION: when done with iteration, a iterator should be closed.
func (db *DBImpl) NewIterator(options *ReadOptions) (iterator Iterator, err error) {
	if db.IsClosed() {
		return nil, ErrDBClosed
	}

	tables, decr := db.getMemTables()
	defer decr()

	var iters []Iterator
	for _, table := range tables {
		iters = append(iters, &memTableIterator{table.skl.Iterator()})
	}

	iters = db.ls.appendIterators(iters, options)
	dbIter := NewDBIterator(db, options,
		NewTablesMergeIterator(db.option, iters, false), db.getMaxVersion())

	return dbIter, nil
}

// GetSnapshot return a snapshot with current max version
func (db *DBImpl) GetSnapshot() *Snapshot {
	return db.snapshots.New(db.getMaxVersion())
}

// ReleaseSnapshot delete the snapshot from db
// CAUTION: when snapshot not be used again, ReleaseSnapshot should be called.
func (db *DBImpl) ReleaseSnapshot(snapshot *Snapshot) {
	db.snapshots.Delete(snapshot)
}

// GetExtend Get read newest value (with meta) of the key if not any snapshot set in options,
// otherwise it will read the version (or below it if there is no such version) in snapshot.
func (db *DBImpl) GetExtend(options *ReadOptions, key []byte) (value *EValue, err error) {
	if len(key) == 0 {
		return nil, ErrEmptyKey
	}

	res, err := db.get(options, key)
	if err != nil {
		return nil, err
	}

	if res.Value == nil && res.Meta == 0 {
		return nil, ErrKeyNotFound
	}

	if res.Meta&Delete == Delete {
		return nil, ErrKeyNotFound
	}

	if res.Meta&ValPtr == ValPtr {
		var vp valPtr
		vp.Decode(res.Value)

		// TODO: need to improve read
		entry, err := db.vlog.Read(options, vp)
		if err != nil {
			db.option.Logger.Errorf("Unable to read from vlog: Key: %v, Version : %v, meta: %v"+
				" Error: %v", key, res.version, res.Meta, err)
			return nil, err
		}

		res.Value = safeCopy(res.Value, entry.value)
	}

	return &res, nil
}

func (db *DBImpl) get(options *ReadOptions, key []byte) (value EValue, err error) {
	if db.IsClosed() {
		return EValue{}, ErrDBClosed
	}

	tables, decr := db.getMemTables()
	defer decr()

	var version = db.getMaxVersion()
	if options.Snapshot != nil {
		version = options.Snapshot.version
	}

	key = KeyWithVersion(key, version)

	for _, table := range tables {
		k, v := table.skl.Get(key)
		if v == nil {
			continue
		}

		if !SameKey(key, k) {
			continue
		}

		// make a deep copy
		var buf []byte
		buf = safeCopy(buf, v)

		var ev EValue
		ev.Decode(buf)

		return ev, nil
	}

	return db.ls.GetValue(key, 0)
}

func (db *DBImpl) getMemTables() ([]*memTable, func()) {
	db.RLock()
	defer db.RUnlock()

	res := []*memTable{db.mem}
	db.mem.IncrRef()

	// get immutable memtables reverse (get newest first)
	for i := len(db.imm) - 1; i >= 0; i-- {
		res = append(res, db.imm[i])
		db.imm[i].IncrRef()
	}

	return res, func() {
		for _, table := range res {
			table.DecrRef()
		}
	}
}

// GetOption return the options used in the db
func (db *DBImpl) GetOption() Options {
	return *db.option
}

// GetTables return the meta of tables in the db
func (db *DBImpl) GetTables() []TableMeta {
	return db.ls.GetTableMeta()
}

// Close closes a DB. It's crucial to call it to ensure all the pending updates make their way to
// disk. Calling DB.Close() multiple times would still only close the DB once.
func (db *DBImpl) Close() error {
	var err error
	db.closeOnce.Do(func() {
		err = db.doClose()
	})

	return err
}

func (db *DBImpl) IsClosed() bool {
	return atomic.LoadUint32(&db.isClosed) == 1
}

func (db *DBImpl) doClose() error {
	var err error
	db.option.Logger.Debugf("Closing database.")
	db.option.Logger.Infof("Lifetime L0 stalled for: %s\n",
		time.Duration(atomic.LoadInt64(&db.ls.l0stallMs)))

	atomic.StoreInt32(&db.blockWrites, 1)

	// stop value gc first.
	db.option.Logger.Infof("Stoping value log gc goroutines")
	db.closers.valueGC.SignalAndWait()

	// stop writes next.
	db.option.Logger.Infof("Stoping write goroutines")
	db.closers.writes.SignalAndWait()

	// close write chan, doesn't accept any writes.
	close(db.writeCh)

	// make sure all mem in flush chan will flush to disk. should push current mem into imm
	if db.mem != nil {
		if db.mem.skl.Empty() {
			db.mem.DecrRef()
		} else {
			db.option.Logger.Debugf("Flushing memtable.")

			pushMem := func() bool {
				db.Lock()
				defer db.Unlock()

				select {
				case db.flushCh <- db.mem:
					db.imm = append(db.imm, db.mem)
					db.mem = nil
					db.option.Logger.Debugf("pushed the memtable into flush chan")

					return true
				default:
					db.option.Logger.Debugf("pushed the memtable into flush chan failed, retry...")
					// do nothing
				}
				return false
			}

			for {
				if pushMem() {
					break
				}

				// push failed, so wait for a while, then try again.
				time.Sleep(10 * time.Millisecond)
			}
		}
	}

	// stop mem flush and wait it finished
	db.option.Logger.Infof("Stoping memtable flush goroutine")
	close(db.flushCh)
	db.closers.memtable.SignalAndWait()

	db.option.Logger.Infof("Closing value log vFile flush cost time: %d s", db.option.Metric.GetVFileFlushTIme()/1000)
	if verr := db.vlog.Close(); verr != nil {
		err = Wrap(err, "DB.close")
	}

	// stop compact and wait it finished
	db.option.Logger.Infof("Stoping compact goroutines")
	db.closers.compact.SignalAndWait()

	db.option.Logger.Infof("Closing tables")
	db.option.Logger.Infof(db.LevelsToString())
	if lErr := db.ls.Close(); err == nil {
		err = Wrap(lErr, "DB.Close")
	}

	// mark the status of db is closed
	atomic.StoreUint32(&db.isClosed, 1)

	// release dir locks
	db.option.Logger.Infof("Releasing directories lock guard")
	if db.dirLockGuard != nil {
		if guardErr := db.dirLockGuard.Release(); err == nil {
			err = Wrap(guardErr, "DB.Close")
		}
	}
	if db.valueDirGuard != nil {
		if guardErr := db.valueDirGuard.Release(); err == nil {
			err = Wrap(guardErr, "DB.Close")
		}
	}

	// close manifest file
	db.option.Logger.Infof("Closing manifest file")
	if manifestErr := db.manifest.Close(); err == nil {
		err = Wrap(manifestErr, "DB.Close")
	}

	// Fsync directories to ensure that lock file, and any other removed files whose directory
	// we haven't specifically fsynced, are guaranteed to have their directory entry removal
	// persisted to disk.
	db.option.Logger.Infof("Syncing database's directories")
	if syncErr := syncDir(db.option.Dir); err == nil {
		err = Wrap(syncErr, "DB.Close")
	}
	if syncErr := syncDir(db.option.ValueLogDir); err == nil {
		err = Wrap(syncErr, "DB.Close")
	}

	return err
}

func (db *DBImpl) cleanup() {
	// stop memtable flush
	db.stopMemTableFlush()

	// stop compact
	db.stopCompact()

	// stop value log gc
	if db.closers.valueGC != nil {
		db.closers.valueGC.Signal()
	}

	// stop write
	if db.closers.writes != nil {
		db.closers.writes.Signal()
	}
}

func (db *DBImpl) stopMemTableFlush() {
	if db.closers.memtable != nil {
		// close flush chan
		close(db.flushCh)
		// wait for running memtable flush finish, keep data consistency
		db.closers.memtable.SignalAndWait()
	}
}

func (db *DBImpl) startMemoryFlush() {
	if db.closers.memtable != nil {
		db.flushCh = make(chan *memTable, db.option.NumMemtables)
		db.closers.memtable = internel.NewCloser(1)
		go func() {
			_ = db.flushMemTable(db.closers.memtable)
		}()
	}
}

func (db *DBImpl) stopCompact() {
	if db.closers.compact != nil {
		// wait for running compact finish, keep data consistency
		db.closers.compact.SignalAndWait()
	}
}

func (db *DBImpl) startCompact() {
	if db.closers.compact != nil {
		db.closers.compact = internel.NewCloser(1)
		db.ls.StartCompact(db.closers.compact)
	}
}

func (db *DBImpl) doWrites(c *internel.Closer) {
	defer c.Done()

	pendingCh := make(chan struct{}, 1)
	write := func(batches []*writeBatchInternel) {
		if err := db.writeBatches(batches); err != nil {
			db.option.Logger.Errorf("write batch errs: %+v", err)
		}

		<-pendingCh
	}

	batches := make([]*writeBatchInternel, 0, 10)
	for {
		var batch *writeBatchInternel
		select {
		case batch = <-db.writeCh:
		case <-c.HasBeenClosed():
			goto closedCase
		}

		for {
			batches = append(batches, batch)

			// write should be blocked if there are so many writes
			if len(batches) >= 3*writeChCapacity {
				pendingCh <- struct{}{}
				goto writeCase
			}

			select {
			case batch = <-db.writeCh:
			case pendingCh <- struct{}{}:
				goto writeCase
			case <-c.HasBeenClosed():
				goto closedCase
			}
		}

	closedCase:
		for {
			select {
			case batch = <-db.writeCh:
				batches = append(batches, batch)
			default:
				pendingCh <- struct{}{}
				write(batches)
				return
			}
		}

	writeCase:
		go write(batches)
		batches = make([]*writeBatchInternel, 0, 10)
	}
}

func (db *DBImpl) writeBatches(batches []*writeBatchInternel) error {
	if len(batches) == 0 {
		return nil
	}

	done := func(err error) {
		for _, batch := range batches {
			batch.err = err
			batch.wg.Done()
		}
	}

	// TODO remove this part
	//db.option.Logger.Debugf("writing batches. Write to value log")
	//currentVlog := db.vlog
	//if db.option.VFileWriteParallelism > 1 {
	//	vId := rand.Intn(db.option.VFileWriteParallelism)
	//	if vId < len(db.vlogs) {
	//		currentVlog = db.vlogs[vId]
	//	}
	//}
	//
	//for _, batch := range batches {
	//	if errs := currentVlog.Write(batch); errs != nil {
	//		done(errs)
	//		return errs
	//	}
	//}

	db.option.Logger.Debugf("Writing to memtable")
	count := 0
	for _, batch := range batches {
		if len(batch.entries) == 0 {
			continue
		}

		count += len(batch.entries)
		var i uint64
		var err error
		for err = db.ensureRoomForWrite(); err == ErrNoRoom; err = db.ensureRoomForWrite() {
			i++

			if i%100 == 0 {
				db.option.Logger.Debugf("Making room for writes.")
			}

			// We need to poll a bit because both ensureRoomForWrite and the flusher need access to s.imm.
			// When flushChan is full and you are blocked there, and the flusher is trying to update s.imm,
			// you will get a deadlock.
			time.Sleep(10 * time.Millisecond)
		}

		if err != nil {
			done(err)
			return err
		}

		if err = db.writeToLSM(batch); err != nil {
			done(err)
			return err
		}
	}

	done(nil)
	db.option.Logger.Debugf("%d entries written.", count)

	return nil
}

func (db *DBImpl) ensureRoomForWrite() error {
	db.Lock()
	defer db.Unlock()

	// current memtable is not full
	if !db.mem.IsFull() {
		return nil
	}

	var err error
	// current memtable is full, move it to imm and flush if have to, then create a new one
	select {
	case db.flushCh <- db.mem:
		db.option.Logger.Debugf("Flushing memtable, memtable.size=%d, flush chan: %d",
			db.mem.skl.Size(), len(db.flushCh))

		db.imm = append(db.imm, db.mem)
		db.mem, err = db.newMemTable()
		if err != nil {
			return Wrapf(err, "cannot create new mem table")
		}

		return nil
	default:
		return ErrNoRoom
	}
}

// TODO
func (db *DBImpl) writeToLSM(batch *writeBatchInternel) error {
	for _, entry := range batch.entries {
		vs := EValue{
			Value: entry.value,
			Meta:  entry.rtype &^ ValPtr,
		}

		if err := db.mem.Put(entry.key, vs); err != nil {
			return Wrapf(err, "while writing to memTable")
		}
	}

	if batch.sync {
		return db.mem.SyncWAL()
	}

	return nil
}

func (db *DBImpl) openMemTables() error {
	files, err := ioutil.ReadDir(db.option.Dir)
	if err != nil {
		return Wrapf(err, "Unable to open mem dir: %q", db.option.Dir)
	}

	var fids []int
	for _, file := range files {
		if !strings.HasSuffix(file.Name(), MemFileExt) {
			continue
		}
		fsz := len(file.Name())
		fid, err := strconv.ParseInt(file.Name()[:fsz-len(MemFileExt)], 10, 64)
		if err != nil {
			return Wrapf(err, "Unable to parse log id. file: %s", file.Name())
		}

		fids = append(fids, int(fid))
	}

	// Sort by fid in ascending order (aka. by table files' created time).
	sort.Slice(fids, func(i, j int) bool {
		return fids[i] < fids[j]
	})
	for _, fid := range fids {
		flags := os.O_RDWR
		mt, err := OpenMemTable(fid, flags, *db.option)
		if err != nil {
			return Wrapf(err, "while opening fid: %d", fid)
		}
		// If this memtable is empty we don't need to add it. This is a
		// memtable that was completely truncated.
		if mt.skl.Empty() {
			mt.DecrRef()
			continue
		}
		// These should no longer be written to. So, make them part of the imm.
		db.imm = append(db.imm, mt)
	}
	if len(fids) != 0 {
		db.nextMemFid = fids[len(fids)-1]
	}

	db.nextMemFid++
	return nil
}

func (db *DBImpl) flushMemTable(c *internel.Closer) error {
	defer c.Done()

	// TODO: 潜在的并行点
	for mt := range db.flushCh {
		if mt == nil {
			continue
		}

		// If an errs occurs during flushing, continue flushing until it is successful.
		for {
			err := db.doFlush(mt)
			if err == nil {
				// remove the flushed memtable.
				db.Lock()
				db.imm = db.imm[1:]
				mt.DecrRef()
				db.Unlock()

				break
			}

			// Encountered errs. Retry indefinitely.
			db.option.Logger.Errorf("Failure while flushing memtable to disk: %v. Retrying...\n",
				err)
			time.Sleep(time.Second)
		}
	}

	return nil
}

func (db *DBImpl) doFlush(mt *memTable) error {
	if mt.skl.Empty() {
		return nil
	}

	builder := buildL0Table(mt, *db.option, db.vlogs)

	if builder.Empty() {
		builder.Finish()
		return nil
	}

	fileId := db.ls.nextTableId()
	table, err := CreateTable(NewTableFileName(fileId, db.option.Dir), builder)
	if err != nil {
		return Wrapf(err, "while creating table")
	}

	err = db.ls.AddLevel0Table(table)
	_ = table.DecrRef()

	return err
}

func (db *DBImpl) newMemTable() (*memTable, error) {
	mem, err := NewMemTable(db.nextMemFid, *db.option)
	if err != nil {
		return nil, err
	}

	db.nextMemFid++
	return mem, nil
}

func calculateSeparateSize(key []byte, value *EValue) uint64 {
	klen, vlen := uint64(len(key)), uint64(len(value.Value))
	return klen + vlen + 1 + 8
}

func subDoSeparate(keys [][]byte, values []*EValue, separateSizes []uint64, vFiles []*valueLog, opts Options, bWg *sync.WaitGroup) {
	parallelism := len(vFiles)
	total := len(keys)
	var wg sync.WaitGroup
	wg.Add(parallelism)
	start := time.Now()
	for i := 0; i < parallelism; i++ {
		s := i * total / parallelism
		e := (i + 1) * total / parallelism
		var separateSize uint64
		for _, value := range separateSizes[s:e] {
			separateSize += value
		}
		go func(vFile *valueLog) {
			defer wg.Done()
			err := vFile.WriteValues(keys[s:e], values[s:e], separateSize, bWg, opts)
			if err != nil {
				opts.Logger.Errorf("Error writing values to file: %v", err)
			}
		}(vFiles[i])
	}
	wg.Wait()
	opts.Metric.RecordVFileFlushTime(time.Since(start).Milliseconds())
}

// buildL0Table builds a new table from the memtable.
func buildL0Table(mt *memTable, opts Options, vFiles []*valueLog) *TableBuilder {
	iter := mt.skl.Iterator()
	defer iter.Close()
	b := NewTableBuilder(opts)

	write_points := uint64(0)
	keys := make([][]byte, 0)
	values := make([]*EValue, 0)
	separateSize := make([]uint64, 0)
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		var vs EValue
		vs.Decode(iter.Value())
		if len(keys) != 0 && SameKey(iter.Key(), keys[len(keys)-1]) {
			if vs.Meta&Delete == Delete {
				keys[len(keys)-1] = iter.Key()
				values[len(keys)-1] = vs.deepCopy()
			} else if values[len(keys)-1].Meta&Delete == Delete {
				keys = append(keys, iter.Key())
				values = append(values, vs.deepCopy())
			} else {
				keys[len(keys)-1] = iter.Key()
				values[len(keys)-1].Value = append(values[len(keys)-1].Value, vs.Value...)
			}
		} else {
			if len(keys) != 0 {
				separateSize = append(separateSize, calculateSeparateSize(keys[len(keys)-1], values[len(keys)-1]))
			}
			keys = append(keys, iter.Key())
			values = append(values, vs.deepCopy())
		}
		write_points++
	}
	opts.Metric.RecordWritePoints(write_points)
	//TODO remove this part
	suf := opts.Metric.IncrWriteIdx()
	for i := 0; i < len(keys); i++ {
		keys[i] = append(append(ParseKey(keys[i]), []byte(strconv.Itoa(int(suf)))...), keys[i][len(keys[i])-8:]...)
	}

	separateSize = append(separateSize, calculateSeparateSize(keys[len(keys)-1], values[len(keys)-1]))
	if opts.Separate {
		subDoSeparate(keys, values, separateSize, vFiles, opts, &b.wg)
	}

	for i := 0; i < len(keys); i++ {
		key := keys[i]
		vs := values[i]
		var vp valPtr
		if vs.Meta&ValPtr > 0 {
			vp.Decode(vs.Value)
		}

		b.Add(key, *vs, vp.len)
	}

	return b
}

func checkOptions(opt *Options) error {
	// It's okay to have zero compactors which will disable all compactions but
	// we cannot have just one compactor otherwise we will end up with all data
	// on level 2.
	if opt.NumCompactors == 1 {
		return errors.New("Cannot have 1 compactor. Need at least 2")
	}

	if opt.Dir == "" || opt.ValueLogDir == "" {
		return errors.New("Unset directory for DB or value log.")
	}

	opt.maxBatchSize = (15 * opt.MemTableSize) / 100
	opt.maxBatchCount = opt.maxBatchSize / internel.MaxNodeSize

	// This is the maximum value, vlogThreshold can have if dynamic thresholding is enabled.
	maxVt := int(math.Min(maxValueThreshold, float64(opt.maxBatchSize)))

	// We are limiting opt.ValueThreshold to maxValueThreshold for now.
	if opt.ValueThreshold > maxVt {
		return fmt.Errorf("Invalid ValueThreshold, must be less or equal to %d",
			maxValueThreshold)
	}

	// If ValueThreshold is greater than opt.maxBatchSize, we won't be able to push any data using
	// the transaction APIs. Transaction batches entries into batches of size opt.maxBatchSize.
	if opt.ValueThreshold > opt.maxBatchSize {
		return fmt.Errorf("Valuethreshold %d greater than max batch size of %d. Either "+
			"reduce opt.ValueThreshold or increase opt.MaxTableSize.",
			opt.ValueThreshold, opt.maxBatchSize)
	}
	// ValueLogFileSize should be stricly LESS than 2<<30 otherwise we will
	// overflow the uint32 when we mmap it in OpenMemtable.
	if !(opt.ValueLogFileSize < 2<<30 && opt.ValueLogFileSize >= 1<<20) {
		return ErrValueLogSize
	}

	return nil
}

func createDirs(dirs []string) error {
	for _, dir := range dirs {
		dirExists, err := exists(dir)
		if err != nil {
			return Wrapf(err, "Invalid dir: %q", dir)
		}

		if !dirExists {
			err = os.MkdirAll(dir, 0700)
			if err != nil {
				return Wrapf(err, "Error Creating dir: %q", dir)
			}
		}
	}

	return nil
}

func exists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return true, err
}

func (db *DBImpl) LevelsToString() string {
	const MB = float64(1 << 20)
	var b strings.Builder
	b.WriteRune('\n')

	base := func(b bool) string {
		if b {
			return "B"
		}
		return " "
	}

	makeReadable := func(n int64) float64 {
		return float64(n) / MB
	}

	for _, l := range db.ls.GetLevelMeta() {
		b.WriteString(fmt.Sprintf(
			"Level %d [%s]: NumTables: %02d. Size: %.2f MiB of %.2f MiB. Score: %.2f->%.2f"+
				" Target FileSize: %.2f MiB\n",
			l.Id, base(l.IsBaseLevel), l.NumTables,
			makeReadable(l.Size), makeReadable(l.TargetSize), l.Score, l.Adjusted,
			makeReadable(l.MaxFileSize)))
	}

	b.WriteString("Level Done\n")
	return b.String()
}

// DropAll would drop all the data stored in Golat. It does this in the following way.
// - Stop accepting new writes.
// - Pause memtable flushes and compactions.
// - Pick all tables from all levels, create a changeset to delete all these
// tables and apply it to manifest.
// - Pick all log files from value log, and delete all of them. Restart value log files from zero.
// - Resume memtable flushes and compactions.
//
// NOTE: DropAll is resilient to concurrent writes, but not to reads. It is up to the user to not do
// any reads while DropAll is going on, otherwise they may result in panics. Ideally, both reads and
// writes are paused before running DropAll, and resumed after it is finished.
func (db *DBImpl) DropAll() error {
	f, err := db.dropAll()
	if f != nil {
		defer f()
	}

	return err
}

func (db *DBImpl) dropAll() (func(), error) {
	db.option.Logger.Infof("DropAll called. Blocking writes...")
	f, err := db.prepareToDrop()
	if err != nil {
		return f, err
	}

	// prepareToDrop will stop all the incomming write and flushes any pending flush tasks.
	// Before we drop, we'll stop the compaction because anyways all the datas are going to
	// be deleted.
	db.stopCompact()
	resume := func() {
		db.startCompact()
		f()
	}

	// Block all foreign interactions with memory tables.
	db.Lock()
	defer db.Unlock()

	// Remove inmemory tables. Calling DecrRef for safety. Not sure if they're absolutely needed.
	db.mem.DecrRef()
	for _, mt := range db.imm {
		mt.DecrRef()
	}
	db.imm = db.imm[:0]
	db.mem, err = db.newMemTable() // Set it up for future writes.
	if err != nil {
		return resume, Wrapf(err, "cannot open new memtable")
	}

	num, err := db.ls.dropTree()
	if err != nil {
		return resume, err
	}
	db.option.Logger.Infof("Deleted %d SSTables. Now deleting value logs...\n", num)

	num, err = db.vlog.dropAll()
	if err != nil {
		return resume, err
	}
	db.ls.nextId = 1
	db.option.Logger.Infof("Deleted %d value log files. DropAll done.\n", num)

	return resume, nil
}

func (db *DBImpl) blockWrite() error {
	// Stop accepting new writes.
	if !atomic.CompareAndSwapInt32(&db.blockWrites, 0, 1) {
		return ErrBlockedWrites
	}

	// Make all pending writes finish. The following will also close writeCh.
	db.closers.writes.SignalAndWait()
	db.option.Logger.Infof("Writes flushed. Stopping compactions now...")
	return nil
}

func (db *DBImpl) unblockWrite() {
	db.closers.writes = internel.NewCloser(1)
	go db.doWrites(db.closers.writes)

	// Resume writes.
	atomic.StoreInt32(&db.blockWrites, 0)
}

func (db *DBImpl) prepareToDrop() (func(), error) {
	// In order prepare for drop, we need to block the incoming writes and
	// write it to db. Then, flush all the pending flushtask. So that, we
	// don't miss any entries.
	if err := db.blockWrite(); err != nil {
		return nil, err
	}
	batches := make([]*writeBatchInternel, 0, 10)
	for {
		select {
		case r := <-db.writeCh:
			batches = append(batches, r)
		default:
			if err := db.writeBatches(batches); err != nil {
				db.option.Logger.Errorf("writeBatches: %v", err)
			}
			db.stopMemTableFlush()
			return func() {
				db.option.Logger.Infof("Resuming writes")
				db.startMemoryFlush()
				db.unblockWrite()
			}, nil
		}
	}
}

const maxNumSplits = 128

// KeySplits can be used to get rough key ranges to divide up iteration over
// the DB.
func (db *DBImpl) KeySplits(prefix []byte) []string {
	var splits []string
	tables := db.GetTables()

	for _, table := range tables {
		if bytes.HasPrefix(table.Biggest, prefix) {
			splits = append(splits, string(table.Biggest))
		}
	}

	// If the number of splits is low, look at the offsets inside the
	// tables to generate more splits.
	if len(splits) < 32 {
		numTables := len(tables)
		if numTables == 0 {
			numTables = 1
		}
		numPerTable := 32 / numTables
		if numPerTable == 0 {
			numPerTable = 1
		}
		splits = db.ls.KeySplits(numPerTable, prefix)
	}

	// If the number of splits is still < 32, then look at the memtables.
	if len(splits) < 32 {
		maxPerSplit := 10000
		mtSplits := func(mt *memTable) {
			if mt == nil {
				return
			}
			count := 0
			iter := mt.skl.Iterator()
			for iter.SeekToFirst(); iter.Valid(); iter.Next() {
				if count%maxPerSplit == 0 {
					// Add a split every maxPerSplit keys.
					if bytes.HasPrefix(iter.Key(), prefix) {
						splits = append(splits, string(iter.Key()))
					}
				}
				count += 1
			}
			_ = iter.Close()
		}

		db.Lock()
		defer db.Unlock()
		var memTables []*memTable
		memTables = append(memTables, db.imm...)
		for _, mt := range memTables {
			mtSplits(mt)
		}
		mtSplits(db.mem)
	}

	sort.Strings(splits)

	// Limit the maximum number of splits returned by this function. We check against
	// maxNumberSplits * 2 so that the jump variable has a value of at least two.
	// Otherwise, the entire list would be returned without any reduction in size.
	if len(splits) > maxNumSplits*2 {
		newSplits := make([]string, 0)
		jump := len(splits) / maxNumSplits
		if jump < 2 {
			jump = 2
		}

		for i := 0; i < len(splits); i += jump {
			if i >= len(splits) {
				i = len(splits) - 1
			}
			newSplits = append(newSplits, splits[i])
		}

		splits = newSplits
	}

	return splits
}

// GetSampleKeys return the sample keys from db.
// The size of those keys equal to sampleSize, if the db has enough keys. Otherwise, it contains all keys
// and use numGoroutines goroutines to fetch those keys
func (db *DBImpl) GetSampleKeys(sampleSize, numGoroutines int) ([][]byte, error) {
	rangeCh := make(chan keyRange, 3)
	keysCh := make(chan []byte, 32)
	getRanges := func(numGoroutines int) {
		splits := db.KeySplits(nil)
		pickEvery := int(math.Floor(float64(len(splits)) / float64(numGoroutines)))
		if pickEvery < 1 {
			pickEvery = 1
		}
		filtered := splits[:0]
		for i, split := range splits {
			if (i+1)%pickEvery == 0 {
				filtered = append(filtered, split)
			}
		}
		splits = filtered

		var start []byte
		for _, key := range splits {
			rangeCh <- keyRange{left: start, right: safeCopy(nil, []byte(key))}
			start = safeCopy(nil, []byte(key))
		}
	}

	iterate := func(ctx context.Context, kr keyRange) error {
		ropt := DefaultReadOptions
		ropt.AllVersion = true

		iter, err := db.NewIterator(&ropt)
		if err != nil {
			return err
		}
		defer iter.Close()

		sendIt := func(key []byte) error {
			select {
			case keysCh <- key:
			case <-ctx.Done():
				return ctx.Err()
			}
			return nil
		}

		var prevKey []byte
		it, _ := iter.(*DBIterator)
		for it.Seek(kr.left); it.Valid(); {
			item := it.GetItem()
			if bytes.Equal(item.Key(), prevKey) {
				it.Next()
				continue
			}

			prevKey = append(prevKey[:0], item.Key()...)

			// Check if we reached the end of the key range.
			if len(kr.right) > 0 && bytes.Compare(item.Key(), kr.right) >= 0 {
				break
			}

			if err := sendIt(item.KeyCopy(nil)); err != nil {
				return err
			}
		}

		return nil
	}

	getKeys := func(ctx context.Context) error {
		for {
			select {
			case kr, ok := <-rangeCh:
				if !ok {
					// Done with the keys.
					return nil
				}
				if err := iterate(ctx, kr); err != nil {
					return err
				}
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}

	go getRanges(numGoroutines)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var count int
	errCh := make(chan error, numGoroutines)
	var wg sync.WaitGroup
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			if err := getKeys(ctx); err != nil {
				select {
				case errCh <- err:
				default:
				}
			}
		}()
	}

	res := [][]byte{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			key, ok := <-keysCh
			if !ok {
				return
			}

			res = append(res, key)
			count++
			if count >= sampleSize {
				cancel()
				return
			}
		}
	}()

	wg.Wait()
	close(keysCh)

	select {
	case err := <-errCh: // Check errs from getKeys.
		return nil, err
	default:
	}

	return res, nil
}

type EValue struct {
	// 元数据信息 删除、新增、指针
	Meta byte
	// 二进制数据
	Value []byte

	version uint64 // not decode or encode
}

func (ev *EValue) Decode(b []byte) {
	// 必须深拷贝 否则可能引发问题
	ev.Meta = b[0]
	ev.Value = b[1:]
}

func (ev *EValue) Encode() []byte {
	sz := 1 + len(ev.Value)
	b := make([]byte, sz)
	b[0] = ev.Meta
	copy(b[1:sz], ev.Value)

	return b
}

func (ev *EValue) EncodeTo(buf *bytes.Buffer) {
	buf.WriteByte(ev.Meta)
	buf.Write(ev.Value)
}

func (ev *EValue) EncodedSize() uint32 {
	return uint32(1 + len(ev.Value))
}

func (ev *EValue) deepCopy() *EValue {
	return &EValue{
		Meta:    ev.Meta,
		Value:   safeCopy(nil, ev.Value),
		version: ev.version,
	}
}
