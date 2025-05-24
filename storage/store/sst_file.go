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
	"context"
	"equinox/pkg/file"
	"equinox/pkg/limiter"
	"equinox/storage/codec"
	"equinox/storage/metric"
	"equinox/storage/types"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"go.uber.org/zap"
)

const (
	// MagicNumber is written as the first 4 bytes of a data file to
	// identify the file as a sst1 formatted file
	MagicNumber uint32 = 0x16D216D2

	// Version indicates the version of the SST file format.
	Version byte = 1

	// Size in bytes of an index entry
	indexEntrySize = 28

	// Size in bytes used to store the count of index entries for a key
	indexCountSize = 2

	// Size in bytes used to store the type of block encoded
	indexTypeSize = 1

	// Max number of blocks for a given key that can exist in a single file
	maxIndexEntries = (1 << (indexCountSize * 8)) - 1

	// max length of a key in an index entry (measurement + tags)
	maxKeyLength = (1 << (2 * 8)) - 1

	// The threshold amount data written before we periodically fsync a SST file.  This helps avoid
	// long pauses due to very large fsyncs at the end of writing a SST file.
	fsyncEvery = 25 * 1024 * 1024
)

const (
	SSTFileExtension    = "sst"
	TmpSSTFileExtension = "tmp"
	BadSSTFileExtension = "bad"
)

// SSTFile represents an on-disk SST file.
type SSTFile interface {
	// Path returns the underlying file path for the SSTFile.  If the file
	// has not be written or loaded from disk, the zero value is returned.
	Path() string

	// Read returns all the values in the block where time t resides.
	Read(key []byte, t int64) ([]types.Value, error)

	// ReadAt returns all the values in the block identified by entry.
	ReadAt(entry *IndexEntry, values []types.Value) ([]types.Value, error)
	ReadFloatBlockAt(entry *IndexEntry, values *[]types.FloatValue) ([]types.FloatValue, error)
	ReadFloatArrayBlockAt(entry *IndexEntry, values *types.FloatArray) error
	ReadIntegerBlockAt(entry *IndexEntry, values *[]types.IntegerValue) ([]types.IntegerValue, error)
	ReadIntegerArrayBlockAt(entry *IndexEntry, values *types.IntegerArray) error
	ReadUnsignedBlockAt(entry *IndexEntry, values *[]types.UnsignedValue) ([]types.UnsignedValue, error)
	ReadUnsignedArrayBlockAt(entry *IndexEntry, values *types.UnsignedArray) error
	ReadStringBlockAt(entry *IndexEntry, values *[]types.StringValue) ([]types.StringValue, error)
	ReadStringArrayBlockAt(entry *IndexEntry, values *types.StringArray) error
	ReadBooleanBlockAt(entry *IndexEntry, values *[]types.BooleanValue) ([]types.BooleanValue, error)
	ReadBooleanArrayBlockAt(entry *IndexEntry, values *types.BooleanArray) error

	// Entries returns the index entries for all blocks for the given key.
	Entries(key []byte) []IndexEntry

	ReadEntries(key []byte, entries *[]IndexEntry) []IndexEntry

	ContainsValue(key []byte, t int64) bool

	// Contains returns true if the file contains any values for the given
	// key.
	Contains(key []byte) bool

	// OverlapsTimeRange returns true if the time range of the file intersect min and max.
	OverlapsTimeRange(min, max int64) bool

	// OverlapsKeyRange returns true if the key range of the file intersects min and max.
	OverlapsKeyRange(min, max []byte) bool

	// TimeRange returns the min and max time across all keys in the file.
	TimeRange() (int64, int64)

	// TombstoneRange returns ranges of time that are deleted for the given key.
	TombstoneRange(key []byte) []TimeRange

	// KeyRange returns the min and max keys in the file.
	KeyRange() ([]byte, []byte)

	// KeyCount returns the number of distinct keys in the file.
	KeyCount() int

	// Seek returns the position in the index with the key <= key.
	Seek(key []byte) int

	// KeyAt returns the key located at index position idx.
	KeyAt(idx int) ([]byte, byte)

	// Type returns the block type of the values stored for the key.  Returns one of
	// BlockFloat64, BlockInt64, BlockBoolean, BlockString.  If key does not exist,
	// an error is returned.
	Type(key []byte) (byte, error)

	// BatchDelete return a BatchDeleter that allows for multiple deletes in batches
	// and group commit or rollback.
	BatchDelete() BatchDeleter

	// Delete removes the keys from the set of keys available in this file.
	Delete(keys [][]byte) error

	// DeleteRange removes the values for keys between timestamps min and max.
	DeleteRange(keys [][]byte, min, max int64) error

	// HasTombstones returns true if file contains values that have been deleted.
	HasTombstones() bool

	TombstoneStats() TombstoneStat

	// Close closes the underlying file resources.
	Close() error

	// Size returns the size of the file on disk in bytes.
	Size() uint32

	// Rename renames the existing SST file to a new name and replaces the mmap backing slice using the new
	// file name. Index and Reader state are not re-initialized.
	Rename(path string) error

	// Remove deletes the file from the filesystem.
	Remove() error

	// InUse returns true if the file is currently in use by queries.
	InUse() bool

	// Ref records that this file is actively in use.
	Ref()

	// Unref records that this file is no longer in use.
	Unref()

	// Stats returns summary information about the SST file.
	Stats() FileStat

	// BlockIterator returns an iterator pointing to the first block in the file and
	// allows sequential iteration to each and every block.
	BlockIterator() *BlockIterator

	// Free releases any resources held by the FileStore to free up system resources.
	Free() error
}

type FileStore struct {
	mu           sync.RWMutex
	lastModified time.Time
	// Most recently known file stats. If nil then stats will need to be
	// recalculated
	lastFileStats []FileStat

	currentGeneration int
	dir               string

	files []SSTFile

	SstMMAPWillNeed bool          // If true then the kernel will be advised MMAP_WILLNEED for SST files.
	OpenLimiter     limiter.Fixed // limit the number of concurrent opening SST files.

	logger       *zap.Logger // Logger to be used for important messages
	traceLogger  *zap.Logger // Logger to be used when trace-logging is on.
	traceLogging bool

	purger *purger

	Metric *metric.FileStoreMetrics

	currentTempDirID int

	parseFileName ParseFileNameFunc

	copyFiles bool

	VM *VFileRegionManager
}

// FileStat holds information about a SST file on disk.
type FileStat struct {
	Path             string
	HasTombstone     bool
	Size             uint32
	LastModified     int64
	MinTime, MaxTime int64
	MinKey, MaxKey   []byte
}

// OverlapsTimeRange returns true if the time range of the file intersect min and max.
func (f FileStat) OverlapsTimeRange(min, max int64) bool {
	return f.MinTime <= max && f.MaxTime >= min
}

// OverlapsKeyRange returns true if the min and max keys of the file overlap the arguments min and max.
func (f FileStat) OverlapsKeyRange(min, max []byte) bool {
	return len(min) != 0 && len(max) != 0 && bytes.Compare(f.MinKey, max) <= 0 && bytes.Compare(f.MaxKey, min) >= 0
}

// ContainsKey returns true if the min and max keys of the file overlap the arguments min and max.
func (f FileStat) ContainsKey(key []byte) bool {
	return bytes.Compare(f.MinKey, key) >= 0 || bytes.Compare(key, f.MaxKey) <= 0
}

// NewFileStore returns a new instance of FileStore based on the given directory.
func NewFileStore(dir string) *FileStore {
	logger := zap.NewNop()
	fs := &FileStore{
		dir:          dir,
		lastModified: time.Time{},
		logger:       logger,
		traceLogger:  logger,
		OpenLimiter:  limiter.NewFixed(runtime.GOMAXPROCS(0)),
		purger: &purger{
			files:  map[string]SSTFile{},
			logger: logger,
		},
		parseFileName: DefaultParseFileName,
		copyFiles:     runtime.GOOS == "windows",
	}
	fs.purger.fileStore = fs
	return fs
}

func (f *FileStore) WithParseFileNameFunc(parseFileNameFunc ParseFileNameFunc) {
	f.parseFileName = parseFileNameFunc
}

func (f *FileStore) ParseFileName(path string) (int, int, error) {
	return f.parseFileName(path)
}

// enableTraceLogging must be called before the FileStore is opened.
func (f *FileStore) enableTraceLogging(enabled bool) {
	f.traceLogging = enabled
	if enabled {
		f.traceLogger = f.logger
	}
}

// WithLogger sets the logger on the file store.
func (f *FileStore) WithLogger(log *zap.Logger) {
	f.logger = log.With(zap.String("service", "filestore"))
	f.purger.logger = f.logger

	if f.traceLogging {
		f.traceLogger = f.logger
	}
}

// Count returns the number of SST files currently loaded.
func (f *FileStore) Count() int {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return len(f.files)
}

// Files returns the slice of SST files currently loaded. This is only used for
// tests, and the files aren't guaranteed to stay valid in the presence of compactions.
func (f *FileStore) Files() []SSTFile {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.files
}

// Free releases any resources held by the FileStore.  The resources will be re-acquired
// if necessary if they are needed after freeing them.
func (f *FileStore) Free() error {
	f.mu.RLock()
	defer f.mu.RUnlock()
	for _, f := range f.files {
		if err := f.Free(); err != nil {
			return err
		}
	}
	return nil
}

// CurrentGeneration returns the current generation of the SST files.
func (f *FileStore) CurrentGeneration() int {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.currentGeneration
}

// NextGeneration increments the max file ID and returns the new value.
func (f *FileStore) NextGeneration() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.currentGeneration++
	return f.currentGeneration
}

// WalkKeys calls fn for every key in every SST file known to the FileStore.  If the key
// exists in multiple files, it will be invoked for each file.
func (f *FileStore) WalkKeys(seek []byte, fn func(key []byte, typ byte) error) error {
	f.mu.RLock()
	if len(f.files) == 0 {
		f.mu.RUnlock()
		return nil
	}

	// Ensure files are not unmapped while we're iterating over them.
	for _, r := range f.files {
		r.Ref()
		defer r.Unref()
	}

	ki := newMergeKeyIterator(f.files, seek)
	f.mu.RUnlock()
	for ki.Next() {
		key, typ := ki.Read()
		if err := fn(key, typ); err != nil {
			return err
		}
	}

	return nil
}

// Keys returns all keys and types for all files in the file store.
func (f *FileStore) Keys() map[string]byte {
	f.mu.RLock()
	defer f.mu.RUnlock()

	uniqueKeys := map[string]byte{}
	if err := f.WalkKeys(nil, func(key []byte, typ byte) error {
		uniqueKeys[string(key)] = typ
		return nil
	}); err != nil {
		return nil
	}

	return uniqueKeys
}

// Type returns the type of values store at the block for key.
func (f *FileStore) Type(key []byte) (byte, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	for _, f := range f.files {
		if f.Contains(key) {
			return f.Type(key)
		}
	}
	return 0, fmt.Errorf("unknown type for %v", key)
}

// Delete removes the keys from the set of keys available in this file.
func (f *FileStore) Delete(keys [][]byte) error {
	return f.DeleteRange(keys, math.MinInt64, math.MaxInt64)
}

func (f *FileStore) Apply(ctx context.Context, fn func(r SSTFile) error) error {
	// Limit apply fn to number of cores
	limiter := limiter.NewFixed(runtime.GOMAXPROCS(0))

	f.mu.RLock()
	errC := make(chan error, len(f.files))

	for _, f := range f.files {
		go func(r SSTFile) {
			if err := limiter.Take(ctx); err != nil {
				errC <- err
				return
			}
			defer limiter.Release()

			r.Ref()
			defer r.Unref()
			errC <- fn(r)
		}(f)
	}

	var applyErr error
	for i := 0; i < cap(errC); i++ {
		if err := <-errC; err != nil {
			applyErr = err
		}
	}
	f.mu.RUnlock()

	f.mu.Lock()
	f.lastModified = time.Now().UTC()
	f.lastFileStats = nil
	f.mu.Unlock()

	return applyErr
}

// DeleteRange removes the values for keys between timestamps min and max.  This should only
// be used with smaller batches of series keys.
func (f *FileStore) DeleteRange(keys [][]byte, min, max int64) error {
	var batches BatchDeleters
	f.mu.RLock()
	for _, f := range f.files {
		if f.OverlapsTimeRange(min, max) {
			batches = append(batches, f.BatchDelete())
		}
	}
	f.mu.RUnlock()

	if len(batches) == 0 {
		return nil
	}

	if err := func() error {
		if err := batches.DeleteRange(keys, min, max); err != nil {
			return err
		}

		return batches.Commit()
	}(); err != nil {
		// Rollback the deletes
		_ = batches.Rollback()
		return err
	}

	f.mu.Lock()
	f.lastModified = time.Now().UTC()
	f.lastFileStats = nil
	f.mu.Unlock()
	return nil
}

// Open loads all the SST files in the configured directory.
func (f *FileStore) Open(ctx context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	// Not loading files from disk so nothing to do
	if f.dir == "" {
		return nil
	}

	if f.OpenLimiter == nil {
		return errors.New("cannot open FileStore without an OpenLimiter (is EngineOptions.OpenLimiter set?)")
	}

	// find the current max ID for temp directories
	tmpfiles, err := os.ReadDir(f.dir)
	if err != nil {
		return err
	}

	// ascertain the current temp directory number by examining the existing
	// directories and choosing the one with the higest basename when converted
	// to an integer.
	for _, fi := range tmpfiles {
		if !fi.IsDir() || !strings.HasSuffix(fi.Name(), "."+TmpSSTFileExtension) {
			continue
		}

		ss := strings.Split(filepath.Base(fi.Name()), ".")
		if len(ss) != 2 {
			continue
		}

		i, err := strconv.Atoi(ss[0])
		if err != nil || i <= f.currentTempDirID {
			continue
		}

		// i must be a valid integer and greater than f.currentTempDirID at this
		// point
		f.currentTempDirID = i
	}

	files, err := filepath.Glob(filepath.Join(f.dir, "*."+SSTFileExtension))
	if err != nil {
		return err
	}

	// struct to hold the result of opening each reader in a goroutine
	type res struct {
		r   *SSTReader
		err error
	}

	readerC := make(chan *res)
	for i, fn := range files {
		// Keep track of the latest ID
		generation, _, err := f.parseFileName(fn)
		if err != nil {
			return err
		}

		if generation >= f.currentGeneration {
			f.currentGeneration = generation + 1
		}

		file, err := os.OpenFile(fn, os.O_RDONLY, 0666)
		if err != nil {
			return fmt.Errorf("error opening file %s: %v", fn, err)
		}

		go func(idx int, file *os.File) {
			// Ensure a limited number of SST files are loaded at once.
			// Systems which have very large datasets (1TB+) can have thousands
			// of SST files which can cause extremely long load times.
			if err := f.OpenLimiter.Take(ctx); err != nil {
				f.logger.Error("Failed to open sst file", zap.String("path", file.Name()), zap.Error(err))
				readerC <- &res{err: fmt.Errorf("failed to open sst file %q: %w", file.Name(), err)}
				return
			}
			defer f.OpenLimiter.Release()

			start := time.Now()
			df, err := NewSSTReader(file, f.VM, WithMadviseWillNeed(f.SstMMAPWillNeed))
			f.logger.Info("Opened file",
				zap.String("path", file.Name()),
				zap.Int("id", idx),
				zap.Duration("duration", time.Since(start)))

			// If we are unable to read a SST file then log the error, rename
			// the file, and continue loading the shard without it.
			if err != nil {
				f.logger.Error("Cannot read corrupt sst file, renaming", zap.String("path", file.Name()), zap.Int("id", idx), zap.Error(err))
				file.Close()
				if e := os.Rename(file.Name(), file.Name()+"."+BadSSTFileExtension); e != nil {
					f.logger.Error("Cannot rename corrupt sst file", zap.String("path", file.Name()), zap.Int("id", idx), zap.Error(e))
					readerC <- &res{r: df, err: fmt.Errorf("cannot rename corrupt file %s: %v", file.Name(), e)}
					return
				}
				readerC <- &res{r: df, err: fmt.Errorf("cannot read corrupt file %s: %v", file.Name(), err)}
				return
			}
			readerC <- &res{r: df}
		}(i, file)
	}

	var lm int64
	isEmpty := true
	for range files {
		res := <-readerC
		if res.err != nil {
			return res.err
		} else if res.r == nil {
			continue
		}
		f.files = append(f.files, res.r)
		// Accumulate file store size stats
		f.Metric.AddSize(int64(res.r.Size()))
		if ts := res.r.TombstoneStats(); ts.TombstoneExists {
			f.Metric.AddSize(int64(ts.Size))
		}

		// Re-initialize the lastModified time for the file store
		if res.r.LastModified() > lm {
			lm = res.r.LastModified()
		}
		isEmpty = false
	}
	if isEmpty {
		if fi, err := os.Stat(f.dir); err == nil {
			f.lastModified = fi.ModTime().UTC()
		} else {
			close(readerC)
			return err
		}
	} else {
		f.lastModified = time.Unix(0, lm).UTC()
	}
	close(readerC)

	sort.Sort(sstReaders(f.files))
	f.Metric.SetFiles(int64(len(f.files)))
	return nil
}

// Close closes the file store.
func (f *FileStore) Close() error {
	// Make the object appear closed to other method calls.
	f.mu.Lock()

	files := f.files

	f.lastFileStats = nil
	f.files = nil

	// Let other methods access this closed object while we do the actual closing.
	f.mu.Unlock()

	var errSlice []error
	for _, tsmFile := range files {
		errSlice = append(errSlice, tsmFile.Close())
	}

	return errors.Join(errSlice...)
}

// Read returns the slice of values for the given key and the given timestamp,
// if any file matches those constraints.
func (f *FileStore) Read(key []byte, t int64) ([]types.Value, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	for _, f := range f.files {
		// Can this file possibly contain this key and timestamp?
		if !f.Contains(key) {
			continue
		}

		// May have the key and time we are looking for so try to find
		v, err := f.Read(key, t)
		if err != nil {
			return nil, err
		}

		if len(v) > 0 {
			return v, nil
		}
	}
	return nil, nil
}

// SSTReader returns a SSTReader for path if one is currently managed by the FileStore.
// Otherwise it returns nil. If it returns a file, you must call Unref on it when
// you are done, and never use it after that.
func (f *FileStore) SSTReader(path string) *SSTReader {
	f.mu.RLock()
	defer f.mu.RUnlock()
	for _, r := range f.files {
		if r.Path() == path {
			r.Ref()
			return r.(*SSTReader)
		}
	}
	return nil
}

// KeyCursor returns a KeyCursor for key and t across the files in the FileStore.
func (f *FileStore) KeyCursor(ctx context.Context, key []byte, t int64, ascending bool) *KeyCursor {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return newKeyCursor(ctx, f, key, t, ascending)
}

// Stats returns the stats of the underlying files, preferring the cached version if it is still valid.
func (f *FileStore) Stats() []FileStat {
	f.mu.RLock()
	if len(f.lastFileStats) > 0 {
		defer f.mu.RUnlock()
		return f.lastFileStats
	}
	f.mu.RUnlock()

	// The file stats cache is invalid due to changes to files. Need to
	// recalculate.
	f.mu.Lock()
	defer f.mu.Unlock()

	if len(f.lastFileStats) > 0 {
		return f.lastFileStats
	}

	// If lastFileStats's capacity is far away from the number of entries
	// we need to add, then we'll reallocate.
	if cap(f.lastFileStats) < len(f.files)/2 {
		f.lastFileStats = make([]FileStat, 0, len(f.files))
	}

	for _, fd := range f.files {
		f.lastFileStats = append(f.lastFileStats, fd.Stats())
	}
	return f.lastFileStats
}

// ReplaceWithCallback replaces oldFiles with newFiles and calls updatedFn with the files to be added the FileStore.
func (f *FileStore) ReplaceWithCallback(oldFiles, newFiles []string, updatedFn func(r []SSTFile)) error {
	return f.replace(oldFiles, newFiles, updatedFn)
}

// Replace replaces oldFiles with newFiles.
func (f *FileStore) Replace(oldFiles, newFiles []string) error {
	return f.replace(oldFiles, newFiles, nil)
}

func (f *FileStore) replace(oldFiles, newFiles []string, updatedFn func(r []SSTFile)) error {
	if len(oldFiles) == 0 && len(newFiles) == 0 {
		return nil
	}

	f.mu.RLock()
	maxTime := f.lastModified
	f.mu.RUnlock()

	updated := make([]SSTFile, 0, len(newFiles))
	sstTmpExt := fmt.Sprintf("%s.%s", SSTFileExtension, TmpSSTFileExtension)

	// Rename all the new files to make them live on restart
	for _, file := range newFiles {
		if !strings.HasSuffix(file, sstTmpExt) && !strings.HasSuffix(file, SSTFileExtension) {
			// This isn't a .sst or .sst.tmp file.
			continue
		}

		var oldName, newName = file, file
		if strings.HasSuffix(oldName, sstTmpExt) {
			// The new SST files have a tmp extension.  First rename them.
			newName = file[:len(file)-4]
			if err := os.Rename(oldName, newName); err != nil {
				return err
			}
		}

		// Any error after this point should result in the file being bein named
		// back to the original name. The caller then has the opportunity to
		// remove it.
		fd, err := os.Open(newName)
		if err != nil {
			if newName != oldName {
				if err1 := os.Rename(newName, oldName); err1 != nil {
					return err1
				}
			}
			return err
		}

		// Keep track of the new mod time
		if stat, err := fd.Stat(); err == nil {
			if maxTime.IsZero() || stat.ModTime().UTC().After(maxTime) {
				maxTime = stat.ModTime().UTC()
			}
		}

		sst, err := NewSSTReader(fd, f.VM, WithMadviseWillNeed(f.SstMMAPWillNeed))
		if err != nil {
			if newName != oldName {
				if err1 := os.Rename(newName, oldName); err1 != nil {
					return err1
				}
			}
			return err
		}

		updated = append(updated, sst)
		f.Metric.AddTotalWritten(int64(sst.Size()))
	}

	if updatedFn != nil {
		updatedFn(updated)
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	// Copy the current set of active files while we rename
	// and load the new files.  We copy the pointers here to minimize
	// the time that locks are held as well as to ensure that the replacement
	// is atomic.

	updated = append(updated, f.files...)

	// We need to prune our set of active files now
	var active, inuse []SSTFile
	for _, file := range updated {
		keep := true
		for _, remove := range oldFiles {
			if remove == file.Path() {
				keep = false

				// If queries are running against this file, then we need to move it out of the
				// way and let them complete.  We'll then delete the original file to avoid
				// blocking callers upstream.  If the process crashes, the temp file is
				// cleaned up at startup automatically.
				//
				// In order to ensure that there are no races with this (file held externally calls Ref
				// after we check InUse), we need to maintain the invariant that every handle to a file
				// is handed out in use (Ref'd), and handlers only ever relinquish the file once (call Unref
				// exactly once, and never use it again). InUse is only valid during a write lock, since
				// we allow calls to Ref and Unref under the read lock and no lock at all respectively.
				if file.InUse() {
					// Copy all the tombstones related to this SST file
					var deletes []string
					if ts := file.TombstoneStats(); ts.TombstoneExists {
						deletes = append(deletes, ts.Path)
					}

					// Rename the SST file used by this reader
					tempPath := fmt.Sprintf("%s.%s", file.Path(), TmpSSTFileExtension)
					if err := file.Rename(tempPath); err != nil {
						return err
					}

					// Remove the old file and tombstones.  We can't use the normal SSTReader.Remove()
					// because it now refers to our temp file which we can't remove.
					for _, f := range deletes {
						if err := os.Remove(f); err != nil {
							return err
						}
					}

					inuse = append(inuse, file)
					continue
				}

				if err := file.Close(); err != nil {
					return err
				}

				if err := file.Remove(); err != nil {
					return err
				}
				break
			}
		}

		if keep {
			active = append(active, file)
		}
	}

	if err := file.SyncDir(f.dir); err != nil {
		return err
	}

	// Tell the purger about our in-use files we need to remove
	f.purger.add(inuse)

	// If times didn't change (which can happen since file mod times are second level),
	// then add a ns to the time to ensure that lastModified changes since files on disk
	// actually did change
	if maxTime.Equal(f.lastModified) || maxTime.Before(f.lastModified) {
		maxTime = f.lastModified.UTC().Add(1)
	}

	f.lastModified = maxTime.UTC()

	f.lastFileStats = nil
	f.files = active
	sort.Sort(sstReaders(f.files))

	f.Metric.SetFiles(int64(len(f.files)))
	// Recalculate the disk size stat
	var totalSize int64
	for _, file := range f.files {
		totalSize += int64(file.Size())
		if ts := file.TombstoneStats(); ts.TombstoneExists {
			totalSize += int64(ts.Size)
		}
	}
	f.Metric.SetSize(totalSize)

	return nil
}

// LastModified returns the last time the file store was updated with new
// SST files or a delete.
func (f *FileStore) LastModified() time.Time {
	f.mu.RLock()
	defer f.mu.RUnlock()

	return f.lastModified
}

// BlockCount returns number of values stored in the block at location idx
// in the file at path.  If path does not match any file in the store, 0 is
// returned.  If idx is out of range for the number of blocks in the file,
// 0 is returned.
func (f *FileStore) BlockCount(path string, idx int) int {
	f.mu.RLock()
	defer f.mu.RUnlock()

	if idx < 0 {
		return 0
	}

	for _, fd := range f.files {
		if fd.Path() == path {
			iter := fd.BlockIterator()
			for i := 0; i < idx; i++ {
				if !iter.Next() {
					return 0
				}
			}
			_, _, _, _, _, block, _ := iter.Read()
			block, _ = f.getDataBlock(block)
			// on Error, BlockCount(block) returns 0 for cnt
			cnt, _ := codec.BlockCount(block)
			return cnt
		}
	}
	return 0
}

// locations returns the files and index blocks for a key and time.  ascending indicates
// whether the key will be scan in ascending time order or descenging time order.
// This function assumes the read-lock has been taken.
func (f *FileStore) locations(key []byte, t int64, ascending bool) []*location {
	var cache []IndexEntry
	locations := make([]*location, 0, len(f.files))
	for _, fd := range f.files {
		minTime, maxTime := fd.TimeRange()

		// If we ascending and the max time of the file is before where we want to start
		// skip it.
		if ascending && maxTime < t {
			continue
			// If we are descending and the min time of the file is after where we want to start,
			// then skip it.
		} else if !ascending && minTime > t {
			continue
		}
		tombstones := fd.TombstoneRange(key)

		// This file could potential contain points we are looking for so find the blocks for
		// the given key.
		entries := fd.ReadEntries(key, &cache)
	LOOP:
		for i := 0; i < len(entries); i++ {
			ie := entries[i]

			// Skip any blocks only contain values that are tombstoned.
			for _, t := range tombstones {
				if t.Min <= ie.MinTime && t.Max >= ie.MaxTime {
					continue LOOP
				}
			}

			// If we ascending and the max time of a block is before where we are looking, skip
			// it since the data is out of our range
			if ascending && ie.MaxTime < t {
				continue
				// If we descending and the min time of a block is after where we are looking, skip
				// it since the data is out of our range
			} else if !ascending && ie.MinTime > t {
				continue
			}

			location := &location{
				r:     fd,
				entry: ie,
			}

			if ascending {
				// For an ascending cursor, mark everything before the seek time as read
				// so we can filter it out at query time
				location.readMin = math.MinInt64
				location.readMax = t - 1
			} else {
				// For an ascending cursort, mark everything after the seek time as read
				// so we can filter it out at query time
				location.readMin = t + 1
				location.readMax = math.MaxInt64
			}
			// Otherwise, add this file and block location
			locations = append(locations, location)
		}
	}
	return locations
}

// MakeSnapshotLinks creates hardlinks from the supplied SSTFiles to
// corresponding files under a supplied directory.
func (f *FileStore) MakeSnapshotLinks(destPath string, files []SSTFile) (returnErr error) {
	for _, sstf := range files {
		newpath := filepath.Join(destPath, filepath.Base(sstf.Path()))
		err := f.copyOrLink(sstf.Path(), newpath)
		if err != nil {
			return err
		}
		if tf := sstf.TombstoneStats(); tf.TombstoneExists {
			newpath := filepath.Join(destPath, filepath.Base(tf.Path))
			err := f.copyOrLink(tf.Path, newpath)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (f *FileStore) copyOrLink(oldpath string, newpath string) error {
	if f.copyFiles {
		f.logger.Info("copying backup snapshots", zap.String("OldPath", oldpath), zap.String("NewPath", newpath))
		if err := f.copyNotLink(oldpath, newpath); err != nil {
			return err
		}
	} else {
		f.logger.Info("linking backup snapshots", zap.String("OldPath", oldpath), zap.String("NewPath", newpath))
		if err := f.linkNotCopy(oldpath, newpath); err != nil {
			return err
		}
	}
	return nil
}

// copyNotLink - use file copies instead of hard links for 2 scenarios:
// Windows does not permit deleting a file with open file handles
// Azure does not support hard links in its default file system
func (f *FileStore) copyNotLink(oldPath, newPath string) (returnErr error) {
	rfd, err := os.Open(oldPath)
	if err != nil {
		return fmt.Errorf("error opening file for backup %s: %q", oldPath, err)
	} else {
		defer func() {
			if e := rfd.Close(); returnErr == nil && e != nil {
				returnErr = fmt.Errorf("error closing source file for backup %s: %w", oldPath, e)
			}
		}()
	}
	fi, err := rfd.Stat()
	if err != nil {
		return fmt.Errorf("error collecting statistics from file for backup %s: %w", oldPath, err)
	}
	wfd, err := os.OpenFile(newPath, os.O_RDWR|os.O_CREATE, fi.Mode())
	if err != nil {
		return fmt.Errorf("error creating temporary file for backup %s:  %w", newPath, err)
	} else {
		defer func() {
			if e := wfd.Close(); returnErr == nil && e != nil {
				returnErr = fmt.Errorf("error closing temporary file for backup %s: %w", newPath, e)
			}
		}()
	}
	if _, err := io.Copy(wfd, rfd); err != nil {
		return fmt.Errorf("unable to copy file for backup from %s to %s: %w", oldPath, newPath, err)
	}
	if err := os.Chtimes(newPath, fi.ModTime(), fi.ModTime()); err != nil {
		return fmt.Errorf("unable to set modification time on temporary backup file %s: %w", newPath, err)
	}
	return nil
}

// linkNotCopy - use hard links for backup snapshots
func (f *FileStore) linkNotCopy(oldPath, newPath string) error {
	if err := os.Link(oldPath, newPath); err != nil {
		if errors.Is(err, syscall.ENOTSUP) {
			if fi, e := os.Stat(oldPath); e == nil && !fi.IsDir() {
				f.logger.Info("file system does not support hard links, switching to copies for backup", zap.String("OldPath", oldPath), zap.String("NewPath", newPath))
				// Force future snapshots to copy
				f.copyFiles = true
				return f.copyNotLink(oldPath, newPath)
			} else if e != nil {
				// Stat failed
				return fmt.Errorf("error creating hard link for backup, cannot determine if %s is a file or directory: %w", oldPath, e)
			} else {
				return fmt.Errorf("error creating hard link for backup - %s is a directory, not a file: %q", oldPath, err)
			}
		} else {
			return fmt.Errorf("error creating hard link for backup from %s to %s: %w", oldPath, newPath, err)
		}
	} else {
		return nil
	}
}

// CreateSnapshot creates hardlinks for all sst and tombstone files
// in the path provided.
func (f *FileStore) CreateSnapshot() (string, error) {
	f.traceLogger.Info("Creating snapshot", zap.String("dir", f.dir))

	f.mu.Lock()
	// create a copy of the files slice and ensure they aren't closed out from
	// under us, nor the slice mutated.
	files := make([]SSTFile, len(f.files))
	copy(files, f.files)

	for _, sstf := range files {
		sstf.Ref()
		defer sstf.Unref()
	}

	// increment and keep track of the current temp dir for when we drop the lock.
	// this ensures we are the only writer to the directory.
	f.currentTempDirID += 1
	tmpPath := fmt.Sprintf("%d.%s", f.currentTempDirID, TmpSSTFileExtension)
	tmpPath = filepath.Join(f.dir, tmpPath)
	f.mu.Unlock()

	// create the tmp directory and add the hard links. there is no longer any shared
	// mutable state.
	err := os.Mkdir(tmpPath, 0777)
	if err != nil {
		return "", err
	}
	if err := f.MakeSnapshotLinks(tmpPath, files); err != nil {
		// remove temporary directory since we couldn't create our hard links.
		_ = os.RemoveAll(tmpPath)
		return "", fmt.Errorf("CreateSnapshot() failed to create links %v: %w", tmpPath, err)
	}

	return tmpPath, nil
}

func (f *FileStore) getDataBlock(block []byte) ([]byte, error) {
	if codec.IsPtrBlock(block) {
		vPtr := &ValuePtr{}
		err := vPtr.UnmarshalBinary(block)
		if err != nil {
			return nil, err
		}
		var vm *VFileManager
		vm, err = f.VM.GetVFileManager(vPtr.LifeCycle)
		if err != nil {
			return nil, err
		}
		block, err = vm.Read(vPtr)
		if err != nil {
			return nil, err
		}
	}
	return block, nil
}

type sstReaders []SSTFile

func (a sstReaders) Len() int           { return len(a) }
func (a sstReaders) Less(i, j int) bool { return a[i].Path() < a[j].Path() }
func (a sstReaders) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
