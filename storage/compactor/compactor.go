package compactor

// Compactions are the process of creating read-optimized TSM files.
// The files are created by converting write-optimized WAL entries
// to read-optimized TSM format.  They can also be created from existing
// TSM files when there are tombstone records that need to be removed, points
// that were overwritten by later writes and need to updated, or multiple
// smaller TSM files need to be merged to reduce File counts and improve
// compression ratios.
//
// The compaction process is stream-oriented using multiple readers and
// iterators.  The resulting stream is written sorted and chunked to allow for
// one-pass writing of a new TSM File.

import (
	"bytes"
	"equinox/pkg/limiter"
	"equinox/storage/memory"
	"equinox/storage/store"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"
)

const DefaultSegmentSize = 10 * 1024 * 1024
const maxTSMFileSize = uint32(2048 * 1024 * 1024) // 2GB
const logEvery = 2 * DefaultSegmentSize
const DefaultMaxPointsPerBlock = 1000

const (
	// DefaultMaxSavedErrors is the number of errors that are stored by a TSMBatchKeyReader before
	// subsequent errors are discarded
	DefaultMaxSavedErrors = 100
)

// Compactor merges multiple TSM files into new files or
// writes a Cache into 1 or more TSM files.
type Compactor struct {
	Dir  string
	Size int

	FileStore interface {
		NextGeneration() int
		TSMReader(path string) *store.TSMReader
	}

	// RateLimit is the limit for disk writes for all concurrent compactions.
	RateLimit limiter.Rate

	formatFileName store.FormatFileNameFunc
	parseFileName  store.ParseFileNameFunc

	mu                 sync.RWMutex
	snapshotsEnabled   bool
	compactionsEnabled bool

	// lastSnapshotDuration is the amount of time the last snapshot took to complete.
	lastSnapshotDuration time.Duration

	snapshotLatencies *latencies

	// The channel to signal that any in progress snapshots should be aborted.
	snapshotsInterrupt chan struct{}
	// The channel to signal that any in progress level compactions should be aborted.
	compactionsInterrupt chan struct{}

	files map[string]struct{}
}

// NewCompactor returns a new instance of Compactor.
func NewCompactor() *Compactor {
	return &Compactor{
		formatFileName: store.DefaultFormatFileName,
		parseFileName:  store.DefaultParseFileName,
	}
}

func (c *Compactor) WithFormatFileNameFunc(formatFileNameFunc store.FormatFileNameFunc) {
	c.formatFileName = formatFileNameFunc
}

func (c *Compactor) WithParseFileNameFunc(parseFileNameFunc store.ParseFileNameFunc) {
	c.parseFileName = parseFileNameFunc
}

// Open initializes the Compactor.
func (c *Compactor) Open() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.snapshotsEnabled || c.compactionsEnabled {
		return
	}

	c.snapshotsEnabled = true
	c.compactionsEnabled = true
	c.snapshotsInterrupt = make(chan struct{})
	c.compactionsInterrupt = make(chan struct{})
	c.snapshotLatencies = &latencies{values: make([]time.Duration, 4)}

	c.files = make(map[string]struct{})
}

// Close disables the Compactor.
func (c *Compactor) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !(c.snapshotsEnabled || c.compactionsEnabled) {
		return
	}
	c.snapshotsEnabled = false
	c.compactionsEnabled = false
	if c.compactionsInterrupt != nil {
		close(c.compactionsInterrupt)
	}
	if c.snapshotsInterrupt != nil {
		close(c.snapshotsInterrupt)
	}
}

// DisableSnapshots disables the compactor from performing snapshots.
func (c *Compactor) DisableSnapshots() {
	c.mu.Lock()
	c.snapshotsEnabled = false
	if c.snapshotsInterrupt != nil {
		close(c.snapshotsInterrupt)
		c.snapshotsInterrupt = nil
	}
	c.mu.Unlock()
}

// EnableSnapshots allows the compactor to perform snapshots.
func (c *Compactor) EnableSnapshots() {
	c.mu.Lock()
	c.snapshotsEnabled = true
	if c.snapshotsInterrupt == nil {
		c.snapshotsInterrupt = make(chan struct{})
	}
	c.mu.Unlock()
}

// DisableSnapshots disables the compactor from performing compactions.
func (c *Compactor) DisableCompactions() {
	c.mu.Lock()
	c.compactionsEnabled = false
	if c.compactionsInterrupt != nil {
		close(c.compactionsInterrupt)
		c.compactionsInterrupt = nil
	}
	c.mu.Unlock()
}

// EnableCompactions allows the compactor to perform compactions.
func (c *Compactor) EnableCompactions() {
	c.mu.Lock()
	c.compactionsEnabled = true
	if c.compactionsInterrupt == nil {
		c.compactionsInterrupt = make(chan struct{})
	}
	c.mu.Unlock()
}

// WriteSnapshot writes a Cache snapshot to one or more new TSM files.
func (c *Compactor) WriteSnapshot(cache *memory.Cache, logger *zap.Logger) ([]string, error) {
	c.mu.RLock()
	enabled := c.snapshotsEnabled
	intC := c.snapshotsInterrupt
	c.mu.RUnlock()

	if !enabled {
		return nil, ErrSnapshotsDisabled
	}

	start := time.Now()
	card := cache.Count()

	// Enable throttling if we have lower cardinality or snapshots are going fast.
	throttle := card < 3e6 && c.snapshotLatencies.avg() < 15*time.Second

	// Write snapshot concurrently if cardinality is relatively high.
	concurrency := card / 2e6
	if concurrency < 1 {
		concurrency = 1
	}

	// Special case very high cardinality, use max concurrency and don't throttle writes.
	if card >= 3e6 {
		concurrency = 4
		throttle = false
	}

	splits := cache.Split(concurrency)

	type res struct {
		files []string
		err   error
	}

	resC := make(chan res, concurrency)
	for i := 0; i < concurrency; i++ {
		go func(sp *memory.ReadableCache) {
			iter := NewCacheKeyIterator(sp, intC)
			defer func(iter KeyIterator) {
				_ = iter.Close()
			}(iter)
			files, err := c.writeNewFiles(c.FileStore.NextGeneration(), 0, nil, iter, throttle, logger)
			resC <- res{files: files, err: err}
		}(splits[i])
	}

	var err error
	files := make([]string, 0, concurrency)
	for i := 0; i < concurrency; i++ {
		result := <-resC
		if result.err != nil {
			err = result.err
		}
		files = append(files, result.files...)
	}

	dur := time.Since(start).Truncate(time.Second)

	c.mu.Lock()

	// See if we were disabled while writing a snapshot
	enabled = c.snapshotsEnabled
	c.lastSnapshotDuration = dur
	c.snapshotLatencies.add(time.Since(start))
	c.mu.Unlock()

	if !enabled {
		return nil, ErrSnapshotsDisabled
	}

	return files, err
}

// compact writes multiple smaller TSM files into 1 or more larger files.
func (c *Compactor) compact(fast bool, tsmFiles []string, logger *zap.Logger) ([]string, error) {
	size := c.Size
	if size <= 0 {
		size = DefaultMaxPointsPerBlock
	}

	c.mu.RLock()
	intC := c.compactionsInterrupt
	c.mu.RUnlock()

	// The new compacted files need to added to the max generation in the
	// set.  We need to find that max generation as well as the max sequence
	// number to ensure we write to the next unique location.
	var maxGeneration, maxSequence int
	for _, f := range tsmFiles {
		gen, seq, err := c.parseFileName(f)
		if err != nil {
			return nil, err
		}

		if gen > maxGeneration {
			maxGeneration = gen
			maxSequence = seq
		}

		if gen == maxGeneration && seq > maxSequence {
			maxSequence = seq
		}
	}

	// For each TSM File, create a TSM reader
	var trs []*store.TSMReader
	for _, file := range tsmFiles {
		select {
		case <-intC:
			return nil, ErrCompactionAborted{}
		default:
		}

		tr := c.FileStore.TSMReader(file)
		if tr == nil {
			// This would be a bug if this occurred as tsmFiles passed in should only be
			// assigned to one compaction at any one time.  A nil tr would mean the File
			// doesn't exist.
			return nil, ErrCompactionAborted{fmt.Errorf("bad plan: %s", file)}
		}
		defer tr.Unref() // inform that we're done with this reader when this method returns.
		trs = append(trs, tr)
	}

	if len(trs) == 0 {
		logger.Debug("No input files")
		return nil, nil
	}

	tsm, err := NewTSMBatchKeyIterator(size, fast, DefaultMaxSavedErrors, intC, tsmFiles, trs...)
	if err != nil {
		return nil, err
	}

	return c.writeNewFiles(maxGeneration, maxSequence, tsmFiles, tsm, true, logger)
}

// CompactFull writes multiple smaller TSM files into 1 or more larger files.
func (c *Compactor) CompactFull(tsmFiles []string, logger *zap.Logger) ([]string, error) {
	c.mu.RLock()
	enabled := c.compactionsEnabled
	c.mu.RUnlock()

	if !enabled {
		return nil, ErrCompactionsDisabled
	}

	if !c.add(tsmFiles) {
		return nil, ErrCompactionInProgress{}
	}
	defer c.remove(tsmFiles)

	files, err := c.compact(false, tsmFiles, logger)

	// See if we were disabled while writing a snapshot
	c.mu.RLock()
	enabled = c.compactionsEnabled
	c.mu.RUnlock()

	if !enabled {
		if err := c.removeTmpFiles(files); err != nil {
			return nil, err
		}
		return nil, ErrCompactionsDisabled
	}

	return files, err
}

// CompactFast writes multiple smaller TSM files into 1 or more larger files.
func (c *Compactor) CompactFast(tsmFiles []string, logger *zap.Logger) ([]string, error) {
	c.mu.RLock()
	enabled := c.compactionsEnabled
	c.mu.RUnlock()

	if !enabled {
		return nil, ErrCompactionsDisabled
	}

	if !c.add(tsmFiles) {
		return nil, ErrCompactionInProgress{}
	}
	defer c.remove(tsmFiles)

	files, err := c.compact(true, tsmFiles, logger)

	// See if we were disabled while writing a snapshot
	c.mu.RLock()
	enabled = c.compactionsEnabled
	c.mu.RUnlock()

	if !enabled {
		if err := c.removeTmpFiles(files); err != nil {
			return nil, err
		}
		return nil, ErrCompactionsDisabled
	}

	return files, err

}

// removeTmpFiles is responsible for cleaning up a compaction that
// was started, but then abandoned before the temporary files were dealt with.
func (c *Compactor) removeTmpFiles(files []string) error {
	for _, f := range files {
		if err := os.Remove(f); err != nil {
			return fmt.Errorf("error removing temp compaction File: %v", err)
		}
	}
	return nil
}

// writeNewFiles writes from the iterator into new TSM files, rotating
// to a new File once it has reached the max TSM File size.
func (c *Compactor) writeNewFiles(generation, sequence int, src []string, iter KeyIterator, throttle bool, logger *zap.Logger) ([]string, error) {
	// These are the new TSM files written
	var files []string

	for {
		sequence++

		// New TSM files are written to a temp File and renamed when fully completed.
		fileName := filepath.Join(c.Dir, c.formatFileName(generation, sequence)+"."+store.TSMFileExtension+"."+store.TmpTSMFileExtension)
		logger.Debug("Compacting files", zap.Int("file_count", len(src)), zap.String("output_file", fileName))

		// Write as much as possible to this File
		err := c.write(fileName, iter, throttle, logger)

		// We've hit the max File limit and there is more to write.  Create a new File
		// and continue.
		if errors.Is(err, ErrMaxFileExceeded) || errors.Is(err, store.ErrMaxBlocksExceeded) {
			files = append(files, fileName)
			logger.Debug("File size or block count exceeded, opening another output File", zap.String("output_file", fileName))
			continue
		} else if errors.Is(err, store.ErrNoValues) {
			logger.Debug("Dropping empty File", zap.String("output_file", fileName))
			// If the File only contained tombstoned entries, then it would be a 0 length
			// File that we can drop.
			if err := os.RemoveAll(fileName); err != nil {
				return nil, err
			}
			break
		} else if _, ok := err.(ErrCompactionInProgress); ok {
			// Don't clean up the File as another compaction is using it.  This should not happen as the
			// planner keeps track of which files are assigned to compaction plans now.
			return nil, err
		} else if err != nil {
			// We hit an error and didn't finish the compaction.  Abort.
			// Remove any tmp files we already completed
			// discard later errors to return the first one from the write() call
			for _, f := range files {
				_ = os.RemoveAll(f)
			}
			// Remove the temp File
			// discard later errors to return the first one from the write() call
			_ = os.RemoveAll(fileName)
			return nil, err
		}

		files = append(files, fileName)
		break
	}

	return files, nil
}

func (c *Compactor) write(path string, iter KeyIterator, throttle bool, logger *zap.Logger) (err error) {
	fd, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_EXCL, 0666)
	if err != nil {
		return ErrCompactionInProgress{err: err}
	}

	// syncingWriter ensures that whatever we wrap the above File descriptor in
	// it will always be able to be synced by the tsm writer, since it does
	// type assertions to attempt to sync.
	type syncingWriter interface {
		io.Writer
		Sync() error
	}

	// Create the write for the new TSM File.
	var (
		w           store.SSTWriter
		limitWriter syncingWriter = fd
	)

	if c.RateLimit != nil && throttle {
		limitWriter = limiter.NewWriterWithRate(fd, c.RateLimit)
	}

	// Use a disk based TSM buffer if it looks like we might create a big index
	// in memory.
	if iter.EstimatedIndexSize() > 64*1024*1024 {
		w, err = store.NewTSMWriterWithDiskBuffer(limitWriter)
		if err != nil {
			return err
		}
	} else {
		w, err = store.NewTSMWriter(limitWriter)
		if err != nil {
			return err
		}
	}

	defer func() {
		closeErr := w.Close()
		if err == nil {
			err = closeErr
		}

		// Check for errors where we should not remove the File
		_, inProgress := err.(ErrCompactionInProgress)
		maxBlocks := err == store.ErrMaxBlocksExceeded
		maxFileSize := err == ErrMaxFileExceeded
		if inProgress || maxBlocks || maxFileSize {
			return
		}

		if err != nil {
			_ = w.Remove()
		}
	}()

	lastLogSize := w.Size()
	for iter.Next() {
		c.mu.RLock()
		enabled := c.snapshotsEnabled || c.compactionsEnabled
		c.mu.RUnlock()

		if !enabled {
			return ErrCompactionAborted{}
		}
		// Each call to read returns the next sorted key (or the prior one if there are
		// more values to write).  The size of values will be less than or equal to our
		// chunk size (1000)
		key, minTime, maxTime, block, err := iter.Read()
		if err != nil {
			return err
		}

		if minTime > maxTime {
			return fmt.Errorf("invalid index entry for block. min=%d, max=%d", minTime, maxTime)
		}

		// Write the key and value
		if err := w.WriteBlock(key, minTime, maxTime, block); err == store.ErrMaxBlocksExceeded {
			if err := w.WriteIndex(); err != nil {
				return err
			}
			return err
		} else if err != nil {
			return err
		}

		// If we have a max File size configured and we're over it, close out the File
		// and return the error.
		if w.Size() > maxTSMFileSize {
			if err := w.WriteIndex(); err != nil {
				return err
			}

			return ErrMaxFileExceeded
		} else if (w.Size() - lastLogSize) > logEvery {
			logger.Debug("Compaction progress", zap.String("output_file", path), zap.Uint32("size", w.Size()))
			lastLogSize = w.Size()
		}
	}

	// Were there any errors encountered during iteration?
	if err := iter.Err(); err != nil {
		return err
	}

	// We're all done.  Close out the File.
	if err := w.WriteIndex(); err != nil {
		return err
	}
	logger.Debug("Compaction finished", zap.String("output_file", path), zap.Uint32("size", w.Size()))
	return nil
}

func (c *Compactor) add(files []string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	// See if the new files are already in use
	for _, f := range files {
		if _, ok := c.files[f]; ok {
			return false
		}
	}

	// Mark all the new files in use
	for _, f := range files {
		c.files[f] = struct{}{}
	}
	return true
}

func (c *Compactor) remove(files []string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, f := range files {
		delete(c.files, f)
	}
}

type TSMErrors []error

func (t TSMErrors) Error() string {
	e := []string{}
	for _, v := range t {
		e = append(e, v.Error())
	}
	return strings.Join(e, ", ")
}

type block struct {
	key              []byte
	minTime, maxTime int64
	typ              byte
	b                []byte
	tombstones       []store.TimeRange

	// readMin, readMax are the timestamps range of values have been
	// read and encoded from this block.
	readMin, readMax int64
}

func (b *block) overlapsTimeRange(min, max int64) bool {
	return b.minTime <= max && b.maxTime >= min
}

func (b *block) read() bool {
	return b.readMin <= b.minTime && b.readMax >= b.maxTime
}

func (b *block) markRead(min, max int64) {
	if min < b.readMin {
		b.readMin = min
	}

	if max > b.readMax {
		b.readMax = max
	}
}

func (b *block) partiallyRead() bool {
	// If readMin and readMax are still the initial values, nothing has been read.
	if b.readMin == int64(math.MaxInt64) && b.readMax == int64(math.MinInt64) {
		return false
	}
	return b.readMin != b.minTime || b.readMax != b.maxTime
}

type blocks []*block

func (a blocks) Len() int { return len(a) }

func (a blocks) Less(i, j int) bool {
	cmp := bytes.Compare(a[i].key, a[j].key)
	if cmp == 0 {
		return a[i].minTime < a[j].minTime && a[i].maxTime < a[j].minTime
	}
	return cmp < 0
}

func (a blocks) Swap(i, j int) { a[i], a[j] = a[j], a[i] }

type latencies struct {
	i      int
	values []time.Duration
}

func (l *latencies) add(t time.Duration) {
	l.values[l.i%len(l.values)] = t
	l.i++
}

func (l *latencies) avg() time.Duration {
	var n int64
	var sum time.Duration
	for _, v := range l.values {
		if v == 0 {
			continue
		}
		sum += v
		n++
	}

	if n > 0 {
		return time.Duration(int64(sum) / n)
	}
	return time.Duration(0)
}
