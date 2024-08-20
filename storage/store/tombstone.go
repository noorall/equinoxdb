package store

import (
	"bufio"
	"compress/gzip"
	"encoding/binary"
	"equinox/pkg/file"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

const TombstoneFileExtension = "tombstone"
const (
	headerSize = 4
	tombHeader = 0x1503
)

// Tombstoner records tombstones when entries are deleted.
type Tombstoner struct {
	mu sync.RWMutex

	// Path is the location of the file to record tombstone. This should be the
	// full path to a SST file.
	Path string

	FilterFn func(k []byte) bool

	tombstoneStats TombstoneStat

	// Tombstones that have been written but not flushed to disk yet.
	tombstones []Tombstone

	// indicates that the stats may be out of sync with what is on disk and they
	// should be refreshed.
	statsLoaded bool

	// These are references used for pending writes that have not been committed.  If
	// these are nil, then no pending writes are in progress.
	gz                *gzip.Writer
	bw                *bufio.Writer
	pendingFile       *os.File
	tmp               [8]byte
	lastAppliedOffset int64
}

type TombstoneStat struct {
	TombstoneExists bool
	Path            string
	LastModified    int64
	Size            uint32
}

// NewTombstoner constructs a Tombstoner for the given path. FilterFn can be nil.
func NewTombstoner(path string, filterFn func(k []byte) bool) *Tombstoner {
	return &Tombstoner{
		Path:     path,
		FilterFn: filterFn,
	}
}

// Tombstone represents an individual deletion.
type Tombstone struct {
	// Key is the tombstoned series key.
	Key []byte

	// Min and Max are the min and max unix nanosecond time ranges of Key that are deleted.  If
	// the full range is deleted, both values are -1.
	Min, Max int64
}

// Add adds the all keys, across all timestamps, to the tombstone.
func (t *Tombstoner) Add(keys [][]byte) error {
	return t.AddRange(keys, math.MinInt64, math.MaxInt64)
}

// AddRange adds all keys to the tombstone specifying only the data between min and max to be removed.
func (t *Tombstoner) AddRange(keys [][]byte, min, max int64) error {
	for t.FilterFn != nil && len(keys) > 0 && !t.FilterFn(keys[0]) {
		keys = keys[1:]
	}

	if len(keys) == 0 {
		return nil
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	// If this SSTFile has not been written (mainly in tests), don't write a
	// tombstone because the keys will not be written when it's actually saved.
	if t.Path == "" {
		return nil
	}

	t.statsLoaded = false

	if cap(t.tombstones) < len(t.tombstones)+len(keys) {
		ts := make([]Tombstone, len(t.tombstones), len(t.tombstones)+len(keys))
		copy(ts, t.tombstones)
		t.tombstones = ts
	}

	for _, k := range keys {
		if t.FilterFn != nil && !t.FilterFn(k) {
			continue
		}

		t.tombstones = append(t.tombstones, Tombstone{
			Key: k,
			Min: min,
			Max: max,
		})
	}
	return t.writeTombstone(t.tombstones)
}

func (t *Tombstoner) Flush() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if err := t.commit(); err != nil {
		// Reset our temp references and clean up.
		_ = t.rollback()
		return err
	}
	return nil
}

func (t *Tombstoner) Rollback() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.rollback()
}

// Delete removes all the tombstone files from disk.
func (t *Tombstoner) Delete() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := os.RemoveAll(t.tombstonePath()); err != nil {
		return err
	}
	t.statsLoaded = false
	t.lastAppliedOffset = 0

	return nil
}

// HasTombstones return true if there are any tombstone entries recorded.
func (t *Tombstoner) HasTombstones() bool {
	t.mu.RLock()
	n := len(t.tombstones)
	t.mu.RUnlock()

	return n > 0
}

// Walk calls fn for every Tombstone under the Tombstoner.
func (t *Tombstoner) Walk(fn func(t Tombstone) error) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	f, err := os.Open(t.tombstonePath())
	if os.IsNotExist(err) {
		return nil
	} else if err != nil {
		return err
	}
	defer f.Close()

	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return err
	}

	return t.readTombstone(f, fn)
}

func (t *Tombstoner) writeTombstone(tombstones []Tombstone) error {
	tmp, err := os.CreateTemp(filepath.Dir(t.Path), TombstoneFileExtension)
	if err != nil {
		return err
	}
	defer tmp.Close()

	var b [8]byte

	bw := bufio.NewWriterSize(tmp, 1024*1024)

	binary.BigEndian.PutUint32(b[:4], tombHeader)
	if _, err := bw.Write(b[:4]); err != nil {
		return err
	}

	gz := gzip.NewWriter(bw)
	for _, ts := range tombstones {
		if err := t.doWriteTombstone(gz, ts); err != nil {
			return err
		}
	}

	t.gz = gz
	t.bw = bw
	t.pendingFile = tmp
	t.tombstones = t.tombstones[:0]

	return t.commit()
}

func (t *Tombstoner) commit() error {
	// No pending writes
	if t.pendingFile == nil {
		return nil
	}

	if err := t.gz.Close(); err != nil {
		return err
	}

	if err := t.bw.Flush(); err != nil {
		return err
	}

	// fsync the file to flush the write
	if err := t.pendingFile.Sync(); err != nil {
		return err
	}

	tmpFilename := t.pendingFile.Name()
	t.pendingFile.Close()

	if err := file.RenameFile(tmpFilename, t.tombstonePath()); err != nil {
		return err
	}

	if err := file.SyncDir(filepath.Dir(t.tombstonePath())); err != nil {
		return err
	}

	t.pendingFile = nil
	t.bw = nil
	t.gz = nil

	return nil
}

func (t *Tombstoner) rollback() error {
	if t.pendingFile == nil {
		return nil
	}

	tmpFilename := t.pendingFile.Name()
	t.pendingFile.Close()
	t.gz = nil
	t.bw = nil
	t.pendingFile = nil
	return os.Remove(tmpFilename)
}

// readTombstone reads the third version of tombstone files that are capable
// of storing keys and the range of time for the key that points were deleted. This
// format is a binary and compressed with gzip.
func (t *Tombstoner) readTombstone(f *os.File, fn func(t Tombstone) error) error {
	// Skip header, already checked earlier
	if _, err := f.Seek(headerSize, io.SeekStart); err != nil {
		return err
	}

	var (
		min, max int64
		key      []byte
	)

	gr, err := gzip.NewReader(bufio.NewReader(f))
	if err != nil {
		return err
	}
	defer gr.Close()

	b := make([]byte, 4096)
	for {
		if _, err = io.ReadFull(gr, b[:4]); err == io.EOF || err == io.ErrUnexpectedEOF {
			break
		} else if err != nil {
			return err
		}

		keyLen := int(binary.BigEndian.Uint32(b[:4]))
		if keyLen > len(b) {
			b = make([]byte, keyLen)
		}

		if _, err := io.ReadFull(gr, b[:keyLen]); err != nil {
			return err
		}

		// Copy the key since b is re-used
		key = make([]byte, keyLen)
		copy(key, b[:keyLen])

		if _, err := io.ReadFull(gr, b[:8]); err != nil {
			return err
		}

		min = int64(binary.BigEndian.Uint64(b[:8]))

		if _, err := io.ReadFull(gr, b[:8]); err != nil {
			return err
		}

		max = int64(binary.BigEndian.Uint64(b[:8]))

		if err := fn(Tombstone{
			Key: key,
			Min: min,
			Max: max,
		}); err != nil {
			return err
		}
	}

	for _, t := range t.tombstones {
		if err := fn(t); err != nil {
			return err
		}
	}
	return nil
}

func (t *Tombstoner) tombstonePath() string {
	if strings.HasSuffix(t.Path, TombstoneFileExtension) {
		return t.Path
	}

	// Filename is 0000001.sst1
	filename := filepath.Base(t.Path)

	// Strip off the sst1
	ext := filepath.Ext(filename)
	if ext != "" {
		filename = strings.TrimSuffix(filename, ext)
	}

	// Append the "tombstone" suffix to create a 0000001.tombstone file
	return filepath.Join(filepath.Dir(t.Path), filename+"."+TombstoneFileExtension)
}

func (t *Tombstoner) doWriteTombstone(dst io.Writer, ts Tombstone) error {
	binary.BigEndian.PutUint32(t.tmp[:4], uint32(len(ts.Key)))
	if _, err := dst.Write(t.tmp[:4]); err != nil {
		return err
	}
	if _, err := dst.Write([]byte(ts.Key)); err != nil {
		return err
	}
	binary.BigEndian.PutUint64(t.tmp[:], uint64(ts.Min))
	if _, err := dst.Write(t.tmp[:]); err != nil {
		return err
	}

	binary.BigEndian.PutUint64(t.tmp[:], uint64(ts.Max))
	_, err := dst.Write(t.tmp[:])
	return err
}

func (t *Tombstoner) TombstoneStats() TombstoneStat {
	t.mu.RLock()
	if t.statsLoaded {
		stats := t.tombstoneStats
		t.mu.RUnlock()
		return stats
	}
	t.mu.RUnlock()

	stat, err := os.Stat(t.tombstonePath())
	if err != nil {
		t.mu.Lock()
		// The file doesn't exist so record that we tried to load it so
		// we don't continue to keep trying.  This is the common case.
		t.statsLoaded = os.IsNotExist(err)
		t.tombstoneStats.TombstoneExists = false
		stats := t.tombstoneStats
		t.mu.Unlock()
		return stats
	}

	t.mu.Lock()
	t.tombstoneStats = TombstoneStat{
		TombstoneExists: true,
		Path:            t.tombstonePath(),
		LastModified:    stat.ModTime().UnixNano(),
		Size:            uint32(stat.Size()),
	}
	t.statsLoaded = true
	stats := t.tombstoneStats
	t.mu.Unlock()

	return stats
}
