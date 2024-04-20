package equinox

import (
	"bytes"
	"equinox/internel"
	"fmt"
	"math"
	"os"
	"path/filepath"
)

const MemFileExt string = ".mem"

type memTable struct {
	skl *internel.Skiplist
	wal *logFile
	buf *bytes.Buffer

	option     Options
	maxVersion uint64
	NewTable   bool
}

func NewMemTable(id int, option Options) (*memTable, error) {
	mt, err := OpenMemTable(id, os.O_CREATE|os.O_RDWR, option)
	if err == nil && mt.NewTable {
		return mt, nil
	}

	if err != nil {
		return nil, Wrapf(err, "create a new memtable: %s", mt.wal.Fd.Name())
	}

	return nil, fmt.Errorf("file %s already exists", mt.wal.Fd.Name())
}

func OpenMemTable(fid, flags int, option Options) (*memTable, error) {
	filePath := memTableFilePath(option.Dir, fid)
	skl := internel.NewSkiplist(arenaSize(option), option.comparator)
	mt := &memTable{
		skl:    skl,
		option: option,
		buf:    &bytes.Buffer{},
	}

	mt.wal = &logFile{
		fid:  uint32(fid),
		path: filePath,
	}

	err := mt.wal.Open(filePath, flags, 2*option.MemTableSize)
	if err != nil {
		return nil, Wrapf(err, "while opening memtable: %s", filePath)
	}

	skl.Hanlder = func() {
		if err := mt.wal.Delete(); err != nil {
			option.Logger.Errorf("while deleting file: %s, err: %v", filePath, err)
		}
	}

	if mt.wal.MmapFile.NewFile {
		mt.NewTable = true
		return mt, nil
	}

	// restore from wal
	err = mt.restoreFromWAL()

	return mt, Wrap(err, "while restore from wal file")
}

func (m *memTable) Put(key []byte, value EValue) error {
	e := &entry{
		key:   key,
		value: value.Value,
		rtype: value.Meta,
	}

	// TODO may be cause a concurrent writes problem, because use a common buf
	if err := m.wal.WriteEntry(e, m.buf); err != nil {
		return Wrap(err, "cannot write entry to WAL file")
	}

	m.skl.Put(key, value.Encode())
	if version := ParseVersion(key); version > m.maxVersion {
		m.maxVersion = version
	}

	return nil
}

func (m *memTable) IsFull() bool {
	if m.skl.Size() >= uint32(m.option.MemTableSize) {
		return true
	}

	return m.wal.pos >= uint32(m.option.MemTableSize)
}

func (m *memTable) SyncWAL() error {
	return m.wal.Sync()
}

func (m *memTable) IncrRef() {
	m.skl.Ref()
}

// DecrRef decrements the refcount, deallocating the Skiplist when done using it
func (m *memTable) DecrRef() {
	m.skl.Deref()
}

func (m *memTable) restoreFromWAL() error {
	if m.wal == nil || m.skl == nil {
		return nil
	}

	end, err := m.wal.iterate(0, m.restore())
	if err != nil {
		return Wrapf(err, "while restore memtable from wal: %s", m.wal.Fd.Name())
	}

	return m.wal.Truncate(int64(end))
}

func (m *memTable) restore() walker {
	return func(e *entry, _ valPtr) error {
		ev := EValue{
			Meta:  byte(e.rtype),
			Value: e.value,
		}

		if version := ParseVersion(e.key); version > m.maxVersion {
			m.maxVersion = version
		}
		m.skl.Put(e.key, ev.Encode())
		return nil
	}
}

func memTableFilePath(dir string, fid int) string {
	return filepath.Join(dir, fmt.Sprintf("%05d%s", fid, MemFileExt))
}

func arenaSize(option Options) uint32 {
	sz := int64(option.MemTableSize + option.maxBatchSize +
		option.maxBatchCount*internel.MaxNodeSize)

	if sz > math.MaxUint32 {
		panic(fmt.Sprintf("invalid size for arena: %d", sz))
	}

	return uint32(sz)
}
