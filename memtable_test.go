package equinox

import (
	"bytes"
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

var testDir, _ = ioutil.TempDir("", "golbat-test")

var option Options = Options{
	Dir:           testDir,
	MemTableSize:  256,
	maxBatchSize:  128,
	maxBatchCount: 10,
	comparator:    bytes.Compare,
}

func TestNewMemTable(t *testing.T) {
	id := 0
	// create a new memtable
	mt, err := NewMemTable(id, option)
	filePath := mt.wal.path
	require.NotNil(t, mt)
	require.Nil(t, err)
	require.True(t, mt.NewTable)
	require.FileExists(t, filePath)

	// create another memtable with same id
	mt2, err := NewMemTable(id, option)
	require.Nil(t, mt2)
	require.NotNil(t, err)

	// delete the memtable's wal file
	mt.DecrRef()
	require.NoFileExists(t, filePath)
}

func TestPutRecordsAndRestore(t *testing.T) {
	mt, err := NewMemTable(0, option)
	require.Nil(t, err)
	require.NotNil(t, mt)

	e := &entry{
		key:   []byte("testkey"),
		value: []byte("golbat memtable test"),
		rtype: Value,
	}
	esz := 3 + len(e.key) + len(e.value)

	n := option.MemTableSize/esz + 1
	ev := EValue{
		Value: e.value,
		Meta:  e.rtype,
	}
	for i := 0; i < n; i++ {
		mt.Put(e.key, ev)
	}

	mt.SyncWAL()
	require.True(t, mt.IsFull())
	mt.wal.Close()

	info, _ := os.Stat(mt.wal.path)
	require.True(t, info.Size() >= int64(mt.wal.size))

	// open new memtable with same wal file
	mt2, err := OpenMemTable(0, os.O_RDWR, option)
	require.Nil(t, err)
	require.NotNil(t, mt2)
	defer mt2.DecrRef()

	// test value
	cnt := 0
	test := func(e2 *entry, _ valPtr) error {
		require.EqualValues(t, e.key, e2.key)
		require.EqualValues(t, e.value, e2.value)
		require.EqualValues(t, e.rtype, e2.rtype)
		cnt++

		return nil
	}

	mt2.wal.iterate(0, test)
	require.Equal(t, n, cnt)
}
