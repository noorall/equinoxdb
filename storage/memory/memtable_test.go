package memory

import (
	"equinox/storage/config"
	"equinox/storage/types"
	"github.com/stretchr/testify/require"
	"os"
	"testing"
	"time"
)

func DefaultOptions() config.Option {
	testDir, _ := os.MkdirTemp("", "equinox")
	option := config.NewOption()
	option.Dir = testDir
	return option
}

func TestNewMemTable(t *testing.T) {
	id := 0
	option := DefaultOptions()
	// create a new memtable
	mt, err := NewMemTable(id, option)
	require.Nil(t, err)
	require.True(t, mt.NewTable)

	// create another memtable with same id
	mt2, err := NewMemTable(id, option)
	require.Nil(t, mt2)
	require.NotNil(t, err)

	// delete the memtable's wal file
	mt.DecrRef()
}

func TestPutRecordsAndRestore(t *testing.T) {
	option := DefaultOptions()
	mt, err := NewMemTable(0, option)
	require.Nil(t, err)
	require.NotNil(t, mt)

	values := make(map[string][]types.Value)
	tt := time.Now().UnixNano()
	value := "h"
	values["test"] = append(values["test"], types.NewStringValue(tt, value))

	err = mt.WriteMulti(values)
	require.Nil(t, err)

	mt.SyncWAL()

	// open new memtable with same wal file
	mt2, err := OpenMemTable(0, os.O_RDWR, option)
	require.Nil(t, err)
	require.NotNil(t, mt2.Cache.Get([]byte("test")))
	require.NotNil(t, mt2)
	mt2.wal.Delete()
}
