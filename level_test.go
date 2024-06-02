package equinox

import (
	"encoding/binary"
	"io/ioutil"
	"os"
	"sort"
	"testing"

	"equinox/internel"
	"github.com/stretchr/testify/require"
)

var levelOpts = Options{comparator: CompareKeys,
	NumLevelZeroTablesStall: 10,
	CompressionType:         NoCompression,
}

func mockTable(id, size int, dir string) *Table {
	t := Table{opt: &levelOpts}
	t.id = uint64(id)
	t.tableSize = uint32(size)
	t.hasBloomFilter = false
	t.smallest = make([]byte, 8)
	binary.BigEndian.PutUint64(t.smallest, uint64(t.id))
	t.biggest = make([]byte, 8)
	binary.BigEndian.PutUint64(t.biggest, uint64(t.tableSize)+uint64(t.id))
	t.index = &tableIndex{}

	filename := NewTableFileName(t.id, dir)
	mf, err := internel.OpenMmapFile(filename, os.O_CREATE|os.O_RDWR|os.O_EXCL, 16)
	if err != nil {
		panic(err)
	}

	t.MMapFile = mf

	t.IncrRef()
	return &t
}

func getLevelTestTables(from, to int, dir string, reverse bool) ([]*Table, int) {
	tables := make([]*Table, 0)
	totalSize := 0

	if !reverse {
		for i := from; i <= to; i++ {
			tables = append(tables, mockTable(i, i*100, dir))
			totalSize += i * 100
		}
	} else {
		for i := to; i >= from; i-- {
			tables = append(tables, mockTable(i, i*100, dir))
			totalSize += i * 100
		}
	}

	return tables, totalSize
}

func TestLevelNotZero(t *testing.T) {
	dir, err := ioutil.TempDir("", "golbat-test")
	require.NoError(t, err)
	defer removeDir(dir)

	level := NewLevel(&levelOpts, 1)
	defer level.Close()

	totalSize := 0
	initTables, initSize := getLevelTestTables(1, 5, dir, false)
	totalSize += initSize

	// init
	level.Init(initTables)
	require.Equal(t, 5, level.NumTables())
	require.Equal(t, int64(totalSize), level.totalSize)
	require.True(t, sort.SliceIsSorted(level.tables, func(i, j int) bool {
		return levelOpts.comparator(level.tables[i].Smallest(), level.tables[j].Smallest()) < 0
	}))

	// add
	addTables, addSize := getLevelTestTables(6, 10, dir, true)
	for _, table := range addTables {
		require.True(t, level.Add(table))
	}
	totalSize += addSize

	level.Sort()
	require.Equal(t, 10, level.NumTables())
	require.Equal(t, int64(totalSize), level.totalSize)
	require.True(t, sort.SliceIsSorted(level.tables, func(i, j int) bool {
		return levelOpts.comparator(level.tables[i].Smallest(), level.tables[j].Smallest()) < 0
	}))

	// delete
	err = level.Delete(initTables)
	totalSize -= initSize
	require.NoError(t, err)
	require.Equal(t, 5, level.NumTables())
	require.Equal(t, int64(totalSize), level.totalSize)
	require.True(t, sort.SliceIsSorted(level.tables, func(i, j int) bool {
		return levelOpts.comparator(level.tables[i].Smallest(), level.tables[j].Smallest()) < 0
	}))

	// replace
	replaceTables, rSize := getLevelTestTables(11, 15, dir, true)
	totalSize += rSize

	err = level.Replace(addTables, replaceTables)
	require.NoError(t, err)
	require.Equal(t, 5, level.NumTables())
	require.Equal(t, int64(totalSize-addSize), level.totalSize)
	require.True(t, sort.SliceIsSorted(level.tables, func(i, j int) bool {
		return levelOpts.comparator(level.tables[i].Smallest(), level.tables[j].Smallest()) < 0
	}))

}

func TestLevelZero(t *testing.T) {
	dir, err := ioutil.TempDir("", "golbat-test")
	require.NoError(t, err)
	defer removeDir(dir)

	level := NewLevel(&levelOpts, 0)
	defer level.Close()

	totalSize := 0
	initTables, initSize := getLevelTestTables(1, 10, dir, false)
	totalSize += initSize
	// init
	level.Init(initTables)
	require.Equal(t, 10, level.NumTables())
	require.Equal(t, int64(totalSize), level.totalSize)
	require.True(t, sort.SliceIsSorted(level.tables, func(i, j int) bool {
		return level.tables[i].ID() < level.tables[j].ID()
	}))

	lastTable := mockTable(11, 11*100, dir)
	require.False(t, level.Add(lastTable))

	// replace
	err = level.Replace(initTables[9:10], []*Table{lastTable})
	require.Error(t, err)
	require.Contains(t, err.Error(), "level0 unsuppport Replace")
	lastTable.DecrRef()

	// delete
	err = level.Delete(initTables[:5])
	for i := 0; i < 5; i++ {
		totalSize -= int(initTables[i].Size())
	}
	require.NoError(t, err)
	require.Equal(t, 5, level.NumTables())
	require.Equal(t, int64(totalSize), level.totalSize)
	require.True(t, sort.SliceIsSorted(level.tables, func(i, j int) bool {
		return level.tables[i].ID() < level.tables[j].ID()
	}))

	// add
	addTables, addSize := getLevelTestTables(11, 15, dir, false)
	totalSize += addSize
	for _, table := range addTables {
		require.True(t, level.Add(table))
	}

	level.Sort()
	require.Equal(t, 10, level.NumTables())
	require.Equal(t, int64(totalSize), level.totalSize)
	require.True(t, sort.SliceIsSorted(level.tables, func(i, j int) bool {
		return level.tables[i].ID() < level.tables[j].ID()
	}))
}
