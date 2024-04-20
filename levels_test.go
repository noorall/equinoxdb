package equinox

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"equinox/internel"
	"github.com/stretchr/testify/require"
)

type keyValVersion struct {
	key     string
	val     string
	version uint64
	meta    byte
}

var testOption = Options{
	Logger:                  internel.DefaultLogger(internel.DEBUG),
	NumLevelZeroTablesStall: 10,
	MaxLevels:               10,
	BaseLevelSize:           2 << 20,
	BaseTableSize:           2 << 20,
	MemTableSize:            1 << 20,
	LevelSizeMultiplier:     10,
	NumLevelZeroTables:      10,
	NumCompactors:           1,
	ValueLogFileSize:        2 << 20,
	ValueLogMaxEntries:      1000,
	ValueThreshold:          32 << 10,
	comparator:              CompareKeys,
}

func getLevels(numCompactors int) *levels {
	dir, err := ioutil.TempDir("", "golbat-test")
	if err != nil {
		panic(err)
	}

	vlogDir := filepath.Join(dir, "vlog")
	if err := os.Mkdir(vlogDir, 0766); err != nil {
		panic(err)
	}

	opts := testOption
	opts.Dir = dir
	opts.NumCompactors = numCompactors
	opts.ValueLogDir = vlogDir

	mff, mf, err := OpenManifestFile(opts.Dir)
	if err != nil {
		panic(err)
	}

	vlog, err := OpenValueLog(opts)
	if err != nil {
		mff.Close()
		panic(err)
	}

	snapshots := newSnapshotList()
	ls, err := NewLevels(&opts, &mf, vlog, mff, snapshots)
	if err != nil {
		vlog.Close()
		mff.Close()
		panic(err)
	}

	return ls
}

func createAndOpenTable(ls *levels, data []keyValVersion, level int) {
	opts := getTestTableOptions()
	builder := NewTableBuilder(opts)

	for _, item := range data {
		key := KeyWithVersion([]byte(item.key), item.version)
		value := EValue{Meta: item.meta, Value: []byte(item.val)}

		builder.Add(key, value, 0)
	}

	fname := NewTableFileName(ls.nextTableId(), ls.opts.Dir)
	table, err := CreateTable(fname, builder)
	if err != nil {
		panic(err)
	}

	if err := ls.manifest.AddManifestChange([]*manifestChange{
		newManifestChange(mcreate, table.ID(), uint64(level), table.CompressionType())}); err != nil {
		panic(err)
	}

	ls.ls[level].Lock()
	ls.ls[level].tables = append(ls.ls[level].tables, table)
	ls.ls[level].Unlock()
}

func getAllAndCheck(t *testing.T, ls *levels, expected []keyValVersion) {
	for _, kv := range expected {
		key := KeyWithVersion([]byte(kv.key), kv.version)
		value := EValue{Value: []byte(kv.val), Meta: kv.meta, version: kv.version}
		v, err := ls.GetValue(key, 0)
		require.NoError(t, err)
		require.Equal(t, value, v)
	}
}

func getAllAndCheckNotIn(t *testing.T, ls *levels, notIncludes []keyValVersion) {
	var emptyVal EValue
	for _, kv := range notIncludes {
		key := KeyWithVersion([]byte(kv.key), kv.version)
		v, err := ls.GetValue(key, 0)
		require.NoError(t, err)
		require.Equal(t, v, emptyVal)
	}
}

func TestLevelsGet(t *testing.T) {
	createLevel := func(ls *levels, level int, data [][]keyValVersion) {
		for _, v := range data {
			createAndOpenTable(ls, v, level)
		}
	}

	type testCase struct {
		name      string
		levelData map[int][][]keyValVersion
		expect    []keyValVersion
	}

	test := func(t *testing.T, tc testCase, ls *levels) {
		for level, data := range tc.levelData {
			createLevel(ls, level, data)
		}

		for _, item := range tc.expect {
			key := KeyWithVersion([]byte(item.key), item.version)
			v, err := ls.GetValue(key, 0)
			require.NoError(t, err)
			require.Equal(t, item.val, string(v.Value))
			require.True(t, v.version <= item.version)
		}
	}

	tcs := []testCase{
		{
			name: "One Version",
			levelData: map[int][][]keyValVersion{
				0: {
					{{"foo", "bar3", 3, 0}},
				},
				1: {
					{{"foo", "bar1", 1, 0}},
				},
			},
			expect: []keyValVersion{
				{"foo", "bar3", 3, 0},
				{"foo", "bar1", 1, 0},
				{"foo", "bar3", 4, 0}, // ver 4 doesn't exist so we should get bar3.
				{"foo", "bar1", 2, 0}, // ver 2 doesn't exist so we should get bar1.
			},
		},
		{
			name: "More Versions",
			levelData: map[int][][]keyValVersion{
				0: {
					{{"foo", "bar5", 5, 0}},
					{{"foo", "bar7", 7, 0}},
				},
				1: {
					{{"foo", "bar3", 3, 0}, {"foo", "bar1", 1, 0}},
				},
			},
			expect: []keyValVersion{
				{"foo", "bar3", 3, 0},
				{"foo", "bar1", 1, 0},
				{"foo", "bar7", 7, 0},
				{"foo", "bar5", 5, 0},
				{"foo", "bar5", 6, 0}, // ver 6 doesn't exist so we should get bar5.
				{"foo", "bar7", 9, 0}, // ver 9 doesn't exist so we should get bar7.
				{"foo", "bar1", 2, 0}, // ver 2 doesn't exist so we should get bar1.
			},
		},
		{
			name: "More levels",
			levelData: map[int][][]keyValVersion{
				0: {
					{{"fooo", "bar1", 1, 0}},
				},
				1: {
					{{"foo", "bar7", 7, 0}, {"foo", "bar5", 5, 0}},
				},
				2: {
					{{"foo", "bar3", 3, 0}},
				},
			},
			expect: []keyValVersion{
				{"fooo", "bar1", 1, 0},
				{"foo", "bar7", 7, 0},
				{"foo", "bar5", 5, 0},
				{"fooo", "bar1", 2, 0}, // ver 2 doesn't exist so we should get bar1.
				{"foo", "bar5", 6, 0},  // ver 6 doesn't exist so we should get bar5.
				{"foo", "bar7", 9, 0},  // ver 9 doesn't exist so we should get bar7.
				{"foo", "bar3", 4, 0},  // ver 4 doesn't exist so we should get bar3.
				{"foo", "", 2, 0},      // ver 2 doesn't exist so we should get empty.
			},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			ls := getLevels(0)
			defer ls.Close()
			defer removeDir(ls.opts.Dir)

			test(t, tc, ls)
		})
	}
}

func TestCompaction(t *testing.T) {
	t.Run("level 0 to level 1", func(t *testing.T) {
		ls := getLevels(0)
		defer ls.Close()
		defer removeDir(ls.opts.Dir)

		// CAUTION: for tables from level 0, the newer table, the higher version
		l0 := []keyValVersion{{"foo", "bar", 2, 0}, {"fooz", "baz", 1, 0}}
		l01 := []keyValVersion{{"foo", "bar", 3, 0}}
		l1 := []keyValVersion{{"foo", "bar", 1, 0}}

		createAndOpenTable(ls, l0, 0)
		createAndOpenTable(ls, l01, 0)

		createAndOpenTable(ls, l1, 1)

		ls.snapshots.New(4)
		getAllAndCheck(t, ls, []keyValVersion{
			{"foo", "bar", 3, 0}, {"foo", "bar", 2, 0},
			{"foo", "bar", 1, 0}, {"fooz", "baz", 1, 0},
		})

		ci := compactInfo{
			curr:       ls.ls[0],
			next:       ls.ls[1],
			fromTables: ls.ls[0].tables,
			toTables:   ls.ls[1].tables,
			cp:         compactionPriority{dls: ls.getDynamicLevelSize()},
		}

		ci.cp.dls.baseLevel = 1
		err := ls.compact(-1, 0, ci)
		require.NoError(t, err)
		getAllAndCheck(t, ls, []keyValVersion{{"foo", "bar", 3, 0}, {"fooz", "baz", 1, 0}})
	})

	t.Run("level 0 to level 1 with duplicates", func(t *testing.T) {
		ls := getLevels(0)
		defer ls.Close()
		defer removeDir(ls.opts.Dir)

		l0 := []keyValVersion{{"foo", "barNew", 3, 0}, {"fooz", "baz", 1, 0}}
		l01 := []keyValVersion{{"foo", "bar", 4, 0}}
		l1 := []keyValVersion{{"foo", "bar", 3, 0}}

		// Level 0 has table l0 and l01.
		createAndOpenTable(ls, l0, 0)
		createAndOpenTable(ls, l01, 0)
		// Level 1 has table l1.
		createAndOpenTable(ls, l1, 1)

		ls.snapshots.New(5)
		getAllAndCheck(t, ls, []keyValVersion{
			{"foo", "bar", 4, 0}, {"foo", "barNew", 3, 0},
			{"fooz", "baz", 1, 0},
		})

		ci := compactInfo{
			curr:       ls.ls[0],
			next:       ls.ls[1],
			fromTables: ls.ls[0].tables,
			toTables:   ls.ls[1].tables,
			cp:         compactionPriority{dls: ls.getDynamicLevelSize()},
		}

		ci.cp.dls.baseLevel = 1
		err := ls.compact(-1, 0, ci)
		require.NoError(t, err)
		getAllAndCheck(t, ls, []keyValVersion{{"foo", "bar", 4, 0}, {"fooz", "baz", 1, 0}})
	})

	t.Run("level 0 to level 1 with lower overlap", func(t *testing.T) {
		ls := getLevels(0)
		defer ls.Close()
		defer removeDir(ls.opts.Dir)

		// CAUTION: for tables from level 0, the newer table, the higher version
		l0 := []keyValVersion{{"foo", "bar", 3, 0}, {"fooz", "baz", 1, 0}}
		l01 := []keyValVersion{{"foo", "bar", 4, 0}}
		l1 := []keyValVersion{{"foo", "bar", 2, 0}}
		l2 := []keyValVersion{{"foo", "bar", 1, 0}}
		// Level 0 has table l0 and l01.
		createAndOpenTable(ls, l0, 0)
		createAndOpenTable(ls, l01, 0)
		// Level 1 has table l1.
		createAndOpenTable(ls, l1, 1)
		// Level 2 has table l2.
		createAndOpenTable(ls, l2, 2)

		ls.snapshots.New(5)

		getAllAndCheck(t, ls, []keyValVersion{
			{"foo", "bar", 4, 0}, {"foo", "bar", 3, 0}, {"foo", "bar", 2, 0},
			{"foo", "bar", 1, 0}, {"fooz", "baz", 1, 0},
		})

		ci := compactInfo{
			curr:       ls.ls[0],
			next:       ls.ls[1],
			fromTables: ls.ls[0].tables,
			toTables:   ls.ls[1].tables,
			cp:         compactionPriority{dls: ls.getDynamicLevelSize()},
		}

		ci.cp.dls.baseLevel = 1
		err := ls.compact(-1, 0, ci)
		require.NoError(t, err)
		getAllAndCheck(t, ls, []keyValVersion{
			{"foo", "bar", 4, 0}, {"fooz", "baz", 1, 0}, {"fooz", "baz", 1, 0},
		})
	})

	t.Run("level 1 to level 2", func(t *testing.T) {
		ls := getLevels(0)
		defer ls.Close()
		defer removeDir(ls.opts.Dir)

		l1 := []keyValVersion{{"foo", "bar", 3, 0}, {"fooz", "baz", 1, 0}}
		l2 := []keyValVersion{{"foo", "bar", 2, 0}}
		createAndOpenTable(ls, l1, 1)
		createAndOpenTable(ls, l2, 2)

		ls.snapshots.New(4)
		getAllAndCheck(t, ls, []keyValVersion{
			{"foo", "bar", 3, 0}, {"foo", "bar", 2, 0}, {"fooz", "baz", 1, 0},
		})

		ci := compactInfo{
			curr:       ls.ls[1],
			next:       ls.ls[2],
			fromTables: ls.ls[1].tables,
			toTables:   ls.ls[2].tables,
			cp:         compactionPriority{dls: ls.getDynamicLevelSize()},
		}

		ci.cp.dls.baseLevel = 2
		err := ls.compact(-1, 1, ci)
		require.NoError(t, err)
		getAllAndCheck(t, ls, []keyValVersion{
			{"foo", "bar", 3, 0}, {"fooz", "baz", 1, 0},
		})
	})

	t.Run("level 1 to level 2 with delete and overlap", func(t *testing.T) {
		ls := getLevels(0)
		defer ls.Close()
		defer removeDir(ls.opts.Dir)

		l1 := []keyValVersion{{"foo", "bar", 3, Delete}, {"fooz", "baz", 1, Delete}}
		l2 := []keyValVersion{{"foo", "bar", 2, 0}}
		l3 := []keyValVersion{{"foo", "bar", 1, 0}}
		createAndOpenTable(ls, l1, 1)
		createAndOpenTable(ls, l2, 2)
		createAndOpenTable(ls, l3, 3)

		ls.snapshots.New(4)
		getAllAndCheck(t, ls, []keyValVersion{
			{"foo", "bar", 3, Delete},
			{"foo", "bar", 2, 0},
			{"foo", "bar", 1, 0},
			{"fooz", "baz", 1, Delete},
		})

		ci := compactInfo{
			curr:       ls.ls[1],
			next:       ls.ls[2],
			fromTables: ls.ls[1].tables,
			toTables:   ls.ls[2].tables,
			cp:         compactionPriority{dls: ls.getDynamicLevelSize()},
		}

		ci.cp.dls.baseLevel = 2
		err := ls.compact(-1, 1, ci)
		require.NoError(t, err)
		getAllAndCheck(t, ls, []keyValVersion{
			{"foo", "bar", 3, Delete},
			{"foo", "bar", 1, 0},
			{"fooz", "baz", 1, Delete},
		})

		// compact again, this can gc deleted record.
		ci = compactInfo{
			curr:       ls.ls[2],
			next:       ls.ls[3],
			fromTables: ls.ls[2].tables,
			toTables:   ls.ls[3].tables,
			cp:         compactionPriority{dls: ls.getDynamicLevelSize()},
		}

		ci.cp.dls.baseLevel = 3
		err = ls.compact(-1, 2, ci)
		require.NoError(t, err)
		getAllAndCheckNotIn(t, ls, []keyValVersion{
			{"foo", "bar", 3, Delete},
			{"foo", "bar", 2, 0},
			{"foo", "bar", 1, 0},
			{"fooz", "baz", 1, Delete},
		})
	})

	t.Run("level 1 to level 2 with delete and bottom overlap", func(t *testing.T) {
		ls := getLevels(0)
		defer ls.Close()
		defer removeDir(ls.opts.Dir)

		l1 := []keyValVersion{{"foo", "bar", 3, Delete}}
		l2 := []keyValVersion{{"foo", "bar", 2, 0}, {"fooz", "baz", 2, Delete}}
		l3 := []keyValVersion{{"fooz", "baz", 1, 0}}
		createAndOpenTable(ls, l1, 1)
		createAndOpenTable(ls, l2, 2)
		createAndOpenTable(ls, l3, 3)

		ls.snapshots.New(4)

		getAllAndCheck(t, ls, []keyValVersion{
			{"foo", "bar", 3, Delete},
			{"foo", "bar", 2, 0},
			{"fooz", "baz", 2, Delete},
			{"fooz", "baz", 1, 0},
		})

		ci := compactInfo{
			curr:       ls.ls[1],
			next:       ls.ls[2],
			fromTables: ls.ls[1].tables,
			toTables:   ls.ls[2].tables,
			cp:         compactionPriority{dls: ls.getDynamicLevelSize()},
		}

		ci.cp.dls.baseLevel = 2
		err := ls.compact(-1, 1, ci)
		require.NoError(t, err)

		// the tables at L1 doesn't overlap L3, but the tables at L2
		// does, delete keys should not be removed.
		getAllAndCheck(t, ls, []keyValVersion{
			{"foo", "bar", 3, Delete},
			{"fooz", "baz", 2, Delete},
			{"fooz", "baz", 1, 0},
		})

		// compact again, this can gc deleted record.
		ci = compactInfo{
			curr:       ls.ls[2],
			next:       ls.ls[3],
			fromTables: ls.ls[2].tables,
			toTables:   ls.ls[3].tables,
			cp:         compactionPriority{dls: ls.getDynamicLevelSize()},
		}

		ci.cp.dls.baseLevel = 3
		err = ls.compact(-1, 2, ci)
		require.NoError(t, err)
		getAllAndCheckNotIn(t, ls, []keyValVersion{
			{"foo", "bar", 3, Delete},
			{"foo", "bar", 2, 0},
			{"fooz", "baz", 2, Delete},
			{"fooz", "baz", 1, 0},
		})
	})

	t.Run("level 1 to level 2 with delete and without overlap", func(t *testing.T) {
		ls := getLevels(0)
		defer ls.Close()
		defer removeDir(ls.opts.Dir)

		l1 := []keyValVersion{{"foo", "bar", 3, Delete}, {"fooz", "baz", 1, Delete}}
		l2 := []keyValVersion{{"fooo", "barr", 2, 0}}
		createAndOpenTable(ls, l1, 1)
		createAndOpenTable(ls, l2, 2)

		ls.snapshots.New(4)
		getAllAndCheck(t, ls, []keyValVersion{
			{"foo", "bar", 3, Delete}, {"fooo", "barr", 2, 0}, {"fooz", "baz", 1, Delete},
		})

		ci := compactInfo{
			curr:       ls.ls[1],
			next:       ls.ls[2],
			fromTables: ls.ls[1].tables,
			toTables:   ls.ls[2].tables,
			cp:         compactionPriority{dls: ls.getDynamicLevelSize()},
		}

		ci.cp.dls.baseLevel = 2
		err := ls.compact(-1, 1, ci)
		require.NoError(t, err)
		getAllAndCheck(t, ls, []keyValVersion{
			{"fooo", "barr", 2, 0},
		})
	})

	t.Run("level 1 to level 2 with delete and splits", func(t *testing.T) {
		ls := getLevels(0)
		defer ls.Close()
		defer removeDir(ls.opts.Dir)

		l1 := []keyValVersion{{"C", "bar", 3, Delete}}
		l21 := []keyValVersion{{"A", "bar", 2, 0}}
		l22 := []keyValVersion{{"B", "bar", 2, 0}}
		l23 := []keyValVersion{{"C", "bar", 2, 0}}
		l24 := []keyValVersion{{"D", "bar", 2, 0}}
		l3 := []keyValVersion{{"fooz", "baz", 1, 0}}
		createAndOpenTable(ls, l1, 1)
		createAndOpenTable(ls, l21, 2)
		createAndOpenTable(ls, l22, 2)
		createAndOpenTable(ls, l23, 2)
		createAndOpenTable(ls, l24, 2)
		createAndOpenTable(ls, l3, 3)

		ls.snapshots.New(4)
		getAllAndCheck(t, ls, []keyValVersion{
			{"A", "bar", 2, 0},
			{"B", "bar", 2, 0},
			{"C", "bar", 3, Delete},
			{"C", "bar", 2, 0},
			{"D", "bar", 2, 0},
			{"fooz", "baz", 1, 0},
		})

		ci := compactInfo{
			curr:       ls.ls[1],
			next:       ls.ls[2],
			fromTables: ls.ls[1].tables,
			toTables:   ls.ls[2].tables,
			cp:         compactionPriority{dls: ls.getDynamicLevelSize()},
		}

		ci.cp.dls.baseLevel = 2
		err := ls.compact(-1, 1, ci)
		require.NoError(t, err)
		getAllAndCheck(t, ls, []keyValVersion{
			{"A", "bar", 2, 0},
			{"B", "bar", 2, 0},
			{"D", "bar", 2, 0},
			{"fooz", "baz", 1, 0},
		})
	})
}

func TestDiscardVersion(t *testing.T) {
	t.Run("all keys above discardVersion", func(t *testing.T) {
		ls := getLevels(0)
		defer ls.Close()
		defer removeDir(ls.opts.Dir)

		// CAUTION: for tables from level 0, the newer table, the higher version
		l0 := []keyValVersion{{"foo", "bar", 3, 0}, {"fooz", "baz", 3, 0}}
		l01 := []keyValVersion{{"foo", "bar", 4, 0}}
		l1 := []keyValVersion{{"foo", "bar", 2, 0}}

		createAndOpenTable(ls, l0, 0)
		createAndOpenTable(ls, l01, 0)
		createAndOpenTable(ls, l1, 1)

		ls.snapshots.New(1)
		getAllAndCheck(t, ls, []keyValVersion{
			{"foo", "bar", 4, 0}, {"foo", "bar", 3, 0},
			{"foo", "bar", 2, 0}, {"fooz", "baz", 3, 0},
		})

		ci := compactInfo{
			curr:       ls.ls[0],
			next:       ls.ls[1],
			fromTables: ls.ls[0].tables,
			toTables:   ls.ls[1].tables,
			cp:         compactionPriority{dls: ls.getDynamicLevelSize()},
		}

		ci.cp.dls.baseLevel = 1
		err := ls.compact(-1, 0, ci)
		require.NoError(t, err)
		getAllAndCheck(t, ls, []keyValVersion{
			{"foo", "bar", 4, 0}, {"foo", "bar", 3, 0},
			{"foo", "bar", 2, 0}, {"fooz", "baz", 3, 0},
		})
	})

	t.Run("some keys above discardVersion", func(t *testing.T) {
		ls := getLevels(0)
		defer ls.Close()
		defer removeDir(ls.opts.Dir)

		l0 := []keyValVersion{
			{"foo", "bar", 4, 0}, {"foo", "bar", 3, 0},
			{"foo", "bar", 2, 0}, {"fooz", "baz", 2, 0},
		}
		l1 := []keyValVersion{{"foo", "bbb", 1, 0}}

		createAndOpenTable(ls, l0, 0)
		createAndOpenTable(ls, l1, 1)

		ls.snapshots.New(3)
		getAllAndCheck(t, ls, []keyValVersion{
			{"foo", "bar", 4, 0}, {"foo", "bar", 3, 0}, {"foo", "bar", 2, 0},
			{"foo", "bbb", 1, 0}, {"fooz", "baz", 2, 0},
		})

		ci := compactInfo{
			curr:       ls.ls[0],
			next:       ls.ls[1],
			fromTables: ls.ls[0].tables,
			toTables:   ls.ls[1].tables,
			cp:         compactionPriority{dls: ls.getDynamicLevelSize()},
		}

		ci.cp.dls.baseLevel = 1
		err := ls.compact(-1, 0, ci)
		require.NoError(t, err)
		getAllAndCheck(t, ls, []keyValVersion{
			{"foo", "bar", 4, 0}, {"foo", "bar", 3, 0}, {"fooz", "baz", 2, 0},
		})
	})

	t.Run("all keys below discardVersion", func(t *testing.T) {
		ls := getLevels(0)
		defer ls.Close()
		defer removeDir(ls.opts.Dir)

		// CAUTION: for tables from level 0, the newer table, the higher version
		l0 := []keyValVersion{{"foo", "bar", 3, 0}, {"fooz", "baz", 3, 0}}
		l01 := []keyValVersion{{"foo", "bar", 4, 0}}
		l1 := []keyValVersion{{"foo", "bar", 2, 0}}

		createAndOpenTable(ls, l0, 0)
		createAndOpenTable(ls, l01, 0)
		createAndOpenTable(ls, l1, 1)

		ls.snapshots.New(10)
		getAllAndCheck(t, ls, []keyValVersion{
			{"foo", "bar", 4, 0}, {"foo", "bar", 3, 0},
			{"foo", "bar", 2, 0}, {"fooz", "baz", 3, 0},
		})

		ci := compactInfo{
			curr:       ls.ls[0],
			next:       ls.ls[1],
			fromTables: ls.ls[0].tables,
			toTables:   ls.ls[1].tables,
			cp:         compactionPriority{dls: ls.getDynamicLevelSize()},
		}

		ci.cp.dls.baseLevel = 1
		err := ls.compact(-1, 0, ci)
		require.NoError(t, err)
		getAllAndCheck(t, ls, []keyValVersion{
			{"foo", "bar", 4, 0}, {"fooz", "baz", 3, 0},
		})
	})
}
