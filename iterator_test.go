package equinox

import (
	"bytes"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

func createTestDBImpl() (*DBImpl, func()) {
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
	opts.NumCompactors = 0
	opts.ValueLogDir = vlogDir

	vlog, err := OpenValueLog(opts)
	if err != nil {
		panic(err)
	}

	db := &DBImpl{}
	db.vlog = vlog

	return db, func() {
		db.vlog.Close()
		removeDir(dir)
	}
}

func createIterators(data []keyValVersion) Iterator {
	ti := &testIterator{valid: true}
	for _, d := range data {
		ti.data = append(ti.data, &testIteratorItem{
			key:   KeyWithVersion([]byte(d.key), d.version),
			value: []byte(d.val),
			rtype: d.meta})
	}

	ti.sort()
	return ti
}

func iterateAndCheck(t *testing.T, iter *DBIterator, expected []keyValVersion, reverse bool) {
	require.True(t, iter.Valid())
	defer iter.Close()

	start := func(iter Iterator) {
		if reverse {
			iter.SeekToLast()
		} else {
			iter.SeekToFirst()
		}
	}

	iterate := func(iter Iterator) {
		if reverse {
			iter.Prev()
		} else {
			iter.Next()
		}
	}

	var count int
	for start(iter); iter.Valid(); iterate(iter) {
		e := expected[count]
		v := iter.Value()
		require.Equal(t, e.key, string(iter.Key()))
		require.Equal(t, e.val, string(v.Value))
		require.Equal(t, e.meta, v.Meta)
		require.Equal(t, e.version, v.version)

		count++
	}

	require.Equal(t, len(expected), count)
}

func TestForwardIterate(t *testing.T) {
	db, closed := createTestDBImpl()
	defer closed()

	t.Run("without delete", func(t *testing.T) {
		data := []keyValVersion{
			{"foo", "bar1", 1, Value},
			{"foo", "bar2", 2, Value},
			{"foo", "bar4", 4, Value},
			{"fooa", "bar3", 3, Value},
			{"foob", "bar1", 1, Value},
		}

		iter := NewDBIterator(db, &ReadOptions{}, createIterators(data), 4)

		expected := []keyValVersion{
			{"foo", "bar4", 4, Value},
			{"fooa", "bar3", 3, Value},
			{"foob", "bar1", 1, Value},
		}

		iterateAndCheck(t, iter, expected, false)
	})

	t.Run("all version without delete", func(t *testing.T) {
		data := []keyValVersion{
			{"foo", "bar1", 1, Value},
			{"foo", "bar2", 2, Value},
			{"foo", "bar4", 4, Value},
			{"fooa", "bar3", 3, Value},
			{"foob", "bar1", 1, Value},
		}

		iter := NewDBIterator(db, &ReadOptions{AllVersion: true}, createIterators(data), 4)

		expected := []keyValVersion{
			{"foo", "bar4", 4, Value},
			{"foo", "bar2", 2, Value},
			{"foo", "bar1", 1, Value},
			{"fooa", "bar3", 3, Value},
			{"foob", "bar1", 1, Value},
		}

		iterateAndCheck(t, iter, expected, false)
	})

	t.Run("without delete but limit version", func(t *testing.T) {
		data := []keyValVersion{
			{"foo", "bar1", 1, Value},
			{"foo", "bar2", 2, Value},
			{"foo", "bar4", 4, Value},
			{"foo", "bar5", 5, Value},
			{"fooa", "bar3", 3, Value},
			{"foob", "bar1", 1, Value},
			{"fooc", "bar5", 5, Value},
		}

		iter := NewDBIterator(db, &ReadOptions{
			Snapshot: &Snapshot{version: 4},
		}, createIterators(data), 5)

		expected := []keyValVersion{
			{"foo", "bar4", 4, Value},
			{"fooa", "bar3", 3, Value},
			{"foob", "bar1", 1, Value},
		}

		iterateAndCheck(t, iter, expected, false)
	})

	t.Run("all version without delete but limit version", func(t *testing.T) {
		data := []keyValVersion{
			{"foo", "bar1", 1, Value},
			{"foo", "bar2", 2, Value},
			{"foo", "bar4", 4, Value},
			{"foo", "bar5", 5, Value},
			{"fooa", "bar3", 3, Value},
			{"foob", "bar1", 1, Value},
			{"fooc", "bar5", 5, Value},
		}

		iter := NewDBIterator(db, &ReadOptions{
			Snapshot:   &Snapshot{version: 4},
			AllVersion: true,
		}, createIterators(data), 5)

		expected := []keyValVersion{
			{"foo", "bar4", 4, Value},
			{"foo", "bar2", 2, Value},
			{"foo", "bar1", 1, Value},
			{"fooa", "bar3", 3, Value},
			{"foob", "bar1", 1, Value},
		}

		iterateAndCheck(t, iter, expected, false)
	})

	t.Run("with delete", func(t *testing.T) {
		data := []keyValVersion{
			{"foo", "bar1", 1, Value},
			{"foo", "bar2", 2, Value},
			{"foo", "bar4", 4, Delete},
			{"fooa", "bar3", 3, Delete},
			{"fooa", "bar4", 4, Value},
			{"foob", "bar1", 1, Delete},
		}

		iter := NewDBIterator(db, &ReadOptions{}, createIterators(data), 4)

		expected := []keyValVersion{
			{"fooa", "bar4", 4, Value},
		}

		iterateAndCheck(t, iter, expected, false)
	})

	t.Run("all version with delete", func(t *testing.T) {
		data := []keyValVersion{
			{"foo", "bar1", 1, Value},
			{"foo", "bar2", 2, Value},
			{"foo", "bar4", 4, Delete},
			{"fooa", "bar3", 3, Delete},
			{"fooa", "bar4", 4, Value},
			{"foob", "bar1", 1, Delete},
		}

		iter := NewDBIterator(db, &ReadOptions{AllVersion: true}, createIterators(data), 4)

		expected := []keyValVersion{
			{"foo", "bar4", 4, Delete},
			{"foo", "bar2", 2, Value},
			{"foo", "bar1", 1, Value},
			{"fooa", "bar4", 4, Value},
			{"fooa", "bar3", 3, Delete},
			{"foob", "bar1", 1, Delete},
		}

		iterateAndCheck(t, iter, expected, false)
	})

	t.Run("with delete and limit version", func(t *testing.T) {
		data := []keyValVersion{
			{"foo", "bar1", 1, Value},
			{"foo", "bar2", 2, Value},
			{"foo", "bar4", 4, Delete},
			{"fooa", "bar3", 3, Delete},
			{"fooa", "bar4", 4, Value},
			{"foob", "bar1", 1, Delete},
		}

		iter := NewDBIterator(db, &ReadOptions{
			Snapshot: &Snapshot{version: 3},
		}, createIterators(data), 4)

		expected := []keyValVersion{
			{"foo", "bar2", 2, Value},
		}

		iterateAndCheck(t, iter, expected, false)
	})

	t.Run("all version with delete and limit version", func(t *testing.T) {
		data := []keyValVersion{
			{"foo", "bar1", 1, Value},
			{"foo", "bar2", 2, Value},
			{"foo", "bar4", 4, Delete},
			{"fooa", "bar3", 3, Delete},
			{"fooa", "bar4", 4, Value},
			{"foob", "bar1", 1, Delete},
		}

		iter := NewDBIterator(db, &ReadOptions{
			Snapshot:   &Snapshot{version: 3},
			AllVersion: true,
		}, createIterators(data), 4)

		expected := []keyValVersion{
			{"foo", "bar2", 2, Value},
			{"foo", "bar1", 1, Value},
			{"fooa", "bar3", 3, Delete},
			{"foob", "bar1", 1, Delete},
		}

		iterateAndCheck(t, iter, expected, false)
	})
}

func TestBackwardIterate(t *testing.T) {
	db, closed := createTestDBImpl()
	defer closed()

	t.Run("without delete", func(t *testing.T) {
		data := []keyValVersion{
			{"foo", "bar1", 1, Value},
			{"foo", "bar2", 2, Value},
			{"foo", "bar4", 4, Value},
			{"fooa", "bar3", 3, Value},
			{"foob", "bar1", 1, Value},
		}

		iter := NewDBIterator(db, &ReadOptions{}, createIterators(data), 4)

		expected := []keyValVersion{
			{"foob", "bar1", 1, Value},
			{"fooa", "bar3", 3, Value},
			{"foo", "bar4", 4, Value},
		}

		iterateAndCheck(t, iter, expected, true)
	})

	t.Run("all version without delete", func(t *testing.T) {
		data := []keyValVersion{
			{"foo", "bar1", 1, Value},
			{"foo", "bar2", 2, Value},
			{"foo", "bar4", 4, Value},
			{"fooa", "bar3", 3, Value},
			{"foob", "bar1", 1, Value},
		}

		iter := NewDBIterator(db, &ReadOptions{AllVersion: true}, createIterators(data), 4)

		expected := []keyValVersion{
			{"foob", "bar1", 1, Value},
			{"fooa", "bar3", 3, Value},
			{"foo", "bar1", 1, Value},
			{"foo", "bar2", 2, Value},
			{"foo", "bar4", 4, Value},
		}

		iterateAndCheck(t, iter, expected, true)
	})

	t.Run("without delete but limit version", func(t *testing.T) {
		data := []keyValVersion{
			{"foo", "bar1", 1, Value},
			{"foo", "bar2", 2, Value},
			{"foo", "bar4", 4, Value},
			{"foo", "bar5", 5, Value},
			{"fooa", "bar3", 3, Value},
			{"foob", "bar1", 1, Value},
			{"fooc", "bar5", 5, Value},
		}

		iter := NewDBIterator(db, &ReadOptions{
			Snapshot: &Snapshot{version: 4},
		}, createIterators(data), 5)

		expected := []keyValVersion{
			{"foob", "bar1", 1, Value},
			{"fooa", "bar3", 3, Value},
			{"foo", "bar4", 4, Value},
		}

		iterateAndCheck(t, iter, expected, true)
	})

	t.Run("all version without delete but limit version", func(t *testing.T) {
		data := []keyValVersion{
			{"foo", "bar1", 1, Value},
			{"foo", "bar2", 2, Value},
			{"foo", "bar4", 4, Value},
			{"foo", "bar5", 5, Value},
			{"fooa", "bar3", 3, Value},
			{"foob", "bar1", 1, Value},
			{"fooc", "bar5", 5, Value},
		}

		iter := NewDBIterator(db, &ReadOptions{
			Snapshot:   &Snapshot{version: 4},
			AllVersion: true,
		}, createIterators(data), 5)

		expected := []keyValVersion{
			{"foob", "bar1", 1, Value},
			{"fooa", "bar3", 3, Value},
			{"foo", "bar1", 1, Value},
			{"foo", "bar2", 2, Value},
			{"foo", "bar4", 4, Value},
		}

		iterateAndCheck(t, iter, expected, true)
	})

	t.Run("with delete", func(t *testing.T) {
		data := []keyValVersion{
			{"foo", "bar1", 1, Value},
			{"foo", "bar2", 2, Value},
			{"foo", "bar4", 4, Delete},
			{"fooa", "bar3", 3, Delete},
			{"fooa", "bar4", 4, Value},
			{"foob", "bar1", 1, Delete},
		}

		iter := NewDBIterator(db, &ReadOptions{}, createIterators(data), 4)

		expected := []keyValVersion{
			{"fooa", "bar4", 4, Value},
		}

		iterateAndCheck(t, iter, expected, true)
	})

	t.Run("all version with delete", func(t *testing.T) {
		data := []keyValVersion{
			{"foo", "bar1", 1, Value},
			{"foo", "bar2", 2, Value},
			{"foo", "bar4", 4, Delete},
			{"fooa", "bar3", 3, Delete},
			{"fooa", "bar4", 4, Value},
			{"foob", "bar1", 1, Delete},
		}

		iter := NewDBIterator(db, &ReadOptions{AllVersion: true}, createIterators(data), 4)

		expected := []keyValVersion{
			{"foob", "bar1", 1, Delete},
			{"fooa", "bar3", 3, Delete},
			{"fooa", "bar4", 4, Value},
			{"foo", "bar1", 1, Value},
			{"foo", "bar2", 2, Value},
			{"foo", "bar4", 4, Delete},
		}

		iterateAndCheck(t, iter, expected, true)
	})

	t.Run("with delete and limit version", func(t *testing.T) {
		data := []keyValVersion{
			{"foo", "bar1", 1, Value},
			{"foo", "bar2", 2, Value},
			{"foo", "bar4", 4, Delete},
			{"fooa", "bar3", 3, Delete},
			{"fooa", "bar4", 4, Value},
			{"foob", "bar1", 1, Delete},
		}

		iter := NewDBIterator(db, &ReadOptions{
			Snapshot: &Snapshot{version: 3},
		}, createIterators(data), 4)

		expected := []keyValVersion{
			{"foo", "bar2", 2, Value},
		}

		iterateAndCheck(t, iter, expected, true)
	})

	t.Run("all version with delete and limit version", func(t *testing.T) {
		data := []keyValVersion{
			{"foo", "bar1", 1, Value},
			{"foo", "bar2", 2, Value},
			{"foo", "bar4", 4, Delete},
			{"fooa", "bar3", 3, Delete},
			{"fooa", "bar4", 4, Value},
			{"foob", "bar1", 1, Delete},
		}

		iter := NewDBIterator(db, &ReadOptions{
			Snapshot:   &Snapshot{version: 3},
			AllVersion: true,
		}, createIterators(data), 4)

		expected := []keyValVersion{
			{"foob", "bar1", 1, Delete},
			{"fooa", "bar3", 3, Delete},
			{"foo", "bar1", 1, Value},
			{"foo", "bar2", 2, Value},
		}

		iterateAndCheck(t, iter, expected, true)
	})
}

func TestForwardAndBackwardIterate(t *testing.T) {
	db, closed := createTestDBImpl()
	defer closed()

	data := []keyValVersion{
		{"foo", "bar1", 1, Value},
		{"foo", "bar2", 2, Delete},
		{"foo", "bar4", 4, Value},
		{"fooa", "bar3", 3, Value},
		{"foob", "bar1", 1, Delete},
	}

	t.Run("recent version", func(t *testing.T) {
		iter := NewDBIterator(db, &ReadOptions{}, createIterators(data), 4)
		defer iter.Close()

		iter.SeekToFirst()
		require.True(t, iter.Valid())
		v := iter.Value()
		require.Equal(t, "foo", string(iter.Key()))
		require.Equal(t, "bar4", string(v.Value))
		require.Equal(t, uint64(4), v.version)
		require.Equal(t, Value, v.Meta)

		iter.Next()
		require.True(t, iter.Valid())
		v = iter.Value()
		require.Equal(t, "fooa", string(iter.Key()))
		require.Equal(t, "bar3", string(v.Value))
		require.Equal(t, uint64(3), v.version)
		require.Equal(t, Value, v.Meta)

		iter.Prev()
		require.True(t, iter.Valid())
		v = iter.Value()
		require.Equal(t, "foo", string(iter.Key()))
		require.Equal(t, "bar4", string(v.Value))
		require.Equal(t, uint64(4), v.version)
		require.Equal(t, Value, v.Meta)

		iter.Prev()
		require.False(t, iter.Valid())
	})

	t.Run("all version", func(t *testing.T) {
		iter := NewDBIterator(db, &ReadOptions{AllVersion: true}, createIterators(data), 4)
		defer iter.Close()

		iter.SeekToFirst()
		require.True(t, iter.Valid())
		v := iter.Value()
		require.Equal(t, "foo", string(iter.Key()))
		require.Equal(t, "bar4", string(v.Value))
		require.Equal(t, uint64(4), v.version)
		require.Equal(t, Value, v.Meta)

		iter.Next()
		require.True(t, iter.Valid())
		v = iter.Value()
		require.Equal(t, "foo", string(iter.Key()))
		require.Equal(t, "bar2", string(v.Value))
		require.Equal(t, uint64(2), v.version)
		require.Equal(t, Delete, v.Meta)

		iter.Prev()
		require.True(t, iter.Valid())
		v = iter.Value()
		require.Equal(t, "foo", string(iter.Key()))
		require.Equal(t, "bar4", string(v.Value))
		require.Equal(t, uint64(4), v.version)
		require.Equal(t, Value, v.Meta)

		iter.Prev()
		require.False(t, iter.Valid())
	})
}

func TestSeek(t *testing.T) {
	db, closed := createTestDBImpl()
	defer closed()

	t.Run("without delete", func(t *testing.T) {
		data := []keyValVersion{
			{"foo", "bar1", 1, Value},
			{"foo", "bar2", 2, Value},
			{"foo", "bar4", 4, Value},
			{"fooa", "bar3", 3, Value},
			{"foob", "bar1", 1, Value},
		}

		iter := NewDBIterator(db, &ReadOptions{}, createIterators(data), 4)
		defer iter.Close()

		skey := []byte("foo")
		iter.Seek(skey)
		require.True(t, iter.Valid())
		v := iter.Value()
		require.Equal(t, "foo", string(iter.Key()))
		require.Equal(t, "bar4", string(v.Value))
		require.Equal(t, uint64(4), v.version)
		require.Equal(t, Value, v.Meta)

		skey = []byte("foob")
		iter.Seek(skey)
		require.True(t, iter.Valid())
		v = iter.Value()
		require.Equal(t, "foob", string(iter.Key()))
		require.Equal(t, "bar1", string(v.Value))
		require.Equal(t, uint64(1), v.version)
		require.Equal(t, Value, v.Meta)

		iter.Next()
		require.False(t, iter.Valid())
	})

	t.Run("All version without delete", func(t *testing.T) {
		data := []keyValVersion{
			{"foo", "bar1", 1, Value},
			{"foo", "bar2", 2, Value},
			{"foo", "bar4", 4, Value},
			{"fooa", "bar3", 3, Value},
			{"foob", "bar1", 1, Value},
		}

		iter := NewDBIterator(db, &ReadOptions{AllVersion: true}, createIterators(data), 4)
		defer iter.Close()

		skey := []byte("foo")
		expected := []keyValVersion{
			{"foo", "bar4", 4, Value},
			{"foo", "bar2", 2, Value},
			{"foo", "bar1", 1, Value},
		}
		iter.Seek(skey)
		require.True(t, iter.Valid())
		var count int
		for ; iter.Valid(); iter.Next() {
			k := iter.Key()
			v := iter.Value()
			if !bytes.Equal(k, skey) {
				break
			}

			require.Equal(t, expected[count].key, string(k))
			require.Equal(t, expected[count].val, string(v.Value))
			require.Equal(t, expected[count].version, v.version)
			require.Equal(t, expected[count].meta, v.Meta)

			count++
		}
		require.True(t, iter.Valid())
		require.Equal(t, 3, count)

		skey = []byte("foob")
		iter.Seek(skey)
		require.True(t, iter.Valid())
		v := iter.Value()
		require.Equal(t, "foob", string(iter.Key()))
		require.Equal(t, "bar1", string(v.Value))
		require.Equal(t, uint64(1), v.version)
		require.Equal(t, Value, v.Meta)

		iter.Next()
		require.False(t, iter.Valid())
	})

	t.Run("without delete but version limit", func(t *testing.T) {
		data := []keyValVersion{
			{"foo", "bar1", 1, Value},
			{"foo", "bar2", 2, Value},
			{"foo", "bar4", 4, Value},
			{"fooa", "bar4", 4, Value},
			{"foob", "bar1", 1, Value},
		}

		iter := NewDBIterator(db, &ReadOptions{
			Snapshot: &Snapshot{version: 3},
		}, createIterators(data), 4)
		defer iter.Close()

		skey := []byte("foo")
		iter.Seek(skey)
		require.True(t, iter.Valid())
		v := iter.Value()
		require.Equal(t, "foo", string(iter.Key()))
		require.Equal(t, "bar2", string(v.Value))
		require.Equal(t, uint64(2), v.version)
		require.Equal(t, Value, v.Meta)

		skey = []byte("fooa")
		iter.Seek(skey)
		require.False(t, iter.Valid())
	})

	t.Run("all version without delete but version limit", func(t *testing.T) {
		data := []keyValVersion{
			{"foo", "bar1", 1, Value},
			{"foo", "bar2", 2, Value},
			{"foo", "bar4", 4, Value},
			{"fooa", "bar4", 4, Value},
			{"foob", "bar1", 1, Value},
		}

		iter := NewDBIterator(db, &ReadOptions{
			Snapshot:   &Snapshot{version: 3},
			AllVersion: true,
		}, createIterators(data), 4)
		defer iter.Close()

		skey := []byte("foo")
		expected := []keyValVersion{
			{"foo", "bar2", 2, Value},
			{"foo", "bar1", 1, Value},
		}
		iter.Seek(skey)
		require.True(t, iter.Valid())
		var count int
		for ; iter.Valid(); iter.Next() {
			k := iter.Key()
			v := iter.Value()
			if !bytes.Equal(k, skey) {
				break
			}

			require.Equal(t, expected[count].key, string(k))
			require.Equal(t, expected[count].val, string(v.Value))
			require.Equal(t, expected[count].version, v.version)
			require.Equal(t, expected[count].meta, v.Meta)

			count++
		}
		require.True(t, iter.Valid())
		require.Equal(t, 2, count)

		skey = []byte("fooa")
		iter.Seek(skey)
		require.False(t, iter.Valid())
	})

	t.Run("with delete", func(t *testing.T) {
		data := []keyValVersion{
			{"foo", "bar1", 1, Value},
			{"foo", "bar2", 2, Value},
			{"foo", "bar4", 4, Delete},
			{"fooa", "bar3", 3, Delete},
			{"fooa", "bar4", 4, Value},
			{"foob", "bar1", 1, Delete},
		}

		iter := NewDBIterator(db, &ReadOptions{}, createIterators(data), 4)
		defer iter.Close()

		skey := []byte("foo")
		iter.Seek(skey)
		require.True(t, iter.Valid())
		v := iter.Value()
		require.Equal(t, "fooa", string(iter.Key()))
		require.Equal(t, "bar4", string(v.Value))
		require.Equal(t, uint64(4), v.version)
		require.Equal(t, Value, v.Meta)

		skey = []byte("foob")
		iter.Seek(skey)
		require.False(t, iter.Valid())
	})

	t.Run("all version with delete", func(t *testing.T) {
		data := []keyValVersion{
			{"foo", "bar1", 1, Value},
			{"foo", "bar2", 2, Value},
			{"foo", "bar4", 4, Delete},
			{"fooa", "bar3", 3, Delete},
			{"fooa", "bar4", 4, Value},
			{"foob", "bar1", 1, Delete},
		}

		iter := NewDBIterator(db, &ReadOptions{AllVersion: true}, createIterators(data), 4)
		defer iter.Close()

		skey := []byte("foo")
		expected := []keyValVersion{
			{"foo", "bar4", 4, Delete},
			{"foo", "bar2", 2, Value},
			{"foo", "bar1", 1, Value},
		}
		iter.Seek(skey)
		require.True(t, iter.Valid())
		var count int
		for ; iter.Valid(); iter.Next() {
			k := iter.Key()
			v := iter.Value()
			if !bytes.Equal(k, skey) {
				break
			}

			require.Equal(t, expected[count].key, string(k))
			require.Equal(t, expected[count].val, string(v.Value))
			require.Equal(t, expected[count].version, v.version)
			require.Equal(t, expected[count].meta, v.Meta)

			count++
		}
		require.True(t, iter.Valid())
		require.Equal(t, 3, count)

		skey = []byte("foob")
		iter.Seek(skey)
		require.True(t, iter.Valid())
		v := iter.Value()
		require.Equal(t, "foob", string(iter.Key()))
		require.Equal(t, "bar1", string(v.Value))
		require.Equal(t, uint64(1), v.version)
		require.Equal(t, Delete, v.Meta)
	})

	t.Run("with delete and limit version", func(t *testing.T) {
		data := []keyValVersion{
			{"foo", "bar1", 1, Value},
			{"foo", "bar2", 2, Value},
			{"foo", "bar4", 4, Delete},
			{"fooa", "bar3", 3, Delete},
			{"fooa", "bar4", 4, Value},
			{"foob", "bar1", 1, Delete},
		}

		iter := NewDBIterator(db, &ReadOptions{
			Snapshot: &Snapshot{version: 3},
		}, createIterators(data), 4)
		defer iter.Close()

		skey := []byte("foo")
		iter.Seek(skey)
		require.True(t, iter.Valid())
		v := iter.Value()
		require.Equal(t, "foo", string(iter.Key()))
		require.Equal(t, "bar2", string(v.Value))
		require.Equal(t, uint64(2), v.version)
		require.Equal(t, Value, v.Meta)

		skey = []byte("fooa")
		iter.Seek(skey)
		require.False(t, iter.Valid())

		skey = []byte("foob")
		iter.Seek(skey)
		require.False(t, iter.Valid())
	})

	t.Run("all version with delete and limit version", func(t *testing.T) {
		data := []keyValVersion{
			{"foo", "bar1", 1, Value},
			{"foo", "bar2", 2, Value},
			{"foo", "bar4", 4, Delete},
			{"fooa", "bar3", 3, Delete},
			{"fooa", "bar4", 4, Value},
			{"foob", "bar1", 1, Delete},
		}

		iter := NewDBIterator(db, &ReadOptions{
			Snapshot:   &Snapshot{version: 3},
			AllVersion: true,
		}, createIterators(data), 4)
		defer iter.Close()

		skey := []byte("foo")
		expected := []keyValVersion{
			{"foo", "bar2", 2, Value},
			{"foo", "bar1", 1, Value},
		}
		iter.Seek(skey)
		require.True(t, iter.Valid())
		var count int
		for ; iter.Valid(); iter.Next() {
			k := iter.Key()
			v := iter.Value()
			if !bytes.Equal(k, skey) {
				break
			}

			require.Equal(t, expected[count].key, string(k))
			require.Equal(t, expected[count].val, string(v.Value))
			require.Equal(t, expected[count].version, v.version)
			require.Equal(t, expected[count].meta, v.Meta)

			count++
		}
		require.True(t, iter.Valid())
		require.Equal(t, count, 2)

		skey = []byte("fooa")
		iter.Seek(skey)
		require.True(t, iter.Valid())
		v := iter.Value()
		require.Equal(t, "fooa", string(iter.Key()))
		require.Equal(t, "bar3", string(v.Value))
		require.Equal(t, uint64(3), v.version)
		require.Equal(t, Delete, v.Meta)

		skey = []byte("foob")
		iter.Seek(skey)
		require.True(t, iter.Valid())
		v = iter.Value()
		require.Equal(t, "foob", string(iter.Key()))
		require.Equal(t, "bar1", string(v.Value))
		require.Equal(t, uint64(1), v.version)
		require.Equal(t, Delete, v.Meta)
	})
}

type testIteratorItem struct {
	key   []byte
	value []byte
	rtype byte
}

func (tii *testIteratorItem) compare(other *testIteratorItem) int {
	return CompareKeys(tii.key, other.key)
}

type testIterator struct {
	data  []*testIteratorItem
	curr  int
	valid bool
}

func (ti *testIterator) sort() {
	sort.Slice(ti.data, func(i, j int) bool {
		return ti.data[i].compare(ti.data[j]) <= 0
	})
}

func (ti *testIterator) Seek(key []byte) {
	n := len(ti.data)

	found := sort.Search(n, func(i int) bool {
		return CompareKeys(ti.data[i].key, key) >= 0
	})

	if found == n || !SameKey(ti.data[found].key, key) {
		ti.valid = false
		return
	}

	ti.curr = found
	ti.valid = true
}

func (ti *testIterator) SeekToFirst() {
	ti.curr = 0
	ti.valid = true
}

func (ti *testIterator) SeekToLast() {
	ti.curr = len(ti.data) - 1
	ti.valid = true
}

func (ti *testIterator) Next() {
	if ti.curr+1 >= len(ti.data) {
		ti.valid = false
	} else {
		ti.valid = true
		ti.curr++
	}
}

func (ti *testIterator) Prev() {
	if ti.curr-1 < 0 {
		ti.valid = false
	} else {
		ti.valid = true
		ti.curr--
	}
}

func (ti *testIterator) Key() []byte {
	if !ti.valid {
		return nil
	}

	return ti.data[ti.curr].key
}

func (ti *testIterator) Value() EValue {
	var v EValue
	if !ti.valid {
		return v
	}

	curr := ti.data[ti.curr]

	v.Value = curr.value
	v.Meta = curr.rtype
	v.version = ParseVersion(curr.key)

	return v
}

func (ti *testIterator) Valid() bool {
	return ti.valid
}

func (ti *testIterator) Close() error {
	return nil
}
