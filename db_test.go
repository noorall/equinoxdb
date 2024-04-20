package equinox

import (
	"fmt"
	"io/ioutil"
	"sort"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

var testWriteOptions = WriteOptions{Sync: true}

func getTestDB(t *testing.T) (DB, func()) {
	dir, err := ioutil.TempDir("", "golbat-test")
	require.NoError(t, err)

	options := DefaultOptions(dir)
	db, err := Open(options)
	require.NoError(t, err)

	return db, func() {
		db.Close()
		removeDir(dir)
	}
}

func TestOpenDB(t *testing.T) {
	dbi, def := getTestDB(t)
	defer def()

	db := dbi.(*DBImpl)

	require.Equal(t, uint64(0), db.getMaxVersion())
	require.True(t, db.mem.NewTable)
	require.Equal(t, 0, len(db.imm))
	require.Equal(t, db.option.MaxLevels, len(db.ls.GetLevelMeta()))
	require.Equal(t, 0, len(db.ls.GetTableMeta()))

	// open again
	dbi2, err := Open(*db.option)
	require.Error(t, err)
	require.Nil(t, dbi2)
}

func TestDBWrite(t *testing.T) {
	t.Run("without batch", func(t *testing.T) {
		db, def := getTestDB(t)
		defer def()

		for i := 0; i < 100; i++ {
			require.NoError(t, db.Put(&DefaultWriteOptions, []byte(fmt.Sprintf("key%d", i)),
				[]byte(fmt.Sprintf("val%d", i))))
		}
	})

	t.Run("with batch", func(t *testing.T) {
		db, def := getTestDB(t)
		defer def()

		batch := NewWriteBatch(db)
		for i := 0; i < 100; i++ {
			k := []byte(fmt.Sprintf("key%d", i))
			v := []byte(fmt.Sprintf("val%d", i))
			if batch.FullWith(k, v) {
				require.NoError(t, db.Write(&DefaultWriteOptions, batch))
				batch.Clear()
			}

			batch.Put(k, v)
		}

		if !batch.Empty() {
			require.NoError(t, db.Write(&DefaultWriteOptions, batch))
		}
	})
}

func TestDBGet(t *testing.T) {
	db, def := getTestDB(t)
	defer def()

	db.Put(&testWriteOptions, []byte("key1"), []byte("val1"))
	res, err := db.GetExtend(&DefaultReadOptions, []byte("key1"))
	require.NoError(t, err)
	require.Equal(t, "val1", string(res.Value))
	require.Equal(t, Value, res.Meta)

	db.Put(&testWriteOptions, []byte("key1"), []byte("val2"))
	res, err = db.GetExtend(&DefaultReadOptions, []byte("key1"))
	require.NoError(t, err)
	require.Equal(t, "val2", string(res.Value))
	require.Equal(t, Value, res.Meta)

	db.Delete(&testWriteOptions, []byte("key1"))
	_, err = db.GetExtend(&DefaultReadOptions, []byte("key1"))
	require.Equal(t, ErrKeyNotFound, err)

	db.Put(&testWriteOptions, []byte("key1"), []byte("val3"))
	res, err = db.GetExtend(&DefaultReadOptions, []byte("key1"))
	require.NoError(t, err)
	require.Equal(t, "val3", string(res.Value))
	require.Equal(t, Value, res.Meta)

	longVal := make([]byte, 1000)
	db.Put(&testWriteOptions, []byte("key1"), longVal)
	res, err = db.GetExtend(&DefaultReadOptions, []byte("key1"))
	require.NoError(t, err)
	require.EqualValues(t, longVal, res.Value)
	require.Equal(t, Value, res.Meta)
}

func TestDBReadAndWrite(t *testing.T) {
	db, def := getTestDB(t)
	defer def()

	for i := 0; i < 100; i++ {
		require.NoError(t, db.Put(&testWriteOptions, []byte(fmt.Sprintf("key%d", i)),
			[]byte(fmt.Sprintf("val%d", i))))
	}

	for i := 0; i < 100; i++ {
		res, err := db.GetExtend(&DefaultReadOptions, ([]byte(fmt.Sprintf("key%d", i))))
		require.NoError(t, err)

		expected := []byte(fmt.Sprintf("val%d", i))
		require.EqualValues(t, expected, res.Value)
	}
}

func TestDBReadAndWriteWithSnapshot(t *testing.T) {
	db, def := getTestDB(t)
	defer def()

	var snapshot *Snapshot
	for i := 0; i < 100; i++ {
		require.NoError(t, db.Put(&testWriteOptions, []byte(fmt.Sprintf("key%d", i)),
			[]byte(fmt.Sprintf("val%d", i))))

		if i == 50 {
			snapshot = db.GetSnapshot()
		}
	}

	require.Equal(t, uint64(51), snapshot.version)

	options := DefaultReadOptions
	options.Snapshot = snapshot

	for i := 0; i < 100; i++ {
		res, err := db.GetExtend(&options, ([]byte(fmt.Sprintf("key%d", i))))
		if i <= 50 {
			require.NoError(t, err)
			require.EqualValues(t, []byte(fmt.Sprintf("val%d", i)), res.Value)
		} else {
			require.Equal(t, ErrKeyNotFound, err)
		}
	}

	db.ReleaseSnapshot(snapshot)
}

func TestInvalidWrite(t *testing.T) {
	db, def := getTestDB(t)
	defer def()
	batch := NewWriteBatch(db)
	require.ErrorIs(t, db.Write(&DefaultWriteOptions, batch), ErrEmptyBatch)

	batch.Put([]byte{}, []byte("value"))
	require.ErrorIs(t, db.Write(&DefaultWriteOptions, batch), ErrEmptyKey)

	batch.Clear()
	batch.Put(make([]byte, 65537), []byte("value"))
	err := db.Write(&DefaultWriteOptions, batch)
	require.Error(t, err)
	require.Contains(t, err.Error(), "exceeded")

	batch.Clear()
	old := wbOption.ValueLogFileSize
	wbOption.ValueLogFileSize = 1 << 10 // 1KB
	batch.Put([]byte("key"), make([]byte, 2046))
	err = db.Write(&DefaultWriteOptions, batch)
	require.Error(t, err)
	require.Contains(t, err.Error(), "exceeded")
	wbOption.ValueLogFileSize = old

	wbOption.maxBatchCount = 100
	wbOption.maxBatchSize = 1024

	for i := 0; i <= 100; i++ {
		batch.Put([]byte(fmt.Sprintf("key%d", i)), []byte(fmt.Sprintf("val%d", i)))
	}
	err = db.Write(&DefaultWriteOptions, batch)
	require.ErrorIs(t, err, ErrBatchTooBig)
}

func TestConcurrentWrite(t *testing.T) {
	db, def := getTestDB(t)
	defer def()

	n := 20
	m := 500
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < m; j++ {
				db.Put(&testWriteOptions, []byte(fmt.Sprintf("k%05d_%08d", i, j)),
					[]byte(fmt.Sprintf("v%05d_%08d", i, j)))
			}
		}(i)
	}
	wg.Wait()

	iter, err := db.NewIterator(&DefaultReadOptions)
	require.NoError(t, err)
	defer func() {
		_ = iter.Close()
	}()

	var i, j int
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		k := iter.Key()
		v := iter.Value()

		if k == nil {
			break
		}

		require.EqualValues(t, fmt.Sprintf("k%05d_%08d", i, j), k)
		require.EqualValues(t, fmt.Sprintf("v%05d_%08d", i, j), v.Value)
		j++

		if j == m {
			i++
			j = 0
		}
	}

	require.EqualValues(t, n, i)
	require.EqualValues(t, 0, j)
}

func TestWriteToValueLog(t *testing.T) {
	dir, err := ioutil.TempDir("", "golbat-test")
	require.NoError(t, err)
	defer removeDir(dir)

	options := DefaultOptions(dir)
	options.ValueThreshold = 64
	db, err := Open(options)
	require.NoError(t, err)
	defer db.Close()

	n := 1000
	wb := NewWriteBatch(db)
	for i := 0; i < n; i++ {
		k := []byte(fmt.Sprintf("key%d", i))
		v := []byte(fmt.Sprintf("val%064d", i))
		if wb.FullWith(k, v) {
			require.NoError(t, db.Write(&testWriteOptions, wb))
			wb.Clear()
		}

		wb.Put(k, v)
	}
	require.NoError(t, db.Write(&testWriteOptions, wb))

	for i := 0; i < n; i++ {
		res, err := db.GetExtend(&DefaultReadOptions, ([]byte(fmt.Sprintf("key%d", i))))
		require.NoError(t, err)
		require.EqualValues(t, []byte(fmt.Sprintf("val%064d", i)), res.Value)
	}
}

func TestDBLoad(t *testing.T) {
	const n = 10000

	testLoad := func(t *testing.T, compression CompressionType) {
		dir, err := ioutil.TempDir("", "golbat-test")
		require.NoError(t, err)
		defer removeDir(dir)

		options := DefaultOptions(dir)
		options.CompressionType = compression
		{
			db, err := Open(options)
			require.NoError(t, err)
			for i := 0; i < n; i++ {
				if (i % 10000) == 0 {
					fmt.Printf("Putting i=%d\n", i)
				}

				k := []byte(fmt.Sprintf("%09d", i))
				db.Put(&DefaultWriteOptions, k, k)
			}

			dbi, _ := db.(*DBImpl)
			require.Equal(t, uint64(10000), dbi.getMaxVersion())
			db.Close()
		}

		db, err := Open(options)
		require.NoError(t, err)
		dbi, _ := db.(*DBImpl)
		require.Equal(t, uint64(10000), dbi.getMaxVersion())

		for i := 0; i < n; i++ {
			if (i % 10000) == 0 {
				fmt.Printf("Testing i=%d\n", i)
			}

			k := fmt.Sprintf("%09d", i)

			res, err := db.GetExtend(&DefaultReadOptions, []byte(k))
			require.NoError(t, err)
			require.EqualValues(t, k, string(res.Value))
			require.Equal(t, Value, res.Meta)
		}

		db.Close()
		tablesMetas := dbi.ls.GetTableMeta()
		tableFiles := make(map[uint64]bool)
		for _, meta := range tablesMetas {
			tableFiles[meta.Id] = true
		}

		idMap := getTableIdMap(options.Dir)
		require.Equal(t, len(idMap), len(tableFiles))
		for fileID := range idMap {
			require.True(t, tableFiles[fileID], "%d", fileID)
		}

		var fileIDs []uint64
		for k := range tableFiles { // Map to array.
			fileIDs = append(fileIDs, k)
		}
		sort.Slice(fileIDs, func(i, j int) bool { return fileIDs[i] < fileIDs[j] })
		fmt.Printf("FileIDs: %v\n", fileIDs)
	}

	t.Run("without compression", func(t *testing.T) {
		testLoad(t, NoCompression)
	})

	t.Run("with compression", func(t *testing.T) {
		testLoad(t, ZSTDCompression)
	})
}

func TestDBIterate(t *testing.T) {
	db, def := getTestDB(t)
	defer def()

	bkey := func(i int) []byte {
		return []byte(fmt.Sprintf("%09d", i))
	}
	bval := func(i int) []byte {
		return []byte(fmt.Sprintf("%025d", i))
	}

	n := 10000
	for i := 0; i < n; i++ {
		if (i % 1000) == 0 {
			t.Logf("Put i=%d\n", i)
		}
		db.Put(&testWriteOptions, bkey(i), bval(i))
	}

	iter, err := db.NewIterator(&DefaultReadOptions)
	defer func() {
		_ = iter.Close()
	}()

	require.NoError(t, err)

	t.Run("first iterate", func(t *testing.T) {
		var count int
		for iter.SeekToFirst(); iter.Valid(); iter.Next() {
			key := iter.Key()
			value := iter.Value()

			require.EqualValues(t, bkey(count), key)
			require.EqualValues(t, bval(count), value.Value)

			count++
		}

		require.Equal(t, n, count)
	})

	t.Run("second iterate", func(t *testing.T) {
		idx := 5050
		for iter.Seek(bkey(idx)); iter.Valid(); iter.Next() {
			require.EqualValues(t, bkey(idx), iter.Key())
			require.EqualValues(t, bval(idx), iter.Value().Value)
			idx++
		}
	})
}

func TestConcurrentWriteAndIterate(t *testing.T) {
	db, def := getTestDB(t)
	defer def()

	bkey := func(i int) []byte {
		return []byte(fmt.Sprintf("%09d", i))
	}
	bval := func(i int) []byte {
		return []byte(fmt.Sprintf("%025d", i))
	}

	// pre writes
	n := 10000
	for i := 0; i < n; i++ {
		if (i % 1000) == 0 {
			t.Logf("Put i=%d\n", i)
		}
		db.Put(&testWriteOptions, bkey(i), bval(i))
	}

	// the iterator can't able to see those writes that performed after this iterator is created
	iter, err := db.NewIterator(&DefaultReadOptions)
	require.NoError(t, err)
	defer iter.Close()

	var wg sync.WaitGroup
	wg.Add(2)
	write := func(db DB, wg *sync.WaitGroup) {
		const overlap = 5000
		defer wg.Done()

		// write some existed keys and some new keys
		for i := 0; i < n; i++ {
			if (i % 1000) == 0 {
				t.Logf("Put i=%d\n", i)
			}
			db.Put(&DefaultWriteOptions, bkey(i+overlap), bval(i+overlap))
		}
	}

	iterate := func(iter Iterator, wg *sync.WaitGroup) {
		defer wg.Done()
		var count int
		for iter.SeekToFirst(); iter.Valid(); iter.Next() {
			key := iter.Key()
			value := iter.Value()

			require.EqualValues(t, bkey(count), key)
			require.EqualValues(t, bval(count), value.Value)

			count++
		}

		require.Equal(t, n, count)
	}

	go write(db, &wg)
	go iterate(iter, &wg)

	wg.Wait()
}
