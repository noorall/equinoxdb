package equinox

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func key(prefix string, i int) string {
	return fmt.Sprintf("%s%04d", prefix, i)
}

func getTestTableOptions() Options {
	return Options{
		CompressionType:      ZSTDCompression,
		ZSTDCompressionLevel: 15,
		BlockSize:            4 * 1024,
		BloomFalsePositive:   0.01,
		comparator:           CompareKeys,
	}
}

func buildTestTable(t *testing.T, prefix string, n int, opts Options) *Table {
	if opts.BlockSize == 0 {
		opts.BlockSize = 4 * 1024
	}
	AssertTrue(n <= 10000)
	keyValues := make([][]string, n)
	for i := 0; i < n; i++ {
		k := key(prefix, i)
		v := fmt.Sprintf("%d", i)
		keyValues[i] = []string{k, v}
	}
	return buildTable(t, keyValues, opts)
}

func buildTable(t *testing.T, keyValues [][]string, opts Options) *Table {
	b := NewTableBuilder(opts)
	dir, err := ioutil.TempDir("", "golbat-test")
	require.NoError(t, err)
	defer removeDir(dir)

	filename := NewTableFileName(rand.Uint64(), dir)

	sort.Slice(keyValues, func(i, j int) bool {
		return keyValues[i][0] < keyValues[j][0]
	})
	for _, kv := range keyValues {
		AssertTrue(len(kv) == 2)
		b.Add(KeyWithVersion([]byte(kv[0]), 0),
			EValue{Value: []byte(kv[1]), Meta: Value}, 0)
	}
	tbl, err := CreateTable(filename, b)
	require.NoError(t, err, "writing to file failed")
	return tbl
}

func TestTableBasic(t *testing.T) {
	opts := getTestTableOptions()
	tbl := buildTestTable(t, "key", 10, opts)
	defer tbl.DecrRef()

	iter := tbl.NewIterator(false)
	defer iter.Close()
	require.True(t, iter.Valid())

	count := 0
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		k := KeyWithVersion([]byte(key("key", count)), 0)
		v := fmt.Sprintf("%d", count)

		require.EqualValues(t, k, iter.Key())
		require.Equal(t, v, string(iter.Value().Value))
		count++
	}

	require.False(t, iter.Valid())
	require.Equal(t, 10, count)
}

func TestTableManyEntries(t *testing.T) {
	n := 500000
	opts := getTestTableOptions()
	b := NewTableBuilder(opts)
	dir, err := ioutil.TempDir("", "golbat-test")
	require.NoError(t, err)
	defer removeDir(dir)

	filename := NewTableFileName(rand.Uint64(), dir)
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("%016x", i)
		v := fmt.Sprintf("%d", i)

		b.Add([]byte(k), EValue{Value: []byte(v), Meta: Value}, 0)
	}
	tbl, err := CreateTable(filename, b)
	require.NoError(t, err, "writing to file failed")

	iter := tbl.NewIterator(false)
	defer iter.Close()
	require.True(t, iter.Valid())

	count := 0
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		k := fmt.Sprintf("%016x", count)
		v := fmt.Sprintf("%d", count)

		require.EqualValues(t, k, iter.Key())
		require.Equal(t, v, string(iter.Value().Value))
		count++
	}
	require.False(t, iter.Valid())
	require.Equal(t, n, count)
}

func TestTableBigValues(t *testing.T) {
	value := func(i int) []byte {
		return []byte(fmt.Sprintf("%01048576d", i)) // Return 1MB value which is > math.MaxUint16.
	}

	rand.Seed(time.Now().UnixNano())

	n := 100 // Insert 100 keys.
	opts := Options{CompressionType: ZSTDCompression, BlockSize: 4 * 1024, BloomFalsePositive: 0.01,
		tableSize: uint64(n * 1 << 20)}
	builder := NewTableBuilder(opts)

	for i := 0; i < n; i++ {
		key := KeyWithVersion([]byte(key("", i)), uint64(i+1))
		vs := EValue{Value: value(i), Meta: Value}
		builder.Add(key, vs, 0)
	}

	dir, err := ioutil.TempDir("", "golbat-test")
	require.NoError(t, err)
	defer removeDir(dir)

	filename := NewTableFileName(rand.Uint64(), dir)

	tbl, err := CreateTable(filename, builder)
	require.NoError(t, err, "unable to open table")
	defer tbl.DecrRef()

	iter := tbl.NewIterator(false)
	defer iter.Close()
	require.True(t, iter.Valid())

	count := 0
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		require.Equal(t, []byte(key("", count)), ParseKey(iter.Key()), "keys are not equal")
		require.Equal(t, value(count), iter.Value().Value, "values are not equal")
		count++
	}
	require.False(t, iter.Valid(), "table iterator should be invalid now")
	require.Equal(t, n, count)
	require.Equal(t, n, int(tbl.MaxVersion()))
}

func TestTableChecksum(t *testing.T) {
	rand.Seed(time.Now().Unix())
	// we are going to write random byte at random location in table file.
	rb := make([]byte, 100)
	rand.Read(rb)
	opts := getTestTableOptions()
	opts.VerifyTableChecksum = true
	tbl := buildTestTable(t, "k", 10000, opts)
	// Write random bytes at random location.
	start := rand.Intn(len(tbl.Data) - len(rb))
	n := copy(tbl.Data[start:], rb)
	require.Equal(t, n, len(rb))

	require.Panics(t, func() {
		// Either OpenTable will panic on corrupted data or the checksum verification will fail.
		_, err := OpenTable(tbl.MMapFile, opts)
		if strings.Contains(err.Error(), "checksum") {
			panic("checksum mismatch")
		}
	})
}

func TestMaxVersion(t *testing.T) {
	opts := getTestTableOptions()
	b := NewTableBuilder(opts)
	dir, err := ioutil.TempDir("", "golbat-test")
	require.NoError(t, err)
	defer removeDir(dir)

	n := 1000
	for i := 0; i < n; i++ {
		b.Add(KeyWithVersion([]byte(key("key", i)), uint64(i+1)),
			EValue{Value: []byte(key("value", i)), Meta: Value}, 0)
	}

	filename := NewTableFileName(rand.Uint64(), dir)
	table, err := CreateTable(filename, b)
	defer table.DecrRef()

	require.NoError(t, err)
	require.Equal(t, n, int(table.MaxVersion()))
}

func TestSmallestAndBiggest(t *testing.T) {
	opts := getTestTableOptions()
	tbl := buildTestTable(t, "key", 1000, opts)
	defer tbl.DecrRef()

	smallest := KeyWithVersion([]byte(key("key", 0)), 0)
	biggest := KeyWithVersion([]byte(key("key", 999)), 0)

	require.EqualValues(t, smallest, tbl.Smallest())
	require.EqualValues(t, biggest, tbl.Biggest())
}

func TestDoesNotHave(t *testing.T) {
	opts := getTestTableOptions()
	tbl := buildTestTable(t, "key", 10000, opts)
	defer tbl.DecrRef()

	notContain := uint32(1237882)
	require.True(t, tbl.DoesNotHave(notContain))
}

func TestDoesNotHaveRace(t *testing.T) {
	opts := getTestTableOptions()
	tbl := buildTestTable(t, "key", 10000, opts)
	defer tbl.DecrRef()

	var wg sync.WaitGroup
	n, notContain := 5, uint32(1237882)

	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			require.True(t, tbl.DoesNotHave(notContain))
			defer wg.Done()
		}()
	}

	wg.Wait()
}

const numEntries = 5000000

// benchmark test
func BenchmarkTableRead(b *testing.B) {
	tbl := buildBenchmarkTable(b, numEntries)
	b.Logf("table size: %d MB", tbl.Size()/(1<<20))
	defer tbl.DecrRef()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		func() {
			iter := tbl.NewIterator(false)
			defer iter.Close()

			for iter.SeekToFirst(); iter.Valid(); iter.Next() {

			}
		}()
	}
}

func BenchmarkTableReadAndBuild(b *testing.B) {
	tbl := buildBenchmarkTable(b, numEntries)
	b.Logf("table size: %d MB", tbl.Size()/(1<<20))
	defer tbl.DecrRef()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		func() {
			opts := getTestTableOptions()
			newBuilder := NewTableBuilder(opts)
			it := tbl.NewIterator(false)
			defer it.Close()
			for it.SeekToFirst(); it.Valid(); it.Next() {
				vs := it.Value()
				newBuilder.Add(it.Key(), vs, 0)
			}
			newBuilder.Finish()
		}()
	}
}

func BenchmarkTableReadRandom(b *testing.B) {
	tbl := buildBenchmarkTable(b, numEntries)
	defer tbl.DecrRef()
	r := rand.New(rand.NewSource(time.Now().Unix()))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		iter := tbl.NewIterator(false)
		defer iter.Close()

		rid := r.Intn(numEntries)
		k := []byte(fmt.Sprintf("%08x%08x", rid, rid))
		v := []byte(fmt.Sprintf("%d", rid))

		iter.Seek(k)
		if !iter.Valid() {
			b.Fatal("itr should be valid")
		}
		v1 := iter.Value().Value

		if !bytes.Equal(v, v1) {
			b.Fatalf("key:%s expected value:%s, but actual value:%s \n",
				string(k), string(v), string(v1))
		}
	}
}

func BenchmarkTableReadMerge(b *testing.B) {
	dir, err := ioutil.TempDir("", "golbat-test")
	require.NoError(b, err)
	defer removeDir(dir)

	numTables := 5
	tableSize := numEntries / numTables
	tables := []*Table{}
	opts := getTestTableOptions()

	// build tables
	for i := 0; i < numTables; i++ {
		filename := NewTableFileName(rand.Uint64(), dir)
		builder := NewTableBuilder(opts)

		for j := 0; j < tableSize; j++ {
			id := j*numTables + i // tables' key range interleaved

			k := fmt.Sprintf("%08x%08x", id, id)
			v := fmt.Sprintf("%d", id)
			builder.Add([]byte(k),
				EValue{Value: []byte(v), Meta: Value}, 0)
		}

		tbl, err := CreateTable(filename, builder)
		require.NoError(b, err)
		tables = append(tables, tbl)
		defer tbl.DecrRef()
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		func() {
			iters := []Iterator{}
			for _, table := range tables {
				iters = append(iters, table.NewIterator(false))
			}

			miter := NewTablesMergeIterator(&opts, iters, false)
			defer miter.Close()

			for miter.SeekToFirst(); miter.Valid(); miter.Next() {
			}
		}()
	}
}

func buildBenchmarkTable(b *testing.B, count int) *Table {
	opts := getTestTableOptions()
	builder := NewTableBuilder(opts)

	dir, err := ioutil.TempDir("", "golbat-test")
	require.NoError(b, err)
	defer removeDir(dir)

	filename := NewTableFileName(rand.Uint64(), dir)
	for i := 0; i < count; i++ {
		k := fmt.Sprintf("%08x%08x", i, i)
		v := fmt.Sprintf("%d", i)
		builder.Add([]byte(k),
			EValue{Value: []byte(v), Meta: Value}, 0)
	}

	tbl, err := CreateTable(filename, builder)
	require.NoError(b, err)

	return tbl
}

func TestMain(m *testing.M) {
	rand.Seed(time.Now().UTC().UnixNano())
	os.Exit(m.Run())
}
