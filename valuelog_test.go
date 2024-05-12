package equinox

import (
	"equinox/data_type"
	"fmt"
	"io/ioutil"
	"math/rand"
	"testing"

	"equinox/internel"
	"github.com/stretchr/testify/require"
)

func getDefaultOption() *Options {
	return &Options{
		ValueThreshold:     32,
		ValueLogFileSize:   1<<30 - 1,
		ValueLogMaxEntries: 100000,
		maxBatchSize:       1 << 20,
		maxBatchCount:      100,
		Logger:             internel.DefaultLogger(internel.DEBUG),
	}
}

func TestValueLogBasic(t *testing.T) {
	dir, err := ioutil.TempDir("", "golbat-test")
	require.Nil(t, err)
	defer removeDir(dir)

	option := getDefaultOption()
	option.ValueLogDir = dir

	log, err := OpenValueLog(*option, 0)
	require.Nil(t, err)
	require.NotNil(t, log)
	defer log.Close()

	const value = "abcdefghijklmnopqrstuvwxyz"
	const longValue = "abcdefghijklmnopqrstuvwxyz0123456"
	require.True(t, len(longValue) >= option.ValueThreshold)

	wb := &writeBatchInternel{}
	wb.Put([]byte("key1"), []byte(value))
	wb.Put([]byte("key2"), []byte(longValue))

	log.Write(wb)
	require.Equal(t, 2, len(wb.ptrs))
	require.EqualValues(t, valPtr{}, wb.ptrs[0])

	ropt := &ReadOptions{VerifyCheckSum: true}
	entry, err := log.Read(ropt, wb.ptrs[1])
	require.Equal(t, Value, entry.rtype)
	require.NotNil(t, entry)
	require.Nil(t, err)
	require.EqualValues(t, []byte("key2"), entry.key)
	require.EqualValues(t, []byte(longValue), entry.value)
}

func TestValueLogReload(t *testing.T) {
	dir, err := ioutil.TempDir("", "golbat-test")
	require.Nil(t, err)
	defer removeDir(dir)

	option := getDefaultOption()
	option.ValueLogDir = dir
	option.ValueThreshold = 1

	log, err := OpenValueLog(*option, 0)
	require.Nil(t, err)
	require.NotNil(t, log)
	defer log.Close()

	wbOption = option
	wb := &writeBatchInternel{}

	// write
	for i := 0; i < 100; i++ {
		key := getKey(i)
		value := getValue(i * 100)
		if wb.FullWith(key, value) {
			require.NoError(t, log.Write(wb))
			wb.Clear()
		}

		wb.Put(key, value)
	}

	require.NoError(t, log.Write(wb))
	wb.Clear()

	// read
	lf, ok := log.filesMap[1]
	require.True(t, ok)
	i := 0
	vps := make([]valPtr, 0, 100)
	lf.iterate(0, func(e *entry, vp valPtr) error {
		vps = append(vps, vp)
		require.EqualValues(t, getKey(i), e.key)
		require.EqualValues(t, getValue(i*100), e.value)
		i++

		return nil
	})

	// close
	require.NoError(t, log.Close())
	// release log file lock
	for _, lf := range log.filesMap {
		lf.lock.Unlock()
	}

	// reopen
	log, err = OpenValueLog(*option, 0)
	require.Nil(t, err)
	require.NotNil(t, log)
	// read
	ropt := &ReadOptions{VerifyCheckSum: true}
	for i, vp := range vps {
		e, err := log.Read(ropt, vp)
		require.Nil(t, err)
		require.EqualValues(t, getKey(i), e.key)
		require.EqualValues(t, getValue(i*100), e.value)
	}
}

func TestValueLogGC(t *testing.T) {
	dir, err := ioutil.TempDir("", "golbat-test")
	require.NoError(t, err)
	defer removeDir(dir)

	option := getDefaultOption()
	option.ValueLogDir = dir
	option.ValueLogFileSize = 1 << 20 // 2 MB
	option.ValueThreshold = 1 << 10   // 1KB

	log, err := OpenValueLog(*option, 0)
	require.Nil(t, err)
	defer log.Close()

	wbOption = option
	wb := &writeBatchInternel{}

	vals := make(map[string]*EValue)
	valueSize := 4 << 10 // 4 KB
	// add 100 entries
	for i := 0; i < 100; i++ {
		k, v := getKey((i)), getRandValue(valueSize)
		vals[string(k)] = &EValue{Meta: Value | ValPtr, Value: v}
		if wb.FullWith(k, v) {
			require.NoError(t, log.Write(wb))
			wb.Clear()
		}

		wb.Put(k, v)
	}
	require.NoError(t, log.Write(wb))
	wb.Clear()

	// delete first 50 entries
	for i := 0; i < 50; i++ {
		k := getKey(i)
		vals[string(k)].Meta = Delete | ValPtr

		if wb.FullWith(k, emptyValue) {
			require.NoError(t, log.Write(wb))
			wb.Clear()
		}

		wb.Delete(k)
	}
	require.NoError(t, log.Write(wb))
	wb.Clear()

	// gc
	last, ok := log.filesMap[1]
	require.True(t, ok)
	_, err = log.createValueLogFile()
	require.NoError(t, err)
	require.Equal(t, uint32(2), log.maxFid)
	require.Equal(t, 2, len(log.filesMap))

	filter := func(key []byte) *EValue {
		return vals[string(key)]
	}
	db := NewMockDB(log, option, filter)
	require.NoError(t, log.gcLogFile(last, db))

	// get the last log file
	last, ok = log.filesMap[2]
	require.True(t, ok)
	offset, err := last.iterate(0, func(e *entry, _ valPtr) error {
		v, ok := vals[string(e.key)]
		require.True(t, ok)
		require.EqualValues(t, v.Value, e.value)
		require.EqualValues(t, Value, e.rtype)

		return nil
	})

	require.NoError(t, err)
	require.Equal(t, last.size, offset)
}

type mockDB struct {
	vlog   *valueLog
	option *Options
	filter func([]byte) *EValue
}

func NewMockDB(v *valueLog, option *Options, filter func([]byte) *EValue) DB {
	return &mockDB{
		vlog:   v,
		option: option,
		filter: filter,
	}
}

func (db *mockDB) Put(options *WriteOptions, key, value []byte) error {
	wb := &WriteBatch{}
	wb.Put(key, value)

	return db.Write(options, wb)
}

func (db *mockDB) Delete(options *WriteOptions, key []byte) error {
	wb := &WriteBatch{}
	wb.Delete(key)

	return db.Write(options, wb)
}

func (db *mockDB) Write(options *WriteOptions, batch *WriteBatch) error {
	ibatch := &writeBatchInternel{}
	ibatch.Fill(batch)
	return db.vlog.Write(ibatch)
}

func (db *mockDB) Get(options *ReadOptions, key []byte) (value []byte, err error) {
	return db.filter(key).Value, nil
}

func (db *mockDB) NewIterator(options *ReadOptions) (iterator Iterator, err error) {
	return nil, nil
}

func (db *mockDB) GetSnapshot() *Snapshot {
	return nil
}

func (db *mockDB) ReleaseSnapshot(snapshot *Snapshot) {

}

func (db *mockDB) GetExtend(options *ReadOptions, key []byte) (value *EValue, err error) {
	return db.filter(key), nil
}

func (db *mockDB) GetOption() Options {
	return *db.option
}

func (db *mockDB) Close() error {
	return nil
}

func (db *mockDB) TSWrite(options *WriteOptions, points []data_type.TSEntry) error {
	return nil
}

func getKey(i int) []byte {
	return []byte(fmt.Sprintf("key%05d", i))
}

func getValue(i int) []byte {
	return []byte(fmt.Sprintf("value%05d", i))
}

func getRandValue(sz int) []byte {
	buf := make([]byte, sz)
	rand.Read(buf[:rand.Intn(sz)])

	return buf
}
