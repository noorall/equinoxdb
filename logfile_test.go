package equinox

import (
	"bytes"
	"io"
	"io/ioutil"
	"math"
	"os"
	"testing"

	"equinox/internel"
	"github.com/stretchr/testify/require"
)

func TestHeader(t *testing.T) {
	h := header{
		klen:       1,
		vlen:       1,
		recordType: byte(Value),
	}
	res := make([]byte, maxHeaderSize)
	len := h.Encode(res)
	require.Equal(t, 3, len)
	h2 := h
	require.Equal(t, 3, h.Decode(res))
	require.EqualValues(t, h2, h)

	h = header{
		klen:       math.MaxUint16,
		vlen:       math.MaxUint16,
		recordType: byte(Delete),
	}
	res = make([]byte, maxHeaderSize)
	len = h.Encode(res)
	require.Equal(t, 7, len)
	h2 = h
	require.Equal(t, 7, h.Decode(res))
	require.EqualValues(t, h2, h)

	h = header{
		klen:       math.MaxUint32,
		vlen:       math.MaxUint32,
		recordType: byte(ValPtr),
	}
	res = make([]byte, maxHeaderSize)
	len = h.Encode(res)
	require.Equal(t, maxHeaderSize, len)
	h2 = h
	require.Equal(t, maxHeaderSize, h.Decode(res))
	require.EqualValues(t, h2, h)

	reader := bytes.NewReader(res)
	hashReader := internel.NewHashReader(reader)
	len, _ = h.decodeFrom(hashReader)
	require.Equal(t, maxHeaderSize, len)
	require.EqualValues(t, h2, h)
}

func TestValPtr(t *testing.T) {
	v := valPtr{
		fid:    1,
		len:    10,
		offset: 100,
	}

	b := v.Encode()

	v2 := v
	v.Decode(b)
	require.EqualValues(t, v2, v)
}

const logSize = 1024 * 4 // 4KB
func TestLogFile(t *testing.T) {
	dir, _ := ioutil.TempDir("", "golbat-test")
	defer removeDir(dir)

	f, _ := ioutil.TempFile(dir, "logfile")
	var log logFile
	buf := &bytes.Buffer{}
	log.Open(f.Name(), os.O_CREATE|os.O_RDWR, logSize)
	defer log.Close()

	const k = "key1"
	const v = "golbat-test: logfile"

	// write a entry
	e := &entry{
		key:   []byte(k),
		value: []byte(v),
		rtype: Value,
	}

	log.WriteEntry(e, buf)

	// read back
	e2, _ := log.ReadEntry(log.Data, 0)
	require.EqualValues(t, k, string(e2.key))
	require.EqualValues(t, v, string(e2.value))
	require.EqualValues(t, Value, e2.rtype)

	// write more
	sz := log.pos
	end := sz * 101
	for i := 0; i < 100; i++ {
		log.WriteEntry(e, buf)
	}
	require.Equal(t, end, log.pos)

	// iterate and check every entry
	w := func(entry *entry, _ valPtr) error {
		require.EqualValues(t, e2.key, entry.key)
		require.EqualValues(t, e2.value, entry.value)
		require.EqualValues(t, e2.hlen, entry.hlen)
		require.EqualValues(t, e2.rtype, entry.rtype)

		return nil
	}

	n, err := log.iterate(0, w)
	require.Equal(t, end, n)
	require.Nil(t, err)

	// flush
	log.Flush(end)
	fs, _ := f.Stat()
	require.True(t, fs.Size() >= int64(end))

	// read by valptr
	vp := valPtr{
		fid:    0,
		len:    sz,
		offset: 50 * sz,
	}
	bs, err := log.readWithValPtr(vp)
	require.Nil(t, err)
	e3, _ := log.ReadEntry(bs, 0)
	require.EqualValues(t, e2, e3)

	vp.offset = end
	_, err = log.readWithValPtr(vp)
	require.NotNil(t, err)
	require.ErrorIs(t, err, io.EOF)

	vp.offset = end - sz
	vp.len = sz + 1
	_, err = log.readWithValPtr(vp)
	require.NotNil(t, err)
	require.ErrorIs(t, err, io.EOF)
}

func removeDir(dir string) {
	if err := os.RemoveAll(dir); err != nil {
		panic(err)
	}
}
