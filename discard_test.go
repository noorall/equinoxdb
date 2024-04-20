package equinox

import (
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDiscardWriteRead(t *testing.T) {
	dir, err := ioutil.TempDir("", "golbat-test")
	require.Nil(t, err)
	defer removeDir(dir)

	opt := Options{
		ValueLogDir: dir,
	}

	d, err := NewDiscard(opt)
	require.Nil(t, err)
	require.Zero(t, d.next)
	f, c := d.Max()
	require.Zero(t, f)
	require.Zero(t, c)

	// first write and read
	for i := uint64(1); i < 10; i++ {
		require.Equal(t, i*10, d.Update(uint32(i), i*10))
	}
	d.Iterate(func(fid, count uint64) {
		require.Equal(t, fid*10, count)
	})
	maxFid, maxCount := d.Max()
	require.Equal(t, uint32(9), maxFid)
	require.Equal(t, uint64(90), maxCount)

	// second write and read
	for i := uint64(1); i < 10; i++ {
		require.Equal(t, i*20, d.Update(uint32(i), i*10))
	}
	d.Iterate(func(fid, count uint64) {
		require.Equal(t, fid*20, count)
	})
	maxFid, maxCount = d.Max()
	require.Equal(t, uint32(9), maxFid)
	require.Equal(t, uint64(180), maxCount)

	// write with 0 and read
	for i := uint64(1); i < 10; i++ {
		require.Equal(t, i*20, d.Get(uint32(i)))
	}
	d.Iterate(func(fid, count uint64) {
		require.Equal(t, fid*20, count)
	})
	maxFid, maxCount = d.Max()
	require.Equal(t, uint32(9), maxFid)
	require.Equal(t, uint64(180), maxCount)

	// write with -1 and read
	for i := uint32(1); i < 5; i++ {
		require.Zero(t, d.Reset(i))
	}
	d.Iterate(func(fid, count uint64) {
		if fid < 5 {
			require.Zero(t, count)
		} else {
			require.Equal(t, fid*20, count)
		}
	})
	require.Equal(t, uint64(1160), d.Update(8, 1000))
	maxFid, maxCount = d.Max()
	require.Equal(t, uint32(8), maxFid)
	require.Equal(t, uint64(1160), maxCount)
}

func TestDiscardReload(t *testing.T) {
	dir, err := ioutil.TempDir("", "golbat-test")
	require.Nil(t, err)
	defer removeDir(dir)

	opt := Options{
		ValueLogDir: dir,
	}

	d, err := NewDiscard(opt)
	require.Nil(t, err)
	require.Zero(t, d.next)

	// write
	d.Update(1, 1)
	d.Update(2, 2)
	d.Update(1, 3)
	d.Reset(2)
	d.Update(3, 10)

	// close
	require.Nil(t, d.Close())

	// reopen
	d, err = NewDiscard(opt)
	require.Nil(t, err)
	require.Equal(t, 3, d.next)
	// read
	require.Equal(t, uint64(4), d.Get(1))
	require.Equal(t, uint64(0), d.Get(2))
	require.Equal(t, uint64(10), d.Get(3))
}
