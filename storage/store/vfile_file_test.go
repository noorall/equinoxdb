package store

import (
	"equinox/storage/config"
	"github.com/stretchr/testify/require"
	"hash/crc32"
	"os"
	"testing"
)

func TestVFileBasic(t *testing.T) {
	dir, _ := os.MkdirTemp("", "test-vfile")
	defer os.RemoveAll(dir)
	fid := uint32(0)
	fpath := valueFilePath(dir, fid)
	vf := &ValueFile{
		fid:       fid,
		path:      fpath,
		lifeCycle: 0,
		pos:       0,
	}

	err := vf.Open(fpath, os.O_RDWR|os.O_CREATE|os.O_EXCL, 2*int(config.DefaultMaxValueFileSize))
	require.NoError(t, err)

	key := "test"
	b := "block"
	minTime := int64(0)
	maxTime := int64(100)
	off, err := vf.WriteBlock([]byte(key), minTime, maxTime, []byte(b))
	require.NoError(t, err)
	require.Equal(t, uint32(VFileHeaderSize+len(key)+crc32.Size), off)
}

func TestVFileGC(t *testing.T) {
	dir, _ := os.MkdirTemp("", "test-vfile-gc")
	defer os.RemoveAll(dir)
	fid := uint32(0)
	fpath := valueFilePath(dir, fid)
	vf := &ValueFile{
		fid:       fid,
		path:      fpath,
		lifeCycle: 0,
		pos:       0,
	}

	err := vf.Open(fpath, os.O_RDWR|os.O_CREATE|os.O_EXCL, 2*int(config.DefaultMaxValueFileSize))
	require.NoError(t, err)

	key := "test"
	b := "block"
	var off []uint32
	for i := 0; i < 100; i++ {
		offset, err := vf.WriteBlock([]byte(key), int64(i), int64(i), []byte(b))
		require.NoError(t, err)
		off = append(off, offset)
	}
	err = vf.MarkAsDelete([]byte("test"), 0, 50, true)
	require.NoError(t, err)
	require.Equal(t, vf.IsValidateValue([]byte("test"), 0, 50), false)
}
