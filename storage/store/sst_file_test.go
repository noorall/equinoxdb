package store

import (
	"equinox/storage/types"
	"fmt"
	"github.com/stretchr/testify/require"
	"os"
	"path/filepath"
	"strconv"
	"testing"
)

func TestSSTBasic(t *testing.T) {
	dir := t.TempDir()
	fs := newTestFileStore(t, dir)

	data := []keyValues{
		keyValues{"mem1", []types.Value{types.NewValue(0, 1.0)}},
		keyValues{"mem2", []types.Value{types.NewValue(0, 1.0)}},
		keyValues{"mem3", []types.Value{types.NewValue(0, 1.0)}},
		keyValues{"mem4", []types.Value{types.NewValue(0, 1.0)}},
		keyValues{"mem5", []types.Value{types.NewValue(0, 1.0)}},
		keyValues{"mem6", []types.Value{types.NewValue(0, 1.0)}},
		keyValues{"mem7", []types.Value{types.NewValue(0, 1.0)}},
		keyValues{"mem8", []types.Value{types.NewValue(0, 1.0)}},
		keyValues{"mem9", []types.Value{types.NewValue(0, 1.0)}},
		keyValues{"mem10", []types.Value{types.NewValue(0, 1.0)}},
	}

	files, err := newFiles(t, dir, data...)
	if err != nil {
		t.Fatalf("unexpected error creating files: %v", err)
	}

	fs.Replace(nil, files)

	// Search for an entry that exists in the second file
	for i := 1; i <= 10; i++ {
		values, err2 := fs.Read([]byte("mem"+strconv.Itoa(i)), 0)
		require.NoError(t, err2)
		require.Equal(t, len(values), 1)
		require.Equal(t, values[0].Value(), data[i-1].values[0].Value())
	}
}

func TestSSTManyEntries(t *testing.T) {
	dir := t.TempDir()
	fs := newTestFileStore(t, dir)

	data := []keyValues{keyValues{"mem1", make([]types.Value, 0)}}
	for i := 0; i < 500000; i++ {
		data[0].values = append(data[0].values, types.NewValue(int64(i), int64(i)))
	}

	files, err := newFiles(t, dir, data...)
	if err != nil {
		t.Fatalf("unexpected error creating files: %v", err)
	}

	fs.Replace(nil, files)

	sstR := fs.files[0].(*SSTReader)
	val, err := sstR.ReadAll([]byte("mem1"))
	require.NoError(t, err)
	require.Equal(t, len(val), 500000)
	for i := 0; i < 500000; i++ {
		require.Equal(t, val[i].Value(), int64(i))
	}
}

func TestSSTableBigValues(t *testing.T) {
	dir := t.TempDir()
	fs := newTestFileStore(t, dir)
	str := "1231212ewqewqrarwqeqwewqeqweqwrewrasdaseqwewqrewqewqrqwewqeqwrqwewq"

	data := []keyValues{keyValues{"mem1", []types.Value{types.NewValue(0, str)}}}

	files, err := newFiles(t, dir, data...)
	if err != nil {
		t.Fatalf("unexpected error creating files: %v", err)
	}

	fs.Replace(nil, files)
	values, err2 := fs.Read([]byte("mem1"), 0)
	require.NoError(t, err2)
	require.Equal(t, len(values), 1)
	require.Equal(t, values[0].Value(), str)
}
func TestSmallestAndBiggest(t *testing.T) {
	dir := t.TempDir()
	fs := newTestFileStore(t, dir)
	data := []keyValues{
		keyValues{"mem1", []types.Value{types.NewValue(0, 1.0)}},
		keyValues{"mem2", []types.Value{types.NewValue(0, 1.0)}},
		keyValues{"mem3", []types.Value{types.NewValue(0, 1.0)}},
		keyValues{"mem4", []types.Value{types.NewValue(0, 1.0)}},
		keyValues{"mem5", []types.Value{types.NewValue(0, 1.0)}},
		keyValues{"mem6", []types.Value{types.NewValue(0, 1.0)}},
		keyValues{"mem7", []types.Value{types.NewValue(0, 1.0)}},
		keyValues{"mem8", []types.Value{types.NewValue(0, 1.0)}},
		keyValues{"mem9", []types.Value{types.NewValue(0, 1.0)}},
	}

	files, err := newFiles(t, dir, data...)
	if err != nil {
		t.Fatalf("unexpected error creating files: %v", err)
	}

	fs.Replace(nil, files)
	sstR := fs.files[0].(*SSTReader)
	sstR9 := fs.files[8].(*SSTReader)
	smallest, _ := sstR.KeyRange()
	biggest, _ := sstR9.KeyRange()
	require.Equal(t, smallest, []byte("mem1"))
	require.Equal(t, biggest, []byte("mem9"))
}

func TestDoesNotHave(t *testing.T) {
	dir := t.TempDir()
	fs := newTestFileStore(t, dir)
	data := []keyValues{
		keyValues{"mem1", []types.Value{types.NewValue(0, 1.0)}},
		keyValues{"mem2", []types.Value{types.NewValue(0, 1.0)}},
		keyValues{"mem3", []types.Value{types.NewValue(0, 1.0)}},
		keyValues{"mem4", []types.Value{types.NewValue(0, 1.0)}},
		keyValues{"mem5", []types.Value{types.NewValue(0, 1.0)}},
		keyValues{"mem6", []types.Value{types.NewValue(0, 1.0)}},
		keyValues{"mem7", []types.Value{types.NewValue(0, 1.0)}},
		keyValues{"mem8", []types.Value{types.NewValue(0, 1.0)}},
		keyValues{"mem9", []types.Value{types.NewValue(0, 1.0)}},
	}

	files, err := newFiles(t, dir, data...)
	if err != nil {
		t.Fatalf("unexpected error creating files: %v", err)
	}

	fs.Replace(nil, files)

	sstR := fs.files[0].(*SSTReader)
	require.Equal(t, sstR.Contains([]byte("mem1")), true)
	require.Equal(t, sstR.Contains([]byte("mem10")), false)
}

func newTestFileStore(tb testing.TB, dir string) *FileStore {
	fs := NewFileStore(dir)

	tb.Cleanup(func() {
		fs.Close()
	})

	return fs
}

func newFiles(tb testing.TB, dir string, values ...keyValues) ([]string, error) {
	var files []string

	id := 1
	for _, v := range values {
		f := MustTempFile(tb, dir)
		w, err := NewSSTWriter(f)
		if err != nil {
			return nil, err
		}

		if err := w.Write([]byte(v.key), v.values); err != nil {
			return nil, err
		}

		if err := w.WriteIndex(); err != nil {
			return nil, err
		}

		if err := w.Close(); err != nil {
			return nil, err
		}

		newName := filepath.Join(filepath.Dir(f.Name()), DefaultFormatFileName(id, 1)+".sst")
		if err := os.Rename(f.Name(), newName); err != nil {
			return nil, err
		}
		id++

		files = append(files, newName)
	}
	return files, nil
}

func MustTempFile(tb testing.TB, dir string) *os.File {
	f, err := os.CreateTemp(dir, "tsm1test")
	if err != nil {
		panic(fmt.Sprintf("failed to create temp file: %v", err))
	}
	tb.Cleanup(func() {
		f.Close()
	})

	return f
}

type keyValues struct {
	key    string
	values []types.Value
}
