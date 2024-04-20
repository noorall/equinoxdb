package equinox

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSnapshotBasic(t *testing.T) {
	sl := newSnapshotList()
	require.True(t, sl.Empty())

	s := sl.New(1)
	require.False(t, sl.Empty())
	require.Equal(t, uint64(1), s.Version())
	require.Equal(t, s, sl.Newest())
	require.Equal(t, s, sl.Oldest())

	sl.Delete(s)
	require.True(t, sl.Empty())
}

func TestSnapshotCreateWithSameVersion(t *testing.T) {
	sl := newSnapshotList()
	require.True(t, sl.Empty())

	oldest := sl.New(1)
	sl.New(2)
	sl.New(4)
	newest := sl.New(4)

	require.False(t, sl.Empty())
	require.Equal(t, newest, sl.Newest())
	require.Equal(t, oldest, sl.Oldest())
}

func TestSnapshotCreateAndDelete(t *testing.T) {
	sl := newSnapshotList()
	require.True(t, sl.Empty())

	oldest := sl.New(1)
	s := sl.New(2)
	sl.New(4)
	sl.Delete(s)
	sl.New(4)
	newest := sl.New(7)
	sl.Delete(newest)
	newest = sl.New(6)

	require.False(t, sl.Empty())
	require.Equal(t, newest, sl.Newest())
	require.Equal(t, oldest, sl.Oldest())
}
