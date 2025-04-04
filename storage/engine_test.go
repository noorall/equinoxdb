package storage

import (
	"context"
	"equinox/storage/config"
	"equinox/storage/cursor"
	"equinox/storage/types"
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"testing"
	"time"
)

func getTestDB(t *testing.T) *Engine {
	dir, err := ioutil.TempDir("/Users/noorall/GolandProjects/equinox/benchmark/write/db", "equinox-test")
	require.NoError(t, err)

	options := config.NewOption()
	options.Dir = dir
	db, err := NewEngine(options)
	require.NoError(t, err)
	return db
}

func TestWrite(t *testing.T) {
	e := getTestDB(t)
	_ = e.Open(context.Background())
	data := "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz--\n"
	fields := types.Fields{
		"field1": data,
		"field4": data,
	}
	for i := 0; i < 1000; i++ {
		p, _ := types.NewPoint("hh", fields, time.Now(), types.LifeCycleDefault)
		err := e.Write(p)
		require.NoError(t, err)
	}
	err := e.Close()
	require.NoError(t, err)
}

func TestWriteBatch(t *testing.T) {
	e := getTestDB(t)
	_ = e.Open(context.Background())
	data := "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz--\n" // 64 bytes
	fields := types.Fields{
		"field1": data,
		"field4": data,
	}
	var points []types.Point
	for i := 0; i < 1000; i++ {
		p, _ := types.NewPoint("hh", fields, time.Now(), types.LifeCycleDefault)
		points = append(points, p)
		if len(points) == 100 {
			err := e.WriteBatch(points)
			require.NoError(t, err)
			points = points[:0]
		}
	}
	err := e.Close()
	require.NoError(t, err)
}

func TestRead(t *testing.T) {
	e := getTestDB(t)
	_ = e.Open(context.Background())
	var startTimes []int64
	var endTimes []int64
	for i := 0; i < 1000; i++ {
		startTimes = append(startTimes, time.Now().UnixNano())
		p, _ := types.NewPoint("hh", types.Fields{"field1": i}, time.Now(), types.LifeCycleDefault)
		err := e.Write(p)
		require.NoError(t, err)
		endTimes = append(endTimes, time.Now().UnixNano())
		time.Sleep(time.Millisecond)
	}
	for i := 0; i < len(startTimes); i++ {
		cur, err := e.Get([]byte("hh"), []byte("field1"), types.Integer, startTimes[i], endTimes[i], true)
		require.NoError(t, err)
		curi := cur.(cursor.IntegerArrayCursor)
		arr := curi.Next()
		require.Equal(t, 1, arr.Len())
		require.Equal(t, []int64{int64(i)}, arr.Values)
	}
	for i := 0; i < len(startTimes); i++ {
		cur, err := e.Get([]byte("hh"), []byte("field1"), types.Integer, startTimes[i], endTimes[i], false)
		require.NoError(t, err)
		curi := cur.(cursor.IntegerArrayCursor)
		arr := curi.Next()
		require.Equal(t, 1, arr.Len())
		require.Equal(t, []int64{int64(i)}, arr.Values)
	}
	err := e.Close()
	require.NoError(t, err)
}

func TestDelete(t *testing.T) {
	e := getTestDB(t)
	_ = e.Open(context.Background())
	var startTimes []int64
	var endTimes []int64
	for i := 0; i < 1000; i++ {
		startTimes = append(startTimes, time.Now().UnixNano())
		p, _ := types.NewPoint("hh", types.Fields{"field1": i}, time.Now(), types.LifeCycleDefault)
		err := e.Write(p)
		require.NoError(t, err)
		endTimes = append(endTimes, time.Now().UnixNano())
		time.Sleep(time.Millisecond)
	}
	err := e.DeleteRange([]byte("hh"), startTimes[0], endTimes[5])
	require.NoError(t, err)
	cur, err := e.Get([]byte("hh"), []byte("field1"), types.Integer, startTimes[0], endTimes[5], true)
	require.NoError(t, err)
	curi := cur.(cursor.IntegerArrayCursor)
	arr := curi.Next()
	require.Equal(t, 0, arr.Len())

	err = e.Delete([]byte("hh"))
	require.NoError(t, err)
	cur, err = e.Get([]byte("hh"), []byte("field1"), types.Integer, startTimes[0], endTimes[999], true)
	require.NoError(t, err)
	curi = cur.(cursor.IntegerArrayCursor)
	arr = curi.Next()
	require.Equal(t, 0, arr.Len())
	err = e.Close()
	require.NoError(t, err)
}
