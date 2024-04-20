package internel

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const size = 1 << 20

func (s *Skiplist) refCount() int32 {
	return s.ref
}

func getBytes(val int, size int) []byte {
	f := fmt.Sprintf("%%0%dd", size)
	return []byte(fmt.Sprintf(f, val))
}

func length(s *Skiplist) int {
	x := s.getNext(s.head, 0)
	cnt := 0

	for x != nil {
		cnt++
		x = s.getNext(x, 0)
	}

	return cnt
}

func random(rnd *rand.Rand) []byte {
	res := make([]byte, 8)
	k1 := rnd.Uint32()
	k2 := rnd.Uint32()

	binary.LittleEndian.PutUint32(res, k1)
	binary.LittleEndian.PutUint32(res[4:], k2)

	return res
}

func comparator(a, b []byte) int {
	return bytes.Compare(a, b)
}

// TestEmpty tests a empty skiplist
func TestEmpty(t *testing.T) {
	key := []byte("test")
	skl := NewSkiplist(size, comparator)

	require.True(t, skl.getHeight() == 1)
	_, v := skl.Get(key)
	require.True(t, v == nil)
	require.True(t, skl.findLessThan(key) == nil)
	require.True(t, skl.findGreaterOrEqual(key) == nil)
	require.True(t, skl.findLast() == nil)
	require.Equal(t, skl.refCount(), int32(1))

	it := skl.Iterator()
	require.Equal(t, skl.refCount(), int32(2))
	require.False(t, it.Valid())

	it.SeekToFirst()
	require.False(t, it.Valid())

	it.SeekToLast()
	require.False(t, it.Valid())

	it.Seek(key)
	require.False(t, it.Valid())

	it.Close()
	require.Equal(t, skl.refCount(), int32(1))

	skl.Deref()
	require.Equal(t, skl.refCount(), int32(0))

}

// TestFindOperations tests findLast, findLessThan and findGreatOrEqual
func TestFindOperations(t *testing.T) {
	skl := NewSkiplist(size, comparator)

	// clear, gc
	defer skl.Deref()

	for i := 0; i < 1000; i++ {
		skl.Put(getBytes(i*10, 5), getBytes(i, 5))
	}

	k := getBytes(0, 5)
	node := skl.findLessThan(k)
	require.Nil(t, node)

	node = skl.findGreaterOrEqual(k)
	require.NotNil(t, node)
	require.EqualValues(t, getBytes(0, 5), node.val(skl.arena))

	k = getBytes(5000, 5)
	node = skl.findLessThan(k)
	require.NotNil(t, node)
	require.EqualValues(t, getBytes(499, 5), node.val(skl.arena))

	node = skl.findGreaterOrEqual(k)
	require.NotNil(t, node)
	require.EqualValues(t, getBytes(500, 5), node.val(skl.arena))

	k = getBytes(10000, 5)
	node = skl.findLessThan(k)
	require.NotNil(t, node)
	require.EqualValues(t, getBytes(999, 5), node.val(skl.arena))

	node = skl.findGreaterOrEqual(k)
	require.Nil(t, node)

	k = getBytes(5005, 5)
	node = skl.findLessThan(k)
	require.NotNil(t, node)
	require.EqualValues(t, getBytes(500, 5), node.val(skl.arena))

	node = skl.findGreaterOrEqual(k)
	require.NotNil(t, node)
	require.EqualValues(t, getBytes(501, 5), node.val(skl.arena))

	node = skl.findLast()
	require.NotNil(t, node)
	require.EqualValues(t, getBytes(999, 5), node.val(skl.arena))
}

// TestIterator tests SeekToFirst(), SeekToLast(), Seek(), Next() and Prev()
func TestIterator(t *testing.T) {
	skl := NewSkiplist(size, comparator)
	defer skl.Deref()

	iter := skl.Iterator()
	defer iter.Close()

	require.False(t, iter.Valid())
	iter.SeekToFirst()
	require.False(t, iter.Valid())
	iter.SeekToLast()
	require.False(t, iter.Valid())

	const n = 1000
	for i := 0; i < n; i++ {
		skl.Put(getBytes(i*10, 5), getBytes(i, 5))
	}

	iter.SeekToFirst()
	require.True(t, iter.Valid())

	// Next() test
	for i := 0; i < n; i++ {
		require.True(t, iter.Valid())
		require.EqualValues(t, getBytes(i, 5), iter.Value())
		iter.Next()
	}
	require.False(t, iter.Valid())

	// SeekToLast() and Prev() test
	iter.SeekToLast()
	require.True(t, iter.Valid())
	require.EqualValues(t, getBytes(999, 5), iter.Value())
	for i := n - 1; i >= 0; i-- {
		require.True(t, iter.Valid())
		require.EqualValues(t, getBytes(i, 5), iter.Value())
		iter.Prev()
	}
	require.False(t, iter.Valid())

	// SeekToFirst() test
	iter.SeekToFirst()
	require.True(t, iter.Valid())
	require.EqualValues(t, getBytes(0, 5), iter.Value())

	// Seek() test
	k := getBytes(0, 5)
	iter.Seek(k)
	require.True(t, iter.Valid())
	require.EqualValues(t, getBytes(0, 5), iter.Value())

	k = getBytes(5000, 5)
	iter.Seek(k)
	require.True(t, iter.Valid())
	require.EqualValues(t, getBytes(500, 5), iter.Value())

	k = getBytes(5001, 5)
	iter.Seek(k)
	require.True(t, iter.Valid())
	require.EqualValues(t, getBytes(501, 5), iter.Value())

	k = getBytes(9990, 5)
	iter.Seek(k)
	require.True(t, iter.Valid())
	require.EqualValues(t, getBytes(999, 5), iter.Value())

	k = getBytes(10000, 5)
	iter.Seek(k)
	require.False(t, iter.Valid())
}

// TestSequential test a single thread puts, updates and gets
func TestSequential(t *testing.T) {
	skl := NewSkiplist(size, comparator)

	k1 := getBytes(1, 5)
	k2 := getBytes(2, 5)
	k3 := getBytes(3, 5)
	k4 := getBytes(4, 5)

	v1 := getBytes(100, 10)
	v2 := getBytes(101, 10)
	v3 := getBytes(102, 10)
	v4 := getBytes(103, 10)

	skl.Put(k1, v1)
	skl.Put(k2, v2)
	skl.Put(k3, v3)

	_, v := skl.Get(k4)
	require.Nil(t, v)

	_, v = skl.Get(k1)
	require.NotNil(t, v)
	require.EqualValues(t, "0000000100", v)

	_, v = skl.Get(k2)
	require.NotNil(t, v)
	require.EqualValues(t, "0000000101", v2)

	_, v = skl.Get(k3)
	require.NotNil(t, v)
	require.EqualValues(t, "0000000102", v3)

	skl.Put(k3, v4)
	_, v = skl.Get(k3)
	require.NotNil(t, v)
	require.EqualValues(t, "0000000103", v4)

	v5 := getBytes(104, 1024*512) // 0.5 MB
	skl.Put(k4, v5)
	_, v = skl.Get(k4)
	require.NotNil(t, v)
	require.EqualValues(t, v5, v)
}

// TestConcurrent tests concurrent writes followed by concurrent reads
func TestConcurrent(t *testing.T) {
	const n = 1000
	var wg sync.WaitGroup

	skl := NewSkiplist(size, comparator)
	defer skl.Deref()

	// concurrent writes
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			skl.Put(getBytes(i, 5), getBytes(i, 5))
		}(i)
	}
	wg.Wait()

	// concurrent reads
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, v := skl.Get(getBytes(i, 5))
			require.NotNil(t, v)
			require.EqualValues(t, getBytes(i, 5), v)
		}(i)
	}
	wg.Wait()

	require.Equal(t, n, length(skl))
}

// BenchmarkWrites tests the performance of concurrent writes
func BenchmarkWrites(b *testing.B) {
	skl := NewSkiplist(uint32((b.N+1)*MaxNodeSize), comparator)
	defer skl.Deref()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
		v := random(rnd)
		for pb.Next() {
			skl.Put(random(rnd), v)
		}
	})
}

// BenchmarkReadWrite tests the performance of concurrent reads and writes mixes for skiplist
func BenchmarkReadWrite(b *testing.B) {
	for i := 0; i < 11; i++ {
		writeFrac := float64(i) / 10.0

		b.Run(fmt.Sprintf("wfrac_%d", i), func(b *testing.B) {
			skl := NewSkiplist(uint32((b.N+1)*MaxNodeSize), comparator)
			defer skl.Deref()

			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
				val := random(rnd)
				for pb.Next() {
					if rnd.Float64() < writeFrac {
						skl.Put(random(rnd), val)
					} else {
						skl.Get(random(rnd))
					}
				}
			})
		})
	}
}

// BenchmarkReadWriteMap tests the performance of concurrent reads and writes mixes for go map with mutex.
// As a reference for the performance of skiplist
func BenchmarkReadWriteMap(b *testing.B) {
	for i := 0; i < 11; i++ {
		writeFrac := float64(i) / 10.0

		b.Run(fmt.Sprintf("wfrac_%d", i), func(b *testing.B) {
			m := make(map[string][]byte)
			var mutex sync.RWMutex

			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
				val := random(rnd)
				for pb.Next() {
					if rnd.Float64() < writeFrac {
						mutex.Lock()
						m[string(random(rnd))] = val
						mutex.Unlock()
					} else {
						mutex.Lock()
						_ = m[string(random(rnd))]
						mutex.Unlock()
					}
				}
			})
		})
	}
}
