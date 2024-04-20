package internel

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func bloomFilterString(bf BloomFilter) string {
	s := make([]byte, 8*len(bf))

	for i, v := range bf {
		for j := 0; j < 8; j++ {
			idx := 8*i + j
			if v&(1<<uint(j)) != 0 {
				s[idx] = '1'
			} else {
				s[idx] = '.'
			}
		}
	}

	return string(s)
}

func TestHash(t *testing.T) {
	// The magic want numbers come from running the C++ leveldb code in hash.cc.
	testCases := []struct {
		s    string
		want uint32
	}{
		{"", 0xbc9f1d34},
		{"g", 0xd04a8bda},
		{"go", 0x3e0b0745},
		{"gop", 0x0c326610},
		{"goph", 0x8c9d6390},
		{"gophe", 0x9bfd4b0a},
		{"gopher", 0xa78edc7c},
		{"I had a dream it would end this way.", 0xe14a9db9},
	}
	for _, tc := range testCases {
		require.Equal(t, tc.want, Hash([]byte(tc.s)))
	}
}

func TestBloomFilterBasic(t *testing.T) {
	var hashes []uint32
	keys := [][]byte{
		[]byte("hello"),
		[]byte("world"),
	}
	for _, key := range keys {
		hashes = append(hashes, Hash(key))
	}

	bitsPerKey := BloomBitsPerKey(2, 0.01)
	require.Equal(t, 7, bitsPerKey)

	bf := NewBloomFilter(hashes, bitsPerKey)
	expect := "1.............1.........1.....1...1.........1...............1.....1....."
	actual := bloomFilterString(bf)

	require.Equal(t, expect, actual)

	m := map[string]bool{
		"hello":  true,
		"world":  true,
		"bloom":  false,
		"filter": false,
	}

	for k, v := range m {
		require.Equal(t, v, bf.MayContain([]byte(k)))
	}
}

func TestBloomFilterVarintLength(t *testing.T) {
	nextLength := func(x int) int {
		if x < 10 {
			return x + 1
		}
		if x < 100 {
			return x + 10
		}
		if x < 1000 {
			return x + 100
		}
		return x + 1000
	}
	le32 := func(i int) []byte {
		b := make([]byte, 4)
		b[0] = uint8(uint32(i) >> 0)
		b[1] = uint8(uint32(i) >> 8)
		b[2] = uint8(uint32(i) >> 16)
		b[3] = uint8(uint32(i) >> 24)
		return b
	}

	nMediocreFilters, nGoodFilters := 0, 0
	for length := 1; length <= 10000; length = nextLength(length) {
		keys := make([][]byte, 0, length)
		for i := 0; i < length; i++ {
			keys = append(keys, le32(i))
		}
		var hashes []uint32
		for _, key := range keys {
			hashes = append(hashes, Hash(key))
		}
		f := NewBloomFilter(hashes, 10)

		require.Truef(t, len(f) <= (length*10/8)+40,
			"length=%d: len(f)=%d is too large", length, len(f))

		// All added keys must match.
		for _, key := range keys {
			require.Truef(t, f.MayContain(key), "length=%d: did not contain key %q", length, key)
		}

		// Check false positive rate.
		nFalsePositive := 0
		for i := 0; i < 10000; i++ {
			if f.MayContain(le32(1e9 + i)) {
				nFalsePositive++
			}
		}
		require.Truef(t, nFalsePositive <= 0.02*10000,
			"length=%d: %d false positives in 10000", length, nFalsePositive)

		if nFalsePositive > 0.0125*10000 {
			nMediocreFilters++
		} else {
			nGoodFilters++
		}
	}

	require.True(t, nMediocreFilters <= nGoodFilters/5,
		"%d mediocre filters but only %d good filters", nMediocreFilters, nGoodFilters)
}
