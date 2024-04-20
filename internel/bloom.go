package internel

import "math"

const ln2 = math.Ln2

type BloomFilter []byte

// NewFilter returns a new Bloom filter that encodes a set of []byte keys with
// the given number of bits per key, approximately.
//
// A good bitsPerKey value is 10, which yields a filter with ~ 1% false
// positive rate.
func NewBloomFilter(keys []uint32, bitsPerKey int) BloomFilter {
	return BloomFilter(generateBloomFilter(keys, bitsPerKey))
}

func BloomBitsPerKey(numEntries int, fp float64) int {
	size := -1 * float64(numEntries) * math.Log(fp) / math.Pow(ln2, 2)
	locs := math.Ceil(ln2 * size / float64(numEntries))
	return int(locs)
}

// MayContain returns whether the filter may contain given key. False positives
// are possible, where it returns true for keys not in the original set.
func (bf BloomFilter) MayContain(key []byte) bool {
	return bf.MayContainHash(Hash(key))
}

func (bf BloomFilter) MayContainHash(hash uint32) bool {
	if len(bf) < 2 {
		return false
	}

	k := bf[len(bf)-1]
	if k > 30 {
		// This is reserved for potentially new encodings for short Bloom filters.
		// Consider it a match.
		return true
	}

	nBits := uint32(8 * (len(bf) - 1))
	delta := hash>>17 | hash<<15
	for j := uint8(0); j < k; j++ {
		bitPos := hash % nBits
		if bf[bitPos/8]&(1<<(bitPos%8)) == 0 {
			return false
		}

		hash += delta
	}

	return true
}

func generateBloomFilter(keys []uint32, bitsPerKey int) []byte {
	if bitsPerKey < 0 {
		bitsPerKey = 0
	}

	k := uint32(float64(bitsPerKey) * ln2)
	if k < 1 {
		k = 1
	}

	if k > 30 {
		k = 30
	}

	nBits := uint32(len(keys) * bitsPerKey)
	if nBits < 64 {
		nBits = 64
	}

	nBytes := (nBits + 7) / 8
	nBits = nBytes * 8
	buf := extend(nBytes + 1)
	for _, h := range keys {
		delta := h>>17 | h<<15
		for j := uint32(0); j < k; j++ {
			bitPos := h % nBits
			buf[bitPos/8] |= 1 << (bitPos % 8)
			h += delta
		}
	}

	buf[nBytes] = uint8(k)

	return buf
}

func extend(n uint32) []byte {
	var c uint32 = 1024 // 1 KB
	for c < n {
		c += c / 4
	}

	return make([]byte, n, c)
}

// Hash implements a hashing algorithm similar to the Murmur hash.
func Hash(b []byte) uint32 {
	const (
		seed = 0xbc9f1d34
		m    = 0xc6a4a793
	)
	h := uint32(seed) ^ uint32(len(b))*m
	for ; len(b) >= 4; b = b[4:] {
		h += uint32(b[0]) | uint32(b[1])<<8 | uint32(b[2])<<16 | uint32(b[3])<<24
		h *= m
		h ^= h >> 16
	}
	switch len(b) {
	case 3:
		h += uint32(b[2]) << 16
		fallthrough
	case 2:
		h += uint32(b[1]) << 8
		fallthrough
	case 1:
		h += uint32(b[0])
		h *= m
		h ^= h >> 24
	}

	return h
}
