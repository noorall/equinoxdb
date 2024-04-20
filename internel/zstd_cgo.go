//go:build cgo
// +build cgo

package internel

import (
	"github.com/DataDog/zstd"
)

// CgoEnabled is used to check if CGO is enabled while building badger.
const CgoEnabled = true

// ZSTDDecompress decompresses a block using ZSTD algorithm.
func ZSTDDecompress(dst, src []byte) ([]byte, error) {
	return zstd.Decompress(dst, src)
}

// ZSTDCompress compresses a block using ZSTD algorithm.
func ZSTDCompress(dst, src []byte, compressionLevel int) ([]byte, error) {
	return zstd.CompressLevel(dst, src, compressionLevel)
}

// ZSTDCompressBound returns the worst case size needed for a destination buffer.
func ZSTDCompressBound(srcSize int) int {
	return zstd.CompressBound(srcSize)
}
