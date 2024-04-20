//go:build !cgo
// +build !cgo

package internel

import (
	"errors"
)

// CgoEnabled is used to check if CGO is enabled while building badger.
const CgoEnabled = false

var ErrZstdCgo = errors.New("zstd compression requires building with cgo enabled")

// ZSTDDecompress decompresses a block using ZSTD algorithm.
func ZSTDDecompress(dst, src []byte) ([]byte, error) {
	return nil, ErrZstdCgo
}

// ZSTDCompress compresses a block using ZSTD algorithm.
func ZSTDCompress(dst, src []byte, compressionLevel int) ([]byte, error) {
	return nil, ErrZstdCgo
}

// ZSTDCompressBound returns the worst case size needed for a destination buffer.
func ZSTDCompressBound(srcSize int) int {
	panic(ErrZstdCgo)
}
