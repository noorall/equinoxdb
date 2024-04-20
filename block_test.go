package equinox

import (
	"equinox/internel"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBlockBuilderBasic(t *testing.T) {
	opts := Options{BlockSize: 1024}
	builder := blockBuilder{opts: &opts, curBlock: &fileBlock{}}

	key := KeyWithVersion([]byte("key"), uint64(1))
	value := EValue{Meta: Value, Value: []byte("value12345")}

	blk := builder.Add(key, value, uint32(len(value.Value)))
	require.Nil(t, blk)

	require.False(t, builder.Empty())
	require.Equal(t, 1, len(builder.keyHashes))
	require.Equal(t, internel.Hash(ParseKey(key)), builder.keyHashes[0])
	require.EqualValues(t, key, builder.curBlock.baseKey)
	require.Equal(t, 1, len(builder.curBlock.entryOffsets))
	require.EqualValues(t, uint32(0), builder.curBlock.entryOffsets[0])

	require.Equal(t, uint64(1), builder.maxVersion)
	require.Equal(t, uint32(0), builder.lenOffsets)
	require.Equal(t, uint32(len(value.Value)), builder.onDiskSize)

	expected := []byte{}
	// header
	h := fileEntryHeader{
		overlap: uint16(0),
		diff:    uint16(len(key)),
	}
	expected = append(expected, h.Encode()...)
	// key
	expected = append(expected, key...)
	// value
	vBytes := value.Encode()
	expected = append(expected, vBytes...)
	require.Equal(t, 4+len(key)+len(vBytes), builder.curBlock.end)
	require.EqualValues(t, expected, builder.curBlock.data[:builder.curBlock.end])
}

func TestBlockBuilderFull(t *testing.T) {
	opts := Options{BlockSize: 1024}
	builder := blockBuilder{opts: &opts, curBlock: &fileBlock{}}

	version := uint64(1)
	var key []byte
	var value EValue
	for i := 1; i < 1000; i++ {
		if i%10 == 0 {
			version++
		}

		key = KeyWithVersion(getKey(i), version)
		value = EValue{Meta: Value, Value: getValue(i * 10)}
		if builder.isFullWith(key, value) {
			break
		}

		require.Nil(t, builder.Add(key, value, 0))
	}

	require.True(t, builder.curBlock.end < opts.BlockSize)
	require.Equal(t, version, builder.maxVersion)

	beforeSize := builder.curBlock.end

	blk := builder.Add(key, value, 0)
	require.NotNil(t, blk)
	require.NotEqual(t, blk, builder.curBlock)
	require.Equal(t, builder.uncompressedSize, uint32(blk.end))
	expected := beforeSize + len(blk.entryOffsets)*4 + 4 + 4 + 4
	require.Equal(t, expected, blk.end)
}

func TestDiffKey(t *testing.T) {
	key1 := KeyWithVersion([]byte("key"), uint64(1))
	value1 := EValue{Meta: Value, Value: []byte("value12345")}

	key2 := KeyWithVersion([]byte("key"), uint64(10))
	value2 := EValue{Meta: Value, Value: []byte("value67891")}

	opts := Options{BlockSize: 1024}
	builder := blockBuilder{opts: &opts, curBlock: &fileBlock{}}
	builder.Add(key1, value1, 0)
	builder.Add(key2, value2, 0)
	require.Equal(t, 2, len(builder.curBlock.entryOffsets))

	var header fileEntryHeader

	start := builder.curBlock.entryOffsets[1]
	header.Decode(builder.curBlock.data[start : start+4])

	require.Equal(t, uint16(10), header.overlap)
	require.Equal(t, uint16(1), header.diff)
	require.EqualValues(t, []byte{10}, builder.curBlock.data[start+4:start+4+uint32(header.diff)])
}

func TestEmptyBuilder(t *testing.T) {
	opts := Options{BlockSize: 1024}
	builder := blockBuilder{opts: &opts, curBlock: &fileBlock{}}
	require.True(t, builder.Empty())
	require.Equal(t, fileBlock{}, *builder.Finish())
}
