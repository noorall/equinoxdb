package equinox

import (
	"bytes"
	"equinox/internel"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"sort"
	"sync/atomic"
	"unsafe"
)

type fileEntryHeader struct {
	overlap uint16
	diff    uint16
}

const fileEntryHeaderSize = uint16(unsafe.Sizeof(fileEntryHeader{}))

// Encode encodes the header.
func (h fileEntryHeader) Encode() []byte {
	var b [4]byte
	*(*fileEntryHeader)(unsafe.Pointer(&b[0])) = h
	return b[:]
}

// Decode decodes the header.
func (h *fileEntryHeader) Decode(buf []byte) {
	copy(((*[fileEntryHeaderSize]byte)(unsafe.Pointer(h))[:]), buf[:fileEntryHeaderSize])
}

type fileBlock struct {
	data         []byte
	baseKey      []byte   // Base key for the current block.
	entryOffsets []uint32 // Offsets of entries present in current block.
	end          int      // Points to the end offset of the block.
}

type fileBlocks struct {
	blockList []*fileBlock
	index     []byte
	checksum  []byte
	size      int
}

func (fb *fileBlocks) Copy(dst []byte) int {
	written := 0

	for _, b := range fb.blockList {
		written += copy(dst[written:], b.data[:b.end])
	}

	written += copy(dst[written:], fb.index)
	written += copy(dst[written:], internel.U32ToBytes(uint32(len(fb.index))))

	written += copy(dst[written:], fb.checksum)
	written += copy(dst[written:], internel.U32ToBytes(uint32(len(fb.checksum))))

	return written
}

var NumMemBlocks int32

type memBlock struct {
	offset            int
	data              []byte
	checksum          []byte
	entriesIndexStart int      // start index of entryOffsets list
	entryOffsets      []uint32 // used to binary search an entry in the block.
	chkLen            int      // checksum length.
	ref               int32
}

func (b *memBlock) incrRef() bool {
	for {
		ref := atomic.LoadInt32(&b.ref)
		if ref == 0 {
			return false
		}

		if atomic.CompareAndSwapInt32(&b.ref, ref, ref+1) {
			return true
		}
	}
}

func (b *memBlock) decrRef() {
	if b == nil {
		return
	}

	if atomic.AddInt32(&b.ref, -1) == 0 {
		atomic.AddInt32(&NumMemBlocks, -1)
	}
}

func (b *memBlock) verifyCheckSum() error {
	actual := crc32.Checksum(b.data, internel.CastagnoliCrcTable)
	expected := internel.BytesToU32(b.checksum)
	if actual != expected {
		return Wrapf(ErrChecksumMismatch, "actual: %d, expected: %d", actual, expected)
	}

	return nil
}

/*
Structure of Block.
+-------------------+---------------------+--------------------+--------------+------------------+
| Entry1            | Entry2              | Entry3             | Entry4       | Entry5           |
+-------------------+---------------------+--------------------+--------------+------------------+
| Entry6            | ...                 | ...                | ...          | EntryN           |
+-------------------+---------------------+--------------------+--------------+------------------+
| Block Meta(contains list of offsets used| Block Meta Size    | Block        | Checksum Size    |
| to perform binary search in the block)  | (4 Bytes)          | Checksum     | (4 Bytes)        |
+-----------------------------------------+--------------------+--------------+------------------+
*/
type blockBuilder struct {
	curBlock *fileBlock

	compressedSize   uint32
	uncompressedSize uint32

	lenOffsets uint32
	keyHashes  []uint32 // Used for building the bloomfilter.
	opts       *Options
	maxVersion uint64
	onDiskSize uint32
}

func (b *blockBuilder) Add(key []byte, value EValue, valueLen uint32) *fileBlock {
	var block *fileBlock
	if b.isFullWith(key, value) {
		block = b.Finish()
	}

	b.keyHashes = append(b.keyHashes, internel.Hash(ParseKey(key)))
	if version := ParseVersion(key); version > b.maxVersion {
		b.maxVersion = version
	}

	// calculate key's difference
	var diffKey []byte
	if len(b.curBlock.baseKey) == 0 {
		// deep copy
		b.curBlock.baseKey = append(b.curBlock.baseKey[:0], key...)
		diffKey = key
	} else {
		diffKey = b.diff(key)
	}

	AssertTrue(len(key)-len(diffKey) <= math.MaxUint16)
	AssertTrue(len(diffKey) <= math.MaxUint16)

	// storage current entry's offset
	b.curBlock.entryOffsets = append(b.curBlock.entryOffsets, uint32(b.curBlock.end))
	// append entry header
	h := &fileEntryHeader{
		overlap: uint16(len(key) - len(diffKey)),
		diff:    uint16(len(diffKey)),
	}
	b.append(h.Encode())

	// append diffkey
	b.append(diffKey)

	//append the value
	b.append(value.Encode())

	b.onDiskSize += valueLen

	return block
}

func (b *blockBuilder) Empty() bool {
	return len(b.keyHashes) == 0
}

func (b *blockBuilder) Finish() *fileBlock {
	b.finishBlock()

	old := b.curBlock
	b.curBlock = &fileBlock{}

	return old
}

func (b *blockBuilder) isFullWith(key []byte, value EValue) bool {
	if b.Empty() {
		return false
	}

	AssertTrue((uint32(len(b.curBlock.entryOffsets))+1)*4+4+4+4 < math.MaxUint32)
	entriesOffsetsSize := uint32((len(b.curBlock.entryOffsets)+1)*4 +
		4 + // size of list
		4 + // crc32 checksum
		4) // checksum length

	estimatedSize := uint32(b.curBlock.end) + uint32(6) +
		uint32(len(key)) + value.EncodedSize() + entriesOffsetsSize

	AssertTrue(uint64(b.curBlock.end)+uint64(estimatedSize) < math.MaxUint32)

	return estimatedSize > uint32(b.opts.BlockSize)
}

func (b *blockBuilder) finishBlock() {
	if b.Empty() {
		return
	}

	// Append the entryOffsets and its length.
	b.append(internel.U32SliceToBytes(b.curBlock.entryOffsets))
	b.append(internel.U32ToBytes(uint32(len(b.curBlock.entryOffsets))))

	// Append checksum and its length.
	checkSum := b.calculateCheckSum(b.curBlock.data[:b.curBlock.end])
	b.append(checkSum)
	b.append(internel.U32ToBytes(uint32(len(checkSum))))

	atomic.AddUint32(&b.uncompressedSize, uint32(b.curBlock.end))
	b.lenOffsets += uint32(int(math.Ceil(float64(len(b.curBlock.baseKey))/4))*4) + 12
}

func (b *blockBuilder) allocate(sz int) []byte {
	fb := b.curBlock
	if len(fb.data[fb.end:]) < sz {
		// reallocate and double the size
		nsz := 2 * len(fb.data)
		if fb.end+sz > nsz {
			nsz = fb.end + sz
		}

		tmp := make([]byte, nsz)
		copy(tmp, fb.data)
		fb.data = tmp
	}

	fb.end += sz
	return fb.data[fb.end-sz : fb.end]
}

func (b *blockBuilder) append(data []byte) {
	dst := b.allocate(len(data))
	copy(dst, data)
}

func (b *blockBuilder) calculateCheckSum(data []byte) []byte {
	checkSum := crc32.Checksum(data, internel.CastagnoliCrcTable)
	return internel.U32ToBytes(checkSum)
}

func (b *blockBuilder) diff(nKey []byte) []byte {
	i := 0
	for ; i < len(nKey) && i < len(b.curBlock.baseKey); i++ {
		if nKey[i] != b.curBlock.baseKey[i] {
			break
		}
	}

	return nKey[i:]
}

type blockIterator struct {
	curBlock     *memBlock
	tableId      int
	blockId      int
	curEntryId   int
	data         []byte
	baseKey      []byte
	key          []byte
	value        []byte
	entryOffsets []uint32

	err     error
	overlap uint16 // the overlap of the previous key with the base key.

	comparator Comparator
}

func NewBlockIterator(opt *Options, b *memBlock, tid, bid int) *blockIterator {
	iter := blockIterator{
		curBlock: b,
		tableId:  tid,
		blockId:  bid,
	}

	iter.data = b.data[:b.entriesIndexStart]
	iter.entryOffsets = b.entryOffsets
	iter.comparator = opt.comparator

	return &iter
}

type seekMode int

const (
	restart seekMode = iota
	current
)

// Seek the first entry that is >= input key from start
func (iter *blockIterator) Seek(key []byte) {
	iter.seekFrom(key, restart)
}

// Seek the first entry that is >= input key from pos (may be start or last pos)
func (iter *blockIterator) seekFrom(key []byte, mode seekMode) {
	iter.err = nil
	start := 0
	if mode != restart {
		start = iter.curEntryId
	}

	// find the first key of entry that equal or greater than input key
	found := sort.Search(len(iter.entryOffsets), func(i int) bool {
		if i < start {
			return false
		}

		iter.readEntryById(i)
		return iter.comparator(iter.key, key) >= 0
	})

	iter.readEntryById(found)
}

func (iter *blockIterator) SeekToFirst() {
	iter.readEntryById(0)
}

func (iter *blockIterator) SeekToLast() {
	iter.readEntryById(len(iter.entryOffsets) - 1)
}

func (iter *blockIterator) Next() {
	iter.readEntryById(iter.curEntryId + 1)
}

func (iter *blockIterator) Prev() {
	iter.readEntryById(iter.curEntryId - 1)
}

func (iter *blockIterator) Key() []byte {
	return iter.key
}

func (iter *blockIterator) Value() (ret EValue) {
	ret.Decode(iter.value)
	return
}

func (iter *blockIterator) Error() error {
	return iter.err
}

func (iter *blockIterator) Close() {
	iter.curBlock.decrRef()
}

func (iter *blockIterator) Valid() bool {
	return iter.curBlock != nil && iter.err == nil
}

func (iter *blockIterator) readEntryById(eid int) {
	iter.curEntryId = eid
	if eid >= len(iter.entryOffsets) || eid < 0 {
		iter.err = io.EOF
		return
	}

	iter.err = nil
	// lazy init the block baseKey
	if len(iter.baseKey) == 0 {
		var header fileEntryHeader
		header.Decode(iter.data)
		iter.baseKey = iter.data[fileEntryHeaderSize : fileEntryHeaderSize+header.diff]
	}

	startOffset := int(iter.entryOffsets[eid])
	endOffset := len(iter.data)
	// eid points to the last entry in the block.
	if eid+1 < len(iter.entryOffsets) {
		// eid point to some entry other than the last one in the block.
		// EndOffset of the current entry is the start offset of the next entry.
		endOffset = int(iter.entryOffsets[eid+1])
	}

	// just for debug
	defer func() {
		if r := recover(); r != nil {
			var debugBuf bytes.Buffer
			fmt.Fprintf(&debugBuf, "==== Recovered====\n")
			fmt.Fprintf(&debugBuf, "Table ID: %d\nBlock ID: %d\nEntry Idx: %d\nData len: %d\n"+
				"StartOffset: %d\nEndOffset: %d\nEntryOffsets len: %d\nEntryOffsets: %v\n",
				iter.tableId, iter.blockId, iter.curEntryId, len(iter.data),
				startOffset, endOffset, len(iter.entryOffsets), iter.entryOffsets)
			panic(debugBuf.String())
		}
	}()

	entryData := iter.data[startOffset:endOffset]
	var header fileEntryHeader
	header.Decode(entryData)
	// optimize for copy, if current overlap less than prev overlap,
	// than (iter.overlap - header.overlap) byte don't need to copy
	// if there are n entries that overlap after greater overlap,
	// than n * (iter.overlap - header.overlap) copy can be saved.
	if header.overlap > iter.overlap {
		iter.key = append(iter.key[:iter.overlap], iter.baseKey[iter.overlap:header.overlap]...)
	}

	iter.overlap = header.overlap
	valueOffset := fileEntryHeaderSize + header.diff
	diffKey := entryData[fileEntryHeaderSize:valueOffset]
	iter.key = append(iter.key[:header.overlap], diffKey...)
	iter.value = entryData[valueOffset:]
}
