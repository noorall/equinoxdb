package internel

import (
	"sync/atomic"
	"unsafe"
)

const (
	unit        = int(unsafe.Sizeof(uint32(0)))
	aligned     = int(unsafe.Sizeof(uint64(0))) - 1
	MaxNodeSize = int(unsafe.Sizeof(node{}))
)

type Arena struct {
	n   uint32
	buf []byte
}

func NewArena(n uint32) *Arena {
	return &Arena{n: 1, buf: make([]byte, n)}
}

func (a *Arena) addNode(height int) uint32 {
	reservedSize := (maxHeight - height) * unit

	len := uint32(MaxNodeSize - reservedSize + aligned)
	n := atomic.AddUint32(&a.n, len)

	return (n - len + uint32(aligned)) & ^uint32(aligned)
}

func (a *Arena) getNode(offset uint32) *node {
	if offset == 0 {
		return nil
	}

	return (*node)(unsafe.Pointer(&a.buf[offset]))
}

func (a *Arena) getNodeOffset(node *node) uint32 {
	if node == nil {
		return 0
	}

	return uint32(uintptr(unsafe.Pointer(node)) - uintptr(unsafe.Pointer(&a.buf[0])))
}

func (a *Arena) put(d []byte) uint32 {
	len := uint32(len(d))
	end := atomic.AddUint32(&a.n, len)

	start := end - len
	copy(a.buf[start:end], d)

	return start
}

func (a *Arena) getKey(offset uint32, size uint16) []byte {
	return a.buf[offset : offset+uint32(size)]
}
func (a *Arena) getValue(offset uint32, size uint32) []byte {
	return a.buf[offset : offset+size]
}

func (a *Arena) size() uint32 {
	return atomic.LoadUint32(&a.n)
}

type node struct {
	value uint64

	keyOffset uint32
	keySize   uint16

	height uint16

	tower [maxHeight]uint32
}

func newNode(arena *Arena, key, value []byte, height int) *node {
	offset := arena.addNode(height)
	node := arena.getNode(offset)

	node.value = encodeValue(arena.put(value), uint32(len(value)))
	node.keyOffset = arena.put(key)
	node.keySize = uint16(len(key))
	node.height = uint16(height)

	return node
}

func (n *node) getNextOffset(height int) uint32 {
	return atomic.LoadUint32(&n.tower[height])
}

func (n *node) setNextOffset(height int, val, old uint32) bool {
	return atomic.CompareAndSwapUint32(&n.tower[height], old, val)
}

func (n *node) getValueOffset() (valOffset uint32, valSize uint32) {
	val := atomic.LoadUint64(&n.value)
	return decodeValue(val)
}

func (n *node) key(arena *Arena) []byte {
	return arena.getKey(n.keyOffset, n.keySize)
}

func (n *node) val(arena *Arena) []byte {
	valOffset, valSize := n.getValueOffset()
	return arena.getValue(valOffset, valSize)
}

func (n *node) setValue(arena *Arena, value []byte) {
	valOffset := arena.put(value)
	val := encodeValue(valOffset, uint32(len(value)))
	atomic.StoreUint64(&n.value, val)
}

func encodeValue(valOffset uint32, valSize uint32) uint64 {
	return uint64(valSize)<<32 | uint64(valOffset)
}

func decodeValue(val uint64) (valOffset uint32, valSize uint32) {
	valOffset = uint32(val)
	valSize = uint32(val >> 32)

	return
}
