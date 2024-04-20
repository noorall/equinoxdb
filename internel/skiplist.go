package internel

import (
	"sync/atomic"
)

const (
	maxHeight = 20
	mask      = 3
)

type CloseHandler func()
type Comparator func([]byte, []byte) int

type Skiplist struct {
	head  *node
	arena *Arena

	height     int32
	ref        int32
	Hanlder    CloseHandler
	comparator Comparator
}

func NewSkiplist(arenaSize uint32, cmp Comparator) *Skiplist {
	if cmp == nil {
		panic("Unset the comparator for Skiplist!")
	}

	arena := NewArena(arenaSize)
	head := newNode(arena, nil, nil, maxHeight)

	return &Skiplist{head: head,
		arena:      arena,
		height:     1,
		ref:        1,
		comparator: cmp,
	}
}

func (s *Skiplist) Put(key, value []byte) {
	oldHeight := s.getHeight()
	var prev [maxHeight + 1]*node
	var next [maxHeight + 1]*node

	prev[oldHeight] = s.head
	next[oldHeight] = nil

	for i := int(oldHeight) - 1; i >= 0; i-- {
		prev[i], next[i] = s.getSplices(key, prev[i+1], i)
		// Found the exist key, overwrite directly
		if prev[i] == next[i] {
			prev[i].setValue(s.arena, value)
			return
		}
	}

	height := s.randomHeight()

	// CAS
	oldHeight = s.getHeight()
	for height > oldHeight {
		if atomic.CompareAndSwapInt32(&s.height, oldHeight, height) {
			break
		}

		oldHeight = s.getHeight()
	}

	x := newNode(s.arena, key, value, int(height))

	for i := 0; i < int(height); i++ {
		// CAS
		for {
			if prev[i] == nil {
				// Because the new height exceeds old height, may be there are added some new nodes
				// We can search from s.head and add some new splices
				prev[i], next[i] = s.getSplices(key, s.head, i)
			}

			nOffset := s.arena.getNodeOffset(next[i])
			x.tower[i] = nOffset
			// cas, try to add the new node at level
			if prev[i].setNextOffset(i, s.arena.getNodeOffset(x), nOffset) {
				// Insert the new node between prev[i] and next[i].
				// Go to the next level.
				break
			}

			// CAS failed, there are some other nodes inserted concurrently among this node inserting
			// So we need search splices for the node again
			prev[i], next[i] = s.getSplices(key, prev[i], i)
			if prev[i] == next[i] {
				prev[i].setValue(s.arena, value)
				return
			}
		}
	}
}

func (s *Skiplist) Get(key []byte) ([]byte, []byte) {
	node := s.findGreaterOrEqual(key)

	if node == nil {
		return key, nil
	}

	k := node.key(s.arena)

	return k, node.val(s.arena)
}

func (s *Skiplist) Empty() bool {
	return s.findLast() == nil
}

func (s *Skiplist) Size() uint32 {
	return s.arena.size()
}

func (s *Skiplist) Ref() {
	atomic.AddInt32(&s.ref, 1)
}

func (s *Skiplist) Deref() {
	n := atomic.AddInt32(&s.ref, -1)

	if n > 0 {
		return
	}

	if s.Hanlder != nil {
		s.Hanlder()
	}

	s.head = nil
	s.arena = nil
}

func (s *Skiplist) Iterator() *Iterator {
	s.Ref()
	return &Iterator{skl: s}
}

func (s *Skiplist) getSplices(key []byte, from *node, level int) (*node, *node) {
	for {
		next := s.getNext(from, level)
		if next == nil {
			return from, next
		}

		nextKey := next.key(s.arena)
		c := s.comparator(nextKey, key)

		if c < 0 {
			from = next
		} else if c > 0 {
			return from, next
		} else {
			return next, next
		}
	}
}

func (s *Skiplist) getNext(node *node, height int) *node {
	return s.arena.getNode(node.getNextOffset(height))
}

func (s *Skiplist) getHeight() int32 {
	return atomic.LoadInt32(&s.height)
}

// find the rightmost node such that key < target
func (s *Skiplist) findLessThan(target []byte) *node {
	curr, level := s.head, int(s.getHeight())-1

	for {
		next := s.getNext(curr, level)

		if next != nil && s.comparator(next.key(s.arena), target) < 0 {
			curr = next
		} else if level == 0 {
			if curr == s.head {
				return nil
			}

			return curr
		} else {
			level--
		}
	}
}

// find the leftmost node such that key >= target
func (s *Skiplist) findGreaterOrEqual(target []byte) *node {
	curr, level := s.head, int(s.getHeight())-1

	for {
		next := s.getNext(curr, level)

		if next != nil && s.comparator(next.key(s.arena), target) < 0 {
			curr = next
		} else if level == 0 {
			return next
		} else {
			level--
		}
	}
}

func (s *Skiplist) randomHeight() int32 {
	var h int32 = 1
	for h < maxHeight && (FastRand()&mask) == 0 {
		h++
	}

	return h
}

func (s *Skiplist) findLast() *node {
	curr := s.head
	level := int(s.getHeight()) - 1

	for {
		next := s.getNext(curr, level)

		if next != nil {
			curr = next
		} else if level == 0 {
			if curr == s.head {
				return nil
			}

			return curr
		} else {
			level--
		}
	}
}

type Iterator struct {
	skl *Skiplist
	n   *node
}

func (i *Iterator) Key() []byte {
	return i.n.key(i.skl.arena)
}

func (i *Iterator) Value() []byte {
	return i.n.val(i.skl.arena)
}

func (i *Iterator) Valid() bool {
	return i.n != nil
}

func (i *Iterator) Next() {
	if i.Valid() {
		i.n = i.skl.getNext(i.n, 0)
	}
}

func (i *Iterator) Prev() {
	if i.Valid() {
		i.n = i.skl.findLessThan(i.Key())
	}
}

func (i *Iterator) Seek(target []byte) {
	if i.Valid() {
		i.n = i.skl.findGreaterOrEqual(target)
	}
}

func (i *Iterator) SeekToFirst() {
	i.n = i.skl.getNext(i.skl.head, 0)
}

func (i *Iterator) SeekToLast() {
	i.n = i.skl.findLast()
}

func (i *Iterator) Close() error {
	i.skl.Deref()
	return nil
}
