package equinox

import (
	equinox "equinox/common"
	"sync"
)

type Snapshot struct {
	version uint64

	prev *Snapshot
	next *Snapshot
}

func (s *Snapshot) Version() uint64 {
	return s.version
}

// snapshotList is mantian a doubly-linked circular list that keep using snapshot
type snapshotList struct {
	head Snapshot
	sync.RWMutex
}

func newSnapshotList() *snapshotList {
	sl := snapshotList{
		head: Snapshot{version: 0},
	}

	sl.head.prev = &sl.head
	sl.head.next = &sl.head

	return &sl
}

func (sl *snapshotList) Empty() bool {
	return sl.head.next == &sl.head
}

func (sl *snapshotList) Oldest() *Snapshot {
	sl.RLock()
	defer sl.RUnlock()

	equinox.AssertTrue(!sl.Empty())
	return sl.head.next
}

func (sl *snapshotList) Newest() *Snapshot {
	sl.RLock()
	defer sl.RUnlock()

	equinox.AssertTrue(!sl.Empty())
	return sl.head.prev
}

func (sl *snapshotList) New(version uint64) *Snapshot {
	equinox.AssertTrue(sl.Empty() || sl.Newest().version <= version)

	snapshot := &Snapshot{version: version}
	sl.Lock()
	defer sl.Unlock()

	snapshot.next = &sl.head
	snapshot.prev = sl.head.prev
	snapshot.prev.next = snapshot
	snapshot.next.prev = snapshot

	return snapshot
}

func (sl *snapshotList) Delete(s *Snapshot) {
	sl.Lock()
	defer sl.Unlock()

	s.next.prev = s.prev
	s.prev.next = s.next
}
