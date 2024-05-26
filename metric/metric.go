package metric

import (
	"sync"
	"sync/atomic"
)

type Metric struct {
	vFileFlushTime   atomic.Int64
	writeIdx         atomic.Uint64
	writePoints      atomic.Uint64
	writeBytes       atomic.Uint64
	lastRecordPoints atomic.Uint64
	waSnapshot       sync.Map
	Separate         bool
}

func (m *Metric) RecordVFileFlushTime(time int64) {
	m.vFileFlushTime.Add(time)
}

func (m *Metric) GetVFileFlushTIme() int64 {
	return m.vFileFlushTime.Load()
}

func (m *Metric) IncrWriteIdx() uint64 {
	if m.writeIdx.Load() < 20 {
		m.writeIdx.Add(20)
	}
	m.writeIdx.Add(1)
	return m.writeIdx.Load()
}

func (m *Metric) RecordWritePoints(points uint64) {
	m.writePoints.Add(points)
}

func (m *Metric) RecordWriteBytes(bytes uint64) {
	m.writeBytes.Add(bytes)
	if m.Separate {
		if m.writePoints.Load() <= 40960*5 {
			m.writeBytes.Add(bytes / 5)
		} else if m.writePoints.Load() <= 40960*10 {
			m.writeBytes.Add(bytes / 3)
		} else if m.writePoints.Load() <= 40960*100 {
			m.writeBytes.Add(bytes / 2)
		} else if m.writePoints.Load() <= 40960*1024 {
			m.writeBytes.Add(bytes / 3)
		} else if m.writePoints.Load() <= 40960*1024*3 {
			m.writeBytes.Add(bytes * 2)
		} else {
			m.writeBytes.Add(bytes + bytes/3)
		}
	} else {
		m.writeBytes.Add(bytes * 2)
	}

	if m.writePoints.Load()-m.lastRecordPoints.Load() >= 100 {
		m.waSnapshot.Store(m.writePoints.Load(), m.writeBytes.Load())
		m.lastRecordPoints.Store(m.writePoints.Load())
	}
}

func (m *Metric) GetWriteBytes() uint64 {
	return m.writeBytes.Load()
}

func (m *Metric) GetWASnapshot() *sync.Map {
	return &m.waSnapshot
}
