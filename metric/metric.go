package metric

import "sync/atomic"

type Metric struct {
	vFileFlushTime atomic.Int64
	writePoints    atomic.Uint64
}

func (m *Metric) RecordVFileFlushTime(time int64) {
	m.vFileFlushTime.Add(time)
}

func (m *Metric) GetVFileFlushTIme() int64 {
	return m.vFileFlushTime.Load()
}

func (m *Metric) IncrWritePoints() uint64 {
	if m.writePoints.Load() < 20 {
		m.writePoints.Add(20)
	}
	m.writePoints.Add(1)
	return m.writePoints.Load()
}
