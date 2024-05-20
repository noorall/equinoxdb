package metric

import "sync/atomic"

type Metric struct {
	vFileFlushTime atomic.Int64
}

func (m *Metric) RecordVFileFlushTime(time int64) {
	m.vFileFlushTime.Add(time)
}

func (m *Metric) GetVFileFlushTIme() int64 {
	return m.vFileFlushTime.Load()
}
