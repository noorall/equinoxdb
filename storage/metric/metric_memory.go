package metric

import (
	"fmt"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

var globalCacheMetrics = newAllCacheMetrics()

const cacheSubsystem = "memory"

type allCacheMetrics struct {
	MemBytes     *prometheus.GaugeVec
	LastSnapshot *prometheus.GaugeVec
	Writes       *prometheus.GaugeVec
	WriteErr     *prometheus.GaugeVec
	TotalWritten *prometheus.GaugeVec
}

type MemoryMetrics struct {
	MemBytes     prometheus.Gauge
	LastSnapshot prometheus.Gauge
	Writes       prometheus.Gauge
	WriteErr     prometheus.Gauge
	TotalWritten prometheus.Gauge
}

func (f *MemoryMetrics) GetTotalWritten() float64 {
	m := &dto.Metric{}
	if err := f.TotalWritten.Write(m); err != nil {
		fmt.Println("error writing metric:", err)
		return 0
	}
	return m.GetGauge().GetValue()
}

func newAllCacheMetrics() *allCacheMetrics {
	return &allCacheMetrics{
		MemBytes: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: storageNamespace,
			Subsystem: cacheSubsystem,
			Name:      "inuse_bytes",
			Help:      "Gauge of current memory consumption of cache",
		}, labelNames),
		LastSnapshot: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: storageNamespace,
			Subsystem: cacheSubsystem,
			Name:      "latest_snapshot",
			Help:      "Unix time of most recent snapshot",
		}, labelNames),
		Writes: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: storageNamespace,
			Subsystem: cacheSubsystem,
			Name:      "writes_total",
			Help:      "Counter of all writes to cache",
		}, labelNames),
		WriteErr: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: storageNamespace,
			Subsystem: cacheSubsystem,
			Name:      "writes_err",
			Help:      "Counter of failed writes to cache",
		}, labelNames),
		TotalWritten: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: storageNamespace,
			Subsystem: "wal_store",
			Name:      "total_written",
			Help:      "Gauge of total write data size in bytes for wal",
		}, labelNames),
	}
}

func CacheCollectors() []prometheus.Collector {
	return []prometheus.Collector{
		globalCacheMetrics.MemBytes,
		globalCacheMetrics.LastSnapshot,
		globalCacheMetrics.Writes,
		globalCacheMetrics.WriteErr,
		globalCacheMetrics.TotalWritten,
	}
}

func NewCacheMetrics(labels prometheus.Labels) *MemoryMetrics {
	m := &MemoryMetrics{
		MemBytes:     globalCacheMetrics.MemBytes.With(labels),
		LastSnapshot: globalCacheMetrics.LastSnapshot.With(labels),
		Writes:       globalCacheMetrics.Writes.With(labels),
		WriteErr:     globalCacheMetrics.WriteErr.With(labels),
		TotalWritten: globalCacheMetrics.TotalWritten.With(labels),
	}
	m.MemBytes.Set(0)
	m.LastSnapshot.SetToCurrentTime()
	m.Writes.Set(0)
	m.WriteErr.Set(0)
	m.TotalWritten.Set(0)
	return m
}
