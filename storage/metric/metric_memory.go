package metric

import "github.com/prometheus/client_golang/prometheus"

var globalCacheMetrics = newAllCacheMetrics()

const cacheSubsystem = "memory"

type allCacheMetrics struct {
	MemBytes     *prometheus.GaugeVec
	LastSnapshot *prometheus.GaugeVec
	Writes       *prometheus.CounterVec
	WriteErr     *prometheus.CounterVec
}

type MemoryMetrics struct {
	MemBytes     prometheus.Gauge
	LastSnapshot prometheus.Gauge
	Writes       prometheus.Counter
	WriteErr     prometheus.Counter
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
		Writes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: storageNamespace,
			Subsystem: cacheSubsystem,
			Name:      "writes_total",
			Help:      "Counter of all writes to cache",
		}, labelNames),
		WriteErr: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: storageNamespace,
			Subsystem: cacheSubsystem,
			Name:      "writes_err",
			Help:      "Counter of failed writes to cache",
		}, labelNames),
	}
}

func CacheCollectors() []prometheus.Collector {
	return []prometheus.Collector{
		globalCacheMetrics.MemBytes,
		globalCacheMetrics.LastSnapshot,
		globalCacheMetrics.Writes,
		globalCacheMetrics.WriteErr,
	}
}

func NewCacheMetrics(labels prometheus.Labels) *MemoryMetrics {
	return &MemoryMetrics{
		MemBytes:     globalCacheMetrics.MemBytes.With(labels),
		LastSnapshot: globalCacheMetrics.LastSnapshot.With(labels),
		Writes:       globalCacheMetrics.Writes.With(labels),
		WriteErr:     globalCacheMetrics.WriteErr.With(labels),
	}
}
