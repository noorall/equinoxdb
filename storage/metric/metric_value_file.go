package metric

import (
	"github.com/prometheus/client_golang/prometheus"
)

var defaultGlobalValueFileMetrics = newGlobalValueFileMetrics()

type globalValueFileMetrics struct {
	files        *prometheus.GaugeVec
	size         *prometheus.GaugeVec
	totalWritten *prometheus.GaugeVec
}

type ValueFileMetrics struct {
	files        prometheus.Gauge
	size         prometheus.Gauge
	totalWritten prometheus.Gauge
}

func (f *ValueFileMetrics) AddSize(n int64) {
	f.size.Add(float64(n))
}

func (f *ValueFileMetrics) AddTotalWritten(n int64) {
	f.totalWritten.Add(float64(n))
}

func (f *ValueFileMetrics) AddFiles(n int64) {
	f.files.Add(float64(n))
}

func (f *ValueFileMetrics) DecFiles() {
	f.files.Dec()
}

func (f *ValueFileMetrics) IncFiles() {
	f.files.Inc()
}

func NewValueFileMetrics(labels prometheus.Labels) *ValueFileMetrics {
	return &ValueFileMetrics{
		files:        defaultGlobalValueFileMetrics.files.With(labels),
		size:         defaultGlobalValueFileMetrics.size.With(labels),
		totalWritten: defaultGlobalValueFileMetrics.totalWritten.With(labels),
	}
}

func newGlobalValueFileMetrics() *globalValueFileMetrics {
	return &globalValueFileMetrics{
		files: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: storageNamespace,
			Subsystem: "vfile_store",
			Name:      "total",
			Help:      "Gauge of number of files per vfilestore",
		}, labelNames),
		size: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: storageNamespace,
			Subsystem: "vfile_store",
			Name:      "disk_bytes",
			Help:      "Gauge of data size in bytes for each vfilestore",
		}, labelNames),
		totalWritten: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: storageNamespace,
			Subsystem: "vfile_store",
			Name:      "total_written",
			Help:      "Gauge of total write data size in bytes for each vfilestore",
		}, labelNames),
	}
}

func ValueFileCollectors() []prometheus.Collector {
	return []prometheus.Collector{
		defaultGlobalValueFileMetrics.files,
		defaultGlobalValueFileMetrics.size,
		defaultGlobalValueFileMetrics.totalWritten,
	}
}
