package metric

import (
	"fmt"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"sync/atomic"
)

var defaultGlobalFileStoreMetrics = newGlobalFileStoreMetrics()

type globalFileStoreMetrics struct {
	files        *prometheus.GaugeVec
	size         *prometheus.GaugeVec
	totalWritten *prometheus.GaugeVec
}

type FileStoreMetrics struct {
	files        prometheus.Gauge
	size         prometheus.Gauge
	totalWritten prometheus.Gauge
	sizeAtomic   int64
}

func (f *FileStoreMetrics) AddSize(n int64) {
	val := atomic.AddInt64(&f.sizeAtomic, n)
	f.size.Set(float64(val))
}

func (f *FileStoreMetrics) AddTotalWritten(n int64) {
	f.totalWritten.Add(float64(n))
}

func (f *FileStoreMetrics) SetSize(n int64) {
	atomic.StoreInt64(&f.sizeAtomic, n)
	f.size.Set(float64(n))
}

func (f *FileStoreMetrics) SetFiles(n int64) {
	f.files.Set(float64(n))
}

func (f *FileStoreMetrics) GetTotalWritten() float64 {
	m := &dto.Metric{}
	if err := f.totalWritten.Write(m); err != nil {
		fmt.Println("error writing metric:", err)
		return 0
	}
	return m.GetGauge().GetValue()
}

func NewFileStoreMetrics(labels prometheus.Labels) *FileStoreMetrics {
	return &FileStoreMetrics{
		files:        defaultGlobalFileStoreMetrics.files.With(labels),
		size:         defaultGlobalFileStoreMetrics.size.With(labels),
		totalWritten: defaultGlobalFileStoreMetrics.totalWritten.With(labels),
	}
}

func newGlobalFileStoreMetrics() *globalFileStoreMetrics {
	return &globalFileStoreMetrics{
		files: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "equinox",
			Subsystem: "file_store",
			Name:      "total",
			Help:      "Gauge of number of files per filestore",
		}, labelNames),
		size: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "equinox",
			Subsystem: "file_store",
			Name:      "disk_bytes",
			Help:      "Gauge of data size in bytes for each filestore",
		}, labelNames),
		totalWritten: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "equinox",
			Subsystem: "file_store",
			Name:      "total_written",
			Help:      "Gauge of total write data size in bytes for each filestore",
		}, labelNames),
	}
}

func FileStoreCollectors() []prometheus.Collector {
	return []prometheus.Collector{
		defaultGlobalFileStoreMetrics.files,
		defaultGlobalFileStoreMetrics.size,
		defaultGlobalFileStoreMetrics.totalWritten,
	}
}
