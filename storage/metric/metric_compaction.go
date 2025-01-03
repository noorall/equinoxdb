package metric

import (
	"fmt"
	"github.com/prometheus/client_golang/prometheus"
)

var globalCompactionMetrics = newAllCompactionMetrics()

const (
	storageNamespace    = "equinox"
	compactionSubsystem = "compactions"
	Level1              = "1"
	Level2              = "2"
	Level3              = "3"
	LevelOpt            = "opt"
	LevelFull           = "full"
	LevelKey            = "level"
	LevelCache          = "cache"
)

func LabelForLevel(l int) prometheus.Labels {
	switch l {
	case 1:
		return prometheus.Labels{LevelKey: Level1}
	case 2:
		return prometheus.Labels{LevelKey: Level2}
	case 3:
		return prometheus.Labels{LevelKey: Level3}
	}
	panic(fmt.Sprintf("LabelForLevel: level out of range %d", l))
}

func newAllCompactionMetrics() *CompactionMetrics {
	labelNamesWithLevel := append(labelNames, LevelKey)
	return &CompactionMetrics{
		Duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: storageNamespace,
			Subsystem: compactionSubsystem,
			Name:      "duration_seconds",
			Help:      "Histogram of compactions by level since startup",
			// 10 minute compactions seem normal, 1h40min is high
			Buckets: []float64{60, 600, 6000},
		}, labelNamesWithLevel),
		Active: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: storageNamespace,
			Subsystem: compactionSubsystem,
			Name:      "active",
			Help:      "Gauge of compactions (by level) currently running",
		}, labelNamesWithLevel),
		Failed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: storageNamespace,
			Subsystem: compactionSubsystem,
			Name:      "failed",
			Help:      "Counter of SST compactions (by level) that have failed due to error",
		}, labelNamesWithLevel),
		Queued: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: storageNamespace,
			Subsystem: compactionSubsystem,
			Name:      "queued",
			Help:      "Counter of SST compactions (by level) that are currently queued",
		}, labelNamesWithLevel),
	}
}

// CompactionMetrics holds statistics
type CompactionMetrics struct {
	Duration prometheus.ObserverVec
	Active   *prometheus.GaugeVec
	Queued   *prometheus.GaugeVec
	Failed   *prometheus.CounterVec
}

func NewCompactionMetrics(labels prometheus.Labels) *CompactionMetrics {
	return &CompactionMetrics{
		Duration: globalCompactionMetrics.Duration.MustCurryWith(labels),
		Active:   globalCompactionMetrics.Active.MustCurryWith(labels),
		Failed:   globalCompactionMetrics.Failed.MustCurryWith(labels),
		Queued:   globalCompactionMetrics.Queued.MustCurryWith(labels),
	}
}

func CompactionCollectors() []prometheus.Collector {
	return []prometheus.Collector{
		globalCompactionMetrics.Duration,
		globalCompactionMetrics.Active,
		globalCompactionMetrics.Queued,
		globalCompactionMetrics.Failed,
	}
}
