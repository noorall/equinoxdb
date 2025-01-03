package metric

import (
	"github.com/prometheus/client_golang/prometheus"
)

var defaultGlobalEngineMetrics = newGlobalEngineMetrics()

const engineSubsystem = "engine"

type globalEngineMetrics struct {
	writtenDelay  *prometheus.HistogramVec
	writtenOutput *prometheus.GaugeVec
}

type EngineMetrics struct {
	WrittenDelay  prometheus.ObserverVec
	WrittenOutput prometheus.Gauge
}

func NewEngineMetrics(labels prometheus.Labels) *EngineMetrics {

	return &EngineMetrics{
		WrittenDelay: defaultGlobalEngineMetrics.writtenDelay.MustCurryWith(labels),
		WrittenOutput: defaultGlobalEngineMetrics.writtenOutput.With(prometheus.Labels{
			"engine": labels["engine"],
			"type":   "default",
		}),
	}
}

func newGlobalEngineMetrics() *globalEngineMetrics {
	name := append(labelNames, "type")
	return &globalEngineMetrics{
		writtenDelay: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: "equinox",
				Subsystem: engineSubsystem,
				Name:      "write_delay_ms",
				Help:      "Histogram of write write_delay_ms in ms",
				Buckets:   []float64{50, 100, 500},
			}, name),
		writtenOutput: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: "equinox",
				Subsystem: engineSubsystem,
				Name:      "write_output",
				Help:      "Histogram of write write_output MB per s",
			}, name),
	}
}

func EngineCollectors() []prometheus.Collector {
	return []prometheus.Collector{
		defaultGlobalEngineMetrics.writtenDelay,
		defaultGlobalEngineMetrics.writtenOutput,
	}
}
