package metric

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
	"net/http"
	"strconv"
)

func PrometheusCollectors() []prometheus.Collector {
	collectors := EngineCollectors()
	collectors = append(collectors, CompactionCollectors()...)
	collectors = append(collectors, FileStoreCollectors()...)
	collectors = append(collectors, CacheCollectors()...)
	collectors = append(collectors, ValueFileCollectors()...)
	return collectors
}

func RunMetricServer(logger *zap.Logger, port int) {
	for _, collector := range PrometheusCollectors() {
		prometheus.MustRegister(collector)
	}

	http.Handle("/metrics", promhttp.Handler())

	go func() {
		logger.Info("Starting Prometheus metrics server on :", zap.Int("port", port))
		if err := http.ListenAndServe("0.0.0.0:"+strconv.Itoa(port), nil); err != nil {
			logger.Error("metrics HTTP server failed!", zap.Error(err))
		}
	}()
}
