package metric

import (
	"errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
	"net/http"
	"strconv"
	"sync"
)

var metricsOnce sync.Once

func PrometheusCollectors() []prometheus.Collector {
	collectors := EngineCollectors()
	collectors = append(collectors, CompactionCollectors()...)
	collectors = append(collectors, FileStoreCollectors()...)
	collectors = append(collectors, CacheCollectors()...)
	collectors = append(collectors, ValueFileCollectors()...)
	return collectors
}

func RunMetricServer(logger *zap.Logger, port int) *http.Server {
	metricsOnce.Do(func() {
		for _, collector := range PrometheusCollectors() {
			prometheus.MustRegister(collector)
		}
	})

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())

	server := &http.Server{
		Addr:    "0.0.0.0:" + strconv.Itoa(port),
		Handler: mux,
	}

	go func() {
		logger.Info("Starting Prometheus metrics server", zap.Int("port", port))
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("metrics HTTP server failed!", zap.Error(err))
		}
	}()

	return server
}
