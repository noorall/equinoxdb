package main

import (
	"log"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// 示例：定义全局指标（你已经有了）
var globalCompactionMetrics = struct {
	Duration *prometheus.HistogramVec
	Active   prometheus.Gauge
	Failed   prometheus.Counter
	Queued   prometheus.Gauge
}{
	Duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "compaction_duration_seconds",
		Help: "Duration of compaction operations",
	}, []string{"type"}),
	Active: prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "compaction_active_total",
		Help: "Number of active compactions",
	}),
	Failed: prometheus.NewCounter(prometheus.CounterOpts{
		Name: "compaction_failed_total",
		Help: "Number of failed compactions",
	}),
	Queued: prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "compaction_queued_total",
		Help: "Number of queued compactions",
	}),
}

// 你已有的收集器列表函数
func PrometheusCollectors() []prometheus.Collector {
	return []prometheus.Collector{
		globalCompactionMetrics.Duration,
		globalCompactionMetrics.Active,
		globalCompactionMetrics.Failed,
		globalCompactionMetrics.Queued,
		// FileStoreCollectors()... 可继续追加
	}
}

// 注册并启动 HTTP 服务
func InitPrometheusMetrics() {
	for _, collector := range PrometheusCollectors() {
		prometheus.MustRegister(collector)
	}

	http.Handle("/metrics", promhttp.Handler())

	go func() {
		log.Println("Starting Prometheus metrics server on :2112/metrics")
		if err := http.ListenAndServe(":2112", nil); err != nil {
			log.Fatalf("metrics HTTP server failed: %v", err)
		}
	}()
}

func main() {
	InitPrometheusMetrics()

	// 示例业务逻辑（比如更新指标）
	globalCompactionMetrics.Active.Set(1)
	globalCompactionMetrics.Failed.Inc()
	globalCompactionMetrics.Duration.WithLabelValues("major").Observe(2.3)

	select {} // 防止退出
}
