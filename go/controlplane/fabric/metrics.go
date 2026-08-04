package fabric

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
)

type PrometheusMetricsExporter struct {
	mu                       sync.RWMutex
	hits                     uint64
	misses                   uint64
	effectiveGainMs          float64
	hbmUsageBytes            uint64
	quotaBackpressureCounter uint64
}

func NewPrometheusMetricsExporter() *PrometheusMetricsExporter {
	return &PrometheusMetricsExporter{}
}

func (e *PrometheusMetricsExporter) RecordHit() {
	atomic.AddUint64(&e.hits, 1)
}

func (e *PrometheusMetricsExporter) RecordMiss() {
	atomic.AddUint64(&e.misses, 1)
}

func (e *PrometheusMetricsExporter) RecordEffectiveGain(gainMs float64) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.effectiveGainMs += gainMs
}

func (e *PrometheusMetricsExporter) SetHbmUsageBytes(bytes uint64) {
	atomic.StoreUint64(&e.hbmUsageBytes, bytes)
}

func (e *PrometheusMetricsExporter) IncQuotaBackpressure() {
	atomic.AddUint64(&e.quotaBackpressureCounter, 1)
}

func (e *PrometheusMetricsExporter) CalculateHitRate() float64 {
	hits := atomic.LoadUint64(&e.hits)
	misses := atomic.LoadUint64(&e.misses)
	total := hits + misses
	if total == 0 {
		return 0.0
	}
	return float64(hits) / float64(total)
}

func (e *PrometheusMetricsExporter) ExportPrometheusMetrics() string {
	e.mu.RLock()
	defer e.mu.RUnlock()

	hitRate := e.CalculateHitRate()
	hits := atomic.LoadUint64(&e.hits)
	misses := atomic.LoadUint64(&e.misses)
	hbmBytes := atomic.LoadUint64(&e.hbmUsageBytes)
	backpressure := atomic.LoadUint64(&e.quotaBackpressureCounter)

	var b strings.Builder
	b.WriteString("# HELP nexuskv_cache_hit_rate Current KV cache hit rate ratio [0.0 - 1.0]\n")
	b.WriteString("# TYPE nexuskv_cache_hit_rate gauge\n")
	b.WriteString(fmt.Sprintf("nexuskv_cache_hit_rate %.4f\n\n", hitRate))

	b.WriteString("# HELP nexuskv_cache_hits_total Total count of cache hits\n")
	b.WriteString("# TYPE nexuskv_cache_hits_total counter\n")
	b.WriteString(fmt.Sprintf("nexuskv_cache_hits_total %d\n\n", hits))

	b.WriteString("# HELP nexuskv_cache_misses_total Total count of cache misses\n")
	b.WriteString("# TYPE nexuskv_cache_misses_total counter\n")
	b.WriteString(fmt.Sprintf("nexuskv_cache_misses_total %d\n\n", misses))

	b.WriteString("# HELP nexuskv_effective_gain_ms_total Aggregate effective gain time saved in milliseconds\n")
	b.WriteString("# TYPE nexuskv_effective_gain_ms_total counter\n")
	b.WriteString(fmt.Sprintf("nexuskv_effective_gain_ms_total %.2f\n\n", e.effectiveGainMs))

	b.WriteString("# HELP nexuskv_hbm_usage_bytes Current GPU HBM memory allocated in bytes\n")
	b.WriteString("# TYPE nexuskv_hbm_usage_bytes gauge\n")
	b.WriteString(fmt.Sprintf("nexuskv_hbm_usage_bytes %d\n\n", hbmBytes))

	b.WriteString("# HELP nexuskv_quota_backpressure_events_total Total quota admission backpressure events\n")
	b.WriteString("# TYPE nexuskv_quota_backpressure_events_total counter\n")
	b.WriteString(fmt.Sprintf("nexuskv_quota_backpressure_events_total %d\n", backpressure))

	return b.String()
}
