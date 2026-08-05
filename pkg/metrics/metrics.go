package metrics

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	registerOnce sync.Once

	RequestsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "nexuskv_requests_total",
			Help: "Total number of requests",
		},
		[]string{"method"},
	)

	CacheHitsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "nexuskv_cache_hits_total",
			Help: "Total number of cache hits",
		},
		[]string{"tenant", "model"},
	)

	CacheMissesTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "nexuskv_cache_misses_total",
			Help: "Total number of cache misses",
		},
		[]string{"tenant", "model"},
	)

	TransferBandwidthBytesPerSec = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "nexuskv_transfer_bandwidth_bytes_per_sec",
			Help: "Current physical transfer bandwidth in bytes per second",
		},
		[]string{"backend", "tier"},
	)

	FailOpenEventsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "nexuskv_fail_open_events_total",
			Help: "Total number of Fail-Open fallback events",
		},
		[]string{"reason"},
	)

	QuotaRejectionsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "nexuskv_quota_rejections_total",
			Help: "Total number of tenant quota backpressure rejections",
		},
		[]string{"tenant", "resource"},
	)
)

func Init() {
	registerOnce.Do(func() {
		prometheus.MustRegister(
			RequestsTotal,
			CacheHitsTotal,
			CacheMissesTotal,
			TransferBandwidthBytesPerSec,
			FailOpenEventsTotal,
			QuotaRejectionsTotal,
		)
	})
}
