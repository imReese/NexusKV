// pkg/metrics/metrics.go
package metrics

import (
	"net/http"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	once sync.Once

	CacheLookupsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "nexuskv",
			Name:      "cache_lookups_total",
			Help:      "Total number of KV cache lookup requests",
		},
		[]string{"tenant", "model", "result"},
	)

	PrefillSavedTokensTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "nexuskv",
			Name:      "prefill_saved_tokens_total",
			Help:      "Total number of prefill tokens saved via KV cache hit",
		},
		[]string{"tenant", "model"},
	)

	ActivePinnedMemoryBytes = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "nexuskv",
			Name:      "active_pinned_memory_bytes",
			Help:      "Current active pinned memory allocated in Host DRAM/POSIX SHM",
		},
	)

	FailOpenFallbacksTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "nexuskv",
			Name:      "fail_open_fallbacks_total",
			Help:      "Total number of fail-open fallback events triggered",
		},
		[]string{"reason"},
	)
)

func Init() {
	once.Do(func() {
		prometheus.MustRegister(CacheLookupsTotal)
		prometheus.MustRegister(PrefillSavedTokensTotal)
		prometheus.MustRegister(ActivePinnedMemoryBytes)
		prometheus.MustRegister(FailOpenFallbacksTotal)
	})
}

func Handler() http.Handler {
	Init()
	return promhttp.Handler()
}
