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
)

func Init() {
	registerOnce.Do(func() {
		prometheus.MustRegister(RequestsTotal)
	})
}
