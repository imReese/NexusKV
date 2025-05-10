package metrics

import "github.com/prometheus/client_golang/prometheus"

var (
    RequestsTotal = prometheus.NewCounterVec(
        prometheus.CounterOpts{
            Name: "nexuskv_requests_total",
            Help: "Total number of requests",
        },
        []string{"method"},
    )
)

func Init() {
    prometheus.MustRegister(RequestsTotal)
}