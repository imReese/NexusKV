"""Python side Prometheus metrics exporter and latency collector for NexusKV."""

from __future__ import annotations

from dataclasses import dataclass, field

from nexuskv.logger import logger


@dataclass(slots=True)
class MetricCounter:
    name: str
    value: float = 0.0
    labels: dict[str, str] = field(default_factory=dict)

    def inc(self, amount: float = 1.0) -> None:
        self.value += amount


@dataclass(slots=True)
class MetricGauge:
    name: str
    value: float = 0.0

    def set(self, val: float) -> None:
        self.value = val


class PythonMetricsExporter:
    """Collects Python connector metrics for Prometheus integration."""

    def __init__(self) -> None:
        self.lookups_total = MetricCounter(name="nexuskv_cache_lookups_total")
        self.hits_total = MetricCounter(name="nexuskv_cache_hits_total")
        self.misses_total = MetricCounter(name="nexuskv_cache_misses_total")
        self.saved_tokens_total = MetricCounter(name="nexuskv_prefill_saved_tokens_total")
        self.active_pinned_bytes = MetricGauge(name="nexuskv_active_pinned_memory_bytes")
        self.transfer_bandwidth_gbps = MetricGauge(name="nexuskv_transfer_bandwidth_gbps")
        self.fail_open_total = MetricCounter(name="nexuskv_fail_open_fallbacks_total")

    def record_lookup(self, hit: bool, tokens_saved: int = 0) -> None:
        self.lookups_total.inc()
        if hit:
            self.hits_total.inc()
            if tokens_saved > 0:
                self.saved_tokens_total.inc(float(tokens_saved))
        else:
            self.misses_total.inc()

    def record_fail_open(self, reason: str = "timeout") -> None:
        self.fail_open_total.inc()
        logger.warning("Recorded fail-open fallback metric: %s", reason)

    def set_active_pinned_memory(self, size_bytes: int) -> None:
        self.active_pinned_bytes.set(float(size_bytes))

    def set_transfer_bandwidth(self, gbps: float) -> None:
        self.transfer_bandwidth_gbps.set(gbps)

    def export_prometheus_text(self) -> str:
        lines = [
            "# HELP nexuskv_cache_lookups_total Total number of KV cache lookup requests",
            "# TYPE nexuskv_cache_lookups_total counter",
            f"nexuskv_cache_lookups_total {self.lookups_total.value:.0f}",
            "# HELP nexuskv_cache_hits_total Total number of KV cache hit requests",
            "# TYPE nexuskv_cache_hits_total counter",
            f"nexuskv_cache_hits_total {self.hits_total.value:.0f}",
            "# HELP nexuskv_cache_misses_total Total number of KV cache miss requests",
            "# TYPE nexuskv_cache_misses_total counter",
            f"nexuskv_cache_misses_total {self.misses_total.value:.0f}",
            "# HELP nexuskv_prefill_saved_tokens_total Total prefill tokens saved",
            "# TYPE nexuskv_prefill_saved_tokens_total counter",
            f"nexuskv_prefill_saved_tokens_total {self.saved_tokens_total.value:.0f}",
            "# HELP nexuskv_active_pinned_memory_bytes Current active pinned memory in bytes",
            "# TYPE nexuskv_active_pinned_memory_bytes gauge",
            f"nexuskv_active_pinned_memory_bytes {self.active_pinned_bytes.value:.0f}",
            "# HELP nexuskv_transfer_bandwidth_gbps Current physical transfer bandwidth in GB/s",
            "# TYPE nexuskv_transfer_bandwidth_gbps gauge",
            f"nexuskv_transfer_bandwidth_gbps {self.transfer_bandwidth_gbps.value:.2f}",
            "# HELP nexuskv_fail_open_fallbacks_total Total fail-open fallback events",
            "# TYPE nexuskv_fail_open_fallbacks_total counter",
            f"nexuskv_fail_open_fallbacks_total {self.fail_open_total.value:.0f}",
        ]
        return "\n".join(lines) + "\n"
