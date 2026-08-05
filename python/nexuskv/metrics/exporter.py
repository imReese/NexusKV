from __future__ import annotations

import threading
from dataclasses import dataclass, field


@dataclass(slots=True)
class NexusMetricsSnapshot:
    cache_hits: int = 0
    cache_misses: int = 0
    fail_open_events: int = 0
    quota_rejections: int = 0
    total_bytes_transferred: int = 0
    last_transfer_bandwidth_gbps: float = 0.0

    @property
    def hit_rate(self) -> float:
        total = self.cache_hits + self.cache_misses
        if total == 0:
            return 0.0
        return self.cache_hits / total


@dataclass(slots=True)
class NexusMetricsCollector:
    """Thread-safe OpenTelemetry / Prometheus metric aggregator for NexusKV execution."""

    _hits: int = field(default=0, init=False)
    _misses: int = field(default=0, init=False)
    _fail_open: int = field(default=0, init=False)
    _rejections: int = field(default=0, init=False)
    _bytes_transferred: int = field(default=0, init=False)
    _last_transfer_duration_sec: float = field(default=0.0, init=False)
    _lock: threading.Lock = field(default_factory=threading.Lock, init=False)

    def record_cache_hit(self) -> None:
        with self._lock:
            self._hits += 1

    def record_cache_miss(self) -> None:
        with self._lock:
            self._misses += 1

    def record_fail_open(self) -> None:
        with self._lock:
            self._fail_open += 1

    def record_quota_rejection(self) -> None:
        with self._lock:
            self._rejections += 1

    def record_transfer(self, bytes_count: int, duration_sec: float) -> None:
        with self._lock:
            self._bytes_transferred += bytes_count
            if duration_sec > 0:
                self._last_transfer_duration_sec = duration_sec

    def snapshot(self) -> NexusMetricsSnapshot:
        with self._lock:
            gbps = 0.0
            if self._last_transfer_duration_sec > 0 and self._bytes_transferred > 0:
                gbps = (self._bytes_transferred / (1024**3)) / self._last_transfer_duration_sec

            return NexusMetricsSnapshot(
                cache_hits=self._hits,
                cache_misses=self._misses,
                fail_open_events=self._fail_open,
                quota_rejections=self._rejections,
                total_bytes_transferred=self._bytes_transferred,
                last_transfer_bandwidth_gbps=gbps,
            )
