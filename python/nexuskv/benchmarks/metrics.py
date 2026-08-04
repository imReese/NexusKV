from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path

from nexuskv.contracts.generated import TierKind


@dataclass(slots=True)
class RequestMetricRecord:
    request_id: str
    strategy_name: str
    context_length: int
    shared_prefix_len: int
    reuse_ratio: float
    source_tier: TierKind
    decision: str  # "MATERIALIZE", "RECOMPUTE", "SKIP"
    is_hit: bool
    is_useful_reuse: bool
    effective_gain_ms: float
    t_compute_ms: float
    t_cache_ms: float
    fallback_reason: str | None = None


@dataclass(slots=True)
class BenchmarkReport:
    trace_name: str
    strategy_name: str
    total_requests: int
    total_hits: int
    useful_reuses: int
    rejected_unprofitable_hits: int
    recomputations: int
    hit_rate: float
    useful_reuse_rate: float
    aggregate_effective_gain_ms: float
    avg_ttft_ms: float
    p95_ttft_ms: float
    records: list[RequestMetricRecord] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "trace_name": self.trace_name,
            "strategy_name": self.strategy_name,
            "total_requests": self.total_requests,
            "total_hits": self.total_hits,
            "useful_reuses": self.useful_reuses,
            "rejected_unprofitable_hits": self.rejected_unprofitable_hits,
            "recomputations": self.recomputations,
            "hit_rate": self.hit_rate,
            "useful_reuse_rate": self.useful_reuse_rate,
            "aggregate_effective_gain_ms": round(self.aggregate_effective_gain_ms, 3),
            "avg_ttft_ms": round(self.avg_ttft_ms, 3),
            "p95_ttft_ms": round(self.p95_ttft_ms, 3),
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), indent=2)


@dataclass(slots=True)
class BenchmarkMetricsCollector:
    trace_name: str
    strategy_name: str
    records: list[RequestMetricRecord] = field(default_factory=list)

    def record(self, metric: RequestMetricRecord) -> None:
        self.records.append(metric)

    def summarize(self) -> BenchmarkReport:
        if not self.records:
            return BenchmarkReport(
                trace_name=self.trace_name,
                strategy_name=self.strategy_name,
                total_requests=0,
                total_hits=0,
                useful_reuses=0,
                rejected_unprofitable_hits=0,
                recomputations=0,
                hit_rate=0.0,
                useful_reuse_rate=0.0,
                aggregate_effective_gain_ms=0.0,
                avg_ttft_ms=0.0,
                p95_ttft_ms=0.0,
            )

        total = len(self.records)
        hits = sum(1 for r in self.records if r.is_hit)
        useful = sum(1 for r in self.records if r.is_useful_reuse)
        rejected = sum(1 for r in self.records if r.is_hit and not r.is_useful_reuse)
        recomputes = sum(1 for r in self.records if r.decision == "RECOMPUTE")
        agg_gain = sum(r.effective_gain_ms for r in self.records if r.is_useful_reuse)

        ttfts = [r.t_cache_ms if r.decision == "MATERIALIZE" else r.t_compute_ms for r in self.records]
        avg_ttft = sum(ttfts) / total
        sorted_ttfts = sorted(ttfts)
        p95_index = int(0.95 * total)
        p95_ttft = sorted_ttfts[min(p95_index, total - 1)]

        return BenchmarkReport(
            trace_name=self.trace_name,
            strategy_name=self.strategy_name,
            total_requests=total,
            total_hits=hits,
            useful_reuses=useful,
            rejected_unprofitable_hits=rejected,
            recomputations=recomputes,
            hit_rate=hits / total if total > 0 else 0.0,
            useful_reuse_rate=useful / total if total > 0 else 0.0,
            aggregate_effective_gain_ms=agg_gain,
            avg_ttft_ms=avg_ttft,
            p95_ttft_ms=p95_ttft,
            records=self.records,
        )
