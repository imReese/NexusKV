from __future__ import annotations

import time
from dataclasses import dataclass, field
import threading

from nexuskv.contracts.generated import TierKind
from nexuskv.planner.cost import CostEstimator


@dataclass(slots=True)
class DynamicCostProfiler:
    cost_estimator: CostEstimator = field(default_factory=CostEstimator)
    max_samples: int = 100
    _lock: threading.Lock = field(default_factory=threading.Lock, init=False)
    _prefill_token_time_samples: list[float] = field(default_factory=list, init=False)
    _bandwidth_samples: dict[TierKind, list[float]] = field(default_factory=dict, init=False)

    def record_prefill_sample(self, token_count: int, duration_sec: float) -> float:
        if token_count <= 0 or duration_sec <= 0:
            return self.cost_estimator.time_per_token_prefill_sec

        t_per_token = duration_sec / token_count
        with self._lock:
            self._prefill_token_time_samples.append(t_per_token)
            if len(self._prefill_token_time_samples) > self.max_samples:
                self._prefill_token_time_samples.pop(0)

            avg_t = sum(self._prefill_token_time_samples) / len(self._prefill_token_time_samples)
            self.cost_estimator.time_per_token_prefill_sec = avg_t
        return avg_t

    def record_bandwidth_sample(self, tier: TierKind, payload_bytes: int, duration_sec: float) -> float:
        if payload_bytes <= 0 or duration_sec <= 0:
            return self.cost_estimator.bandwidth_config.bandwidth_for_tier(tier)

        bw_bps = payload_bytes / duration_sec
        with self._lock:
            if tier not in self._bandwidth_samples:
                self._bandwidth_samples[tier] = []
            samples = self._bandwidth_samples[tier]
            samples.append(bw_bps)
            if len(samples) > self.max_samples:
                samples.pop(0)

            avg_bw = sum(samples) / len(samples)
            if tier == TierKind.DEVICE:
                self.cost_estimator.bandwidth_config.device_hbm_bw = avg_bw
            elif tier == TierKind.HOST_DRAM:
                self.cost_estimator.bandwidth_config.host_dram_bw = avg_bw
            elif tier == TierKind.LOCAL_SSD:
                self.cost_estimator.bandwidth_config.local_ssd_bw = avg_bw
            elif tier == TierKind.REMOTE_SHARED:
                self.cost_estimator.bandwidth_config.remote_shared_bw = avg_bw
            elif tier == TierKind.OBJECT_STORE:
                self.cost_estimator.bandwidth_config.object_store_bw = avg_bw
        return avg_bw

    def get_current_prefill_time(self) -> float:
        with self._lock:
            return self.cost_estimator.time_per_token_prefill_sec

    def get_current_bandwidth(self, tier: TierKind) -> float:
        with self._lock:
            return self.cost_estimator.bandwidth_config.bandwidth_for_tier(tier)
