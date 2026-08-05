from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum

from nexuskv.contracts.generated import TierKind, TransferBackend


class CostProfilePreset(StrEnum):
    GPU_H100_DEFAULT = "gpu_h100_default"
    GPU_A100_DEFAULT = "gpu_a100_default"
    GENERIC_CPU = "generic_cpu"


@dataclass(slots=True)
class BandwidthConfig:
    # Bandwidth in Bytes / second
    device_hbm_bw: float = 3.35e12  # 3.35 TB/s (H100 HBM3)
    host_dram_bw: float = 2.0e11  # 200 GB/s (PCIe Gen5 x16)
    local_ssd_bw: float = 7.0e9  # 7 GB/s (NVMe Gen4)
    remote_shared_bw: float = 5.0e10  # 50 GB/s (400Gbps RDMA)
    object_store_bw: float = 1.0e9  # 1 GB/s (S3 / MinIO)

    def bandwidth_for_tier(self, tier: TierKind | None) -> float:
        if tier == TierKind.DEVICE:
            return self.device_hbm_bw
        if tier == TierKind.HOST_DRAM:
            return self.host_dram_bw
        if tier == TierKind.LOCAL_SSD:
            return self.local_ssd_bw
        if tier == TierKind.REMOTE_SHARED:
            return self.remote_shared_bw
        if tier == TierKind.OBJECT_STORE:
            return self.object_store_bw
        return self.host_dram_bw


@dataclass(slots=True)
class CostEstimateResult:
    effective_gain_seconds: float
    t_recompute_seconds: float
    t_cache_seconds: float
    t_lookup_seconds: float
    t_transfer_seconds: float
    t_restore_seconds: float
    t_interference_seconds: float
    is_profitable: bool
    explanation: str


@dataclass(slots=True)
class CostEstimator:
    bandwidth_config: BandwidthConfig = field(default_factory=BandwidthConfig)
    # Prefill computation cost per token in seconds (e.g. 5 microseconds/token for 70B model on H100)
    time_per_token_prefill_sec: float = 5.0e-6
    # Fixed scheduling overhead in seconds
    schedule_overhead_sec: float = 1.0e-4
    # Fixed metadata lookup overhead in seconds
    lookup_overhead_sec: float = 2.0e-5
    # Fixed restoration/unpacking overhead in seconds
    restore_overhead_sec: float = 5.0e-5

    def estimate(
        self,
        token_count: int,
        payload_bytes: int,
        source_tier: TierKind | None,
        target_tier: TierKind | None = TierKind.DEVICE,
        backend: TransferBackend | None = None,
        concurrent_transfers: int = 0,
    ) -> CostEstimateResult:
        # Recomputation time
        t_recompute = self.schedule_overhead_sec + (token_count * self.time_per_token_prefill_sec)

        # Transfer bandwidth depends on tier (and concurrency degradation)
        base_bw = self.bandwidth_config.bandwidth_for_tier(source_tier)
        concurrency_factor = 1.0 + (0.25 * max(0, concurrent_transfers - 1))
        effective_bw = base_bw / concurrency_factor if base_bw > 0 else 1.0e6

        t_transfer = payload_bytes / effective_bw if payload_bytes > 0 else 0.0
        t_lookup = self.lookup_overhead_sec
        t_restore = self.restore_overhead_sec

        # Interference cost scales with concurrency
        t_interference = 5.0e-5 * concurrent_transfers

        t_cache = t_lookup + t_transfer + t_restore + t_interference
        effective_gain = t_recompute - t_cache
        is_profitable = effective_gain > 0.0

        if is_profitable:
            explanation = (
                f"Profitable reuse: Effective Gain = {effective_gain * 1000:.3f}ms "
                f"(T_compute={t_recompute * 1000:.3f}ms > T_cache={t_cache * 1000:.3f}ms)"
            )
        else:
            explanation = (
                f"Unprofitable reuse: Effective Gain = {effective_gain * 1000:.3f}ms "
                f"(T_compute={t_recompute * 1000:.3f}ms <= T_cache={t_cache * 1000:.3f}ms)"
            )

        return CostEstimateResult(
            effective_gain_seconds=effective_gain,
            t_recompute_seconds=t_recompute,
            t_cache_seconds=t_cache,
            t_lookup_seconds=t_lookup,
            t_transfer_seconds=t_transfer,
            t_restore_seconds=t_restore,
            t_interference_seconds=t_interference,
            is_profitable=is_profitable,
            explanation=explanation,
        )
