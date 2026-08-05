from __future__ import annotations

from dataclasses import dataclass, field

from nexuskv.contracts.generated import TierKind
from nexuskv.planner.cost import CostEstimateResult, CostEstimator


@dataclass(slots=True)
class WorkerNodeState:
    node_id: str
    address: str
    active_transfers: int = 0
    hbm_usage_bytes: int = 0
    hbm_capacity_bytes: int = 80 * 1024 * 1024 * 1024  # Default 80GB
    cached_prefix_tokens: list[int] = field(default_factory=list)


@dataclass(slots=True)
class RoutingDecision:
    selected_node_id: str
    shared_prefix_len: int
    expected_gain_ms: float
    is_cache_hit: bool
    cost_detail: CostEstimateResult


class CacheAwareRouter:
    """Cache-Aware Cluster Router for LLM Inference.

    Evaluates prefix tree overlap across candidate worker nodes and selects
    the optimal GPU worker node that maximizes net effective gain (G = T_compute - T_cache).
    """

    def __init__(self, cost_estimator: CostEstimator | None = None) -> None:
        self.cost_estimator = cost_estimator or CostEstimator()

    def calculate_shared_prefix_length(
        self, prompt_tokens: list[int], worker_prefix_tokens: list[int]
    ) -> int:
        match_len = 0
        min_len = min(len(prompt_tokens), len(worker_prefix_tokens))
        for i in range(min_len):
            if prompt_tokens[i] == worker_prefix_tokens[i]:
                match_len += 1
            else:
                break
        return match_len

    def select_best_worker(
        self,
        prompt_tokens: list[int],
        candidate_workers: list[WorkerNodeState],
    ) -> RoutingDecision:
        if not candidate_workers:
            raise ValueError("No candidate worker nodes available for routing")

        best_node = candidate_workers[0]
        best_gain = -1e9
        best_prefix_len = 0
        best_cost_res: CostEstimateResult | None = None

        total_tokens = len(prompt_tokens)

        for worker in candidate_workers:
            prefix_len = self.calculate_shared_prefix_length(
                prompt_tokens, worker.cached_prefix_tokens
            )

            # Determine source tier based on cache presence
            tier = TierKind.DEVICE if prefix_len > 0 else TierKind.HOST_DRAM
            payload_bytes = prefix_len * 256  # ~256 bytes per token

            cost_res = self.cost_estimator.estimate(
                token_count=prefix_len if prefix_len > 0 else total_tokens,
                payload_bytes=payload_bytes,
                source_tier=tier,
                target_tier=TierKind.DEVICE,
                concurrent_transfers=worker.active_transfers,
            )

            # Apply Worker Load Penalty
            load_penalty_ms = worker.active_transfers * 0.1
            net_gain = (
                cost_res.t_recompute_seconds - cost_res.t_cache_seconds
            ) * 1000.0 - load_penalty_ms

            if net_gain > best_gain:
                best_gain = net_gain
                best_node = worker
                best_prefix_len = prefix_len
                best_cost_res = cost_res

        is_hit = best_prefix_len > 0 and best_gain > 0

        return RoutingDecision(
            selected_node_id=best_node.node_id,
            shared_prefix_len=best_prefix_len,
            expected_gain_ms=max(0.0, best_gain),
            is_cache_hit=is_hit,
            cost_detail=best_cost_res
            or self.cost_estimator.estimate(
                total_tokens, total_tokens * 256, TierKind.HOST_DRAM, TierKind.DEVICE
            ),
        )
