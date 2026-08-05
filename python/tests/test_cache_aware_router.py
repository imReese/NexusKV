from __future__ import annotations

import unittest
from nexuskv.planner.router import CacheAwareRouter, WorkerNodeState


class TestCacheAwareRouter(unittest.TestCase):
    def setUp(self) -> None:
        self.router = CacheAwareRouter()
        self.common_prefix = [101, 102, 103, 104, 105, 106, 107, 108]
        self.prompt_tokens = self.common_prefix + [201, 202, 203]

    def test_selects_worker_with_longest_cached_prefix(self) -> None:
        worker1 = WorkerNodeState(
            node_id="worker-gpu-01",
            address="192.168.1.1:8080",
            cached_prefix_tokens=[101, 102, 103],  # 3 matching tokens
        )
        worker2 = WorkerNodeState(
            node_id="worker-gpu-02",
            address="192.168.1.2:8080",
            cached_prefix_tokens=self.common_prefix,  # 8 matching tokens
        )

        decision = self.router.select_best_worker(self.prompt_tokens, [worker1, worker2])

        self.assertEqual(decision.selected_node_id, "worker-gpu-02")
        self.assertEqual(decision.shared_prefix_len, 8)
        self.assertTrue(decision.is_cache_hit)
        self.assertGreater(decision.expected_gain_ms, 0.0)

    def test_considers_worker_active_transfer_penalty(self) -> None:
        worker1 = WorkerNodeState(
            node_id="worker-busy-01",
            address="192.168.1.1:8080",
            active_transfers=50,  # High load penalty
            cached_prefix_tokens=self.common_prefix,
        )
        worker2 = WorkerNodeState(
            node_id="worker-idle-02",
            address="192.168.1.2:8080",
            active_transfers=0,   # Idle worker
            cached_prefix_tokens=self.common_prefix,
        )

        decision = self.router.select_best_worker(self.prompt_tokens, [worker1, worker2])

        self.assertEqual(decision.selected_node_id, "worker-idle-02")


if __name__ == "__main__":
    unittest.main()
