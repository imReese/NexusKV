from __future__ import annotations

import unittest
from nexuskv.integrations.vllm_integration import NexusKVCacheAwareMiddleware
from nexuskv.planner.router import WorkerNodeState


class TestNexusKVCacheAwareMiddleware(unittest.TestCase):
    def setUp(self) -> None:
        self.middleware = NexusKVCacheAwareMiddleware()
        self.worker1 = WorkerNodeState(
            node_id="gpu-node-01",
            address="192.168.1.1:8080",
            cached_prefix_tokens=[101, 102, 103, 104, 105],
        )
        self.worker2 = WorkerNodeState(
            node_id="gpu-node-02",
            address="192.168.1.2:8080",
            cached_prefix_tokens=[101, 102, 103, 104, 105, 106, 107, 108],
        )
        self.middleware.register_worker_node(self.worker1)
        self.middleware.register_worker_node(self.worker2)

    def test_turnkey_process_inference_request_end_to_end(self) -> None:
        prompt_tokens = [101, 102, 103, 104, 105, 106, 107, 108, 999]
        decision, transfer_res = self.middleware.process_inference_request(prompt_tokens)

        self.assertEqual(decision.selected_node_id, "gpu-node-02")
        self.assertEqual(decision.shared_prefix_len, 8)
        self.assertTrue(decision.is_cache_hit)
        
        self.assertIsNotNone(transfer_res)
        if transfer_res:
            self.assertTrue(transfer_res.is_success)
            self.assertGreater(transfer_res.payload_bytes, 0)
            self.assertIsNotNone(transfer_res.hbm_block_id)

        # Verify cluster sync registry updated
        cluster_map = self.middleware.sync_manager.get_cluster_cache_map()
        self.assertIn("gpu-node-02", cluster_map)


if __name__ == "__main__":
    unittest.main()
