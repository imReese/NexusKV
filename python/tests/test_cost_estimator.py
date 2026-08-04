import unittest

from nexuskv.contracts.generated import TierKind, TransferBackend
from nexuskv.planner.cost import CostEstimator, BandwidthConfig


class TestCostEstimator(unittest.TestCase):
    def test_cost_estimator_profitable_reuse(self):
        estimator = CostEstimator(
            time_per_token_prefill_sec=1.0e-4,  # Slow prefill
            lookup_overhead_sec=1.0e-5,
        )
        # Long context reuse (10,000 tokens) from Host DRAM
        res = estimator.estimate(
            token_count=10000,
            payload_bytes=10 * 1024 * 1024,  # 10MB
            source_tier=TierKind.HOST_DRAM,
            target_tier=TierKind.DEVICE,
        )
        self.assertTrue(res.is_profitable)
        self.assertGreater(res.effective_gain_seconds, 0.0)
        self.assertIn("Profitable reuse", res.explanation)

    def test_cost_estimator_unprofitable_reuse(self):
        estimator = CostEstimator(
            time_per_token_prefill_sec=1.0e-7,  # Fast prefill
            lookup_overhead_sec=0.1,  # High lookup overhead
        )
        # Short context reuse (10 tokens) from slow storage
        res = estimator.estimate(
            token_count=10,
            payload_bytes=1 * 1024 * 1024,
            source_tier=TierKind.OBJECT_STORE,
            target_tier=TierKind.DEVICE,
        )
        self.assertFalse(res.is_profitable)
        self.assertLessEqual(res.effective_gain_seconds, 0.0)
        self.assertIn("Unprofitable reuse", res.explanation)


if __name__ == "__main__":
    unittest.main()
