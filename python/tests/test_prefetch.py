from __future__ import annotations

import time
import unittest

from nexuskv.execution.prefetch import SpeculativePrefetchEngine


class TestSpeculativePrefetchEngine(unittest.TestCase):
    def test_submit_intent_prefetch_async_pipeline(self) -> None:
        engine = SpeculativePrefetchEngine()
        self.addCleanup(engine.shutdown)
        prefix = list(range(100))
        predicted = list(range(100, 150))

        task = engine.submit_intent_prefetch(
            task_id="task_prefetch_001",
            prefix_tokens=prefix,
            predicted_suffix_tokens=predicted,
            target_tier="HBM",
        )

        self.assertEqual(task.task_id, "task_prefetch_001")
        # Ensure it doesn't block and returns a valid state immediately
        self.assertIn(task.status, ("PENDING", "PREFETCHING", "COMPLETED"))

        # Wait for the background daemon thread to finish the async prefetch simulation
        # 150 tokens = 1 block needed, which simulates 0.05s latency
        for _ in range(20):
            if task.status == "COMPLETED":
                break
            time.sleep(0.05)

        self.assertEqual(task.status, "COMPLETED")
        self.assertTrue(len(task.allocated_block_ids) > 0)
        self.assertIsNotNone(engine.get_task("task_prefetch_001"))


if __name__ == "__main__":
    unittest.main()
