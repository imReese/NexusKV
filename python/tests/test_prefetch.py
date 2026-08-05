from __future__ import annotations

import unittest

from nexuskv.execution.prefetch import SpeculativePrefetchEngine


class TestSpeculativePrefetchEngine(unittest.TestCase):
    def test_submit_intent_prefetch_allocates_blocks(self) -> None:
        engine = SpeculativePrefetchEngine()
        prefix = list(range(100))
        predicted = list(range(100, 150))

        task = engine.submit_intent_prefetch(
            task_id="task_prefetch_001",
            prefix_tokens=prefix,
            predicted_suffix_tokens=predicted,
            target_tier="HBM",
        )

        self.assertEqual(task.task_id, "task_prefetch_001")
        self.assertEqual(task.status, "COMPLETED")
        self.assertTrue(len(task.allocated_block_ids) > 0)
        self.assertIsNotNone(engine.get_task("task_prefetch_001"))


if __name__ == "__main__":
    unittest.main()
