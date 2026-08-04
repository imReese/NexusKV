import unittest

from nexuskv.execution.policy import ExecutionPolicy, QuotaAdmissionPolicy, PlaceholderMode
from nexuskv.execution.quota import QuotaTracker


class TestQuotaAdmission(unittest.TestCase):
    def test_quota_admission_policy_check(self):
        policy = QuotaAdmissionPolicy(
            mode=PlaceholderMode.ENFORCED,
            max_payload_bytes=1000,
            max_entries=5,
            max_concurrent_transfers=2,
            max_pinned_dram_bytes=500,
        )

        # Below limits
        allowed, detail = policy.check_admission(
            payload_bytes=500,
            current_entries=2,
            current_transfers=1,
            current_pinned_bytes=200,
        )
        self.assertTrue(allowed)
        self.assertIsNone(detail)

        # Exceed payload limit
        allowed, detail = policy.check_admission(
            payload_bytes=1200,
            current_entries=2,
            current_transfers=1,
            current_pinned_bytes=200,
        )
        self.assertFalse(allowed)
        self.assertIn("max_payload_bytes", detail)

        # Exceed entries limit
        allowed, detail = policy.check_admission(
            payload_bytes=500,
            current_entries=5,
            current_transfers=1,
            current_pinned_bytes=200,
        )
        self.assertFalse(allowed)
        self.assertIn("max_entries", detail)

    def test_quota_tracker_thread_safety_and_reset(self):
        tracker = QuotaTracker()
        self.assertEqual(tracker.active_entries, 0)
        self.assertEqual(tracker.active_payload_bytes, 0)

        tracker.add_entry(100)
        tracker.add_entry(200)
        self.assertEqual(tracker.active_entries, 2)
        self.assertEqual(tracker.active_payload_bytes, 300)

        tracker.start_transfer(50)
        self.assertEqual(tracker.active_transfers, 1)
        self.assertEqual(tracker.active_pinned_bytes, 50)

        tracker.finish_transfer(50)
        self.assertEqual(tracker.active_transfers, 0)
        self.assertEqual(tracker.active_pinned_bytes, 0)

        tracker.reset()
        self.assertEqual(tracker.active_entries, 0)
        self.assertEqual(tracker.active_payload_bytes, 0)


if __name__ == "__main__":
    unittest.main()
