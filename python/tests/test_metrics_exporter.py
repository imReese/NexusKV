import unittest

from nexuskv.metrics.exporter import NexusMetricsCollector


class TestMetricsExporter(unittest.TestCase):
    def test_metrics_collection_and_hit_rate(self):
        collector = NexusMetricsCollector()
        collector.record_cache_hit()
        collector.record_cache_hit()
        collector.record_cache_miss()

        collector.record_fail_open()
        collector.record_quota_rejection()
        collector.record_transfer(1024 * 1024 * 1024, duration_sec=0.5)

        snap = collector.snapshot()
        self.assertEqual(snap.cache_hits, 2)
        self.assertEqual(snap.cache_misses, 1)
        self.assertAlmostEqual(snap.hit_rate, 2 / 3, places=4)
        self.assertEqual(snap.fail_open_events, 1)
        self.assertEqual(snap.quota_rejections, 1)
        self.assertGreater(snap.last_transfer_bandwidth_gbps, 0.0)


if __name__ == "__main__":
    unittest.main()
