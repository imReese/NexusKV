import unittest

from nexuskv.benchmarks.stress import ClusterStressTestRunner


class TestStressSuite(unittest.TestCase):
    def test_cluster_stress_runner_execution_and_reporting(self):
        runner = ClusterStressTestRunner(num_iterations=5, concurrency=2)
        report = runner.run_stress_test()

        self.assertEqual(report.failed_requests, 0)
        self.assertTrue(report.zero_crash)
        self.assertGreater(report.total_requests_processed, 0)
        self.assertIn("zero_crash", report.to_json())


if __name__ == "__main__":
    unittest.main()
