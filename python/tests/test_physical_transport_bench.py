from __future__ import annotations

import unittest

from nexuskv.benchmarks.physical_transport_bench import PhysicalTransportBenchmarkSuite


class TestPhysicalTransportBenchmarkSuite(unittest.TestCase):
    def setUp(self) -> None:
        self.suite = PhysicalTransportBenchmarkSuite()

    def test_h2d_benchmark_returns_valid_metric(self) -> None:
        metric = self.suite.benchmark_host_to_device(16)
        self.assertEqual(metric.operation, "Host-to-Device (H2D) Physical Copy")
        self.assertEqual(metric.payload_size_mb, 16.0)
        self.assertGreater(metric.bandwidth_gbs, 0.0)
        self.assertGreater(metric.latency_us, 0.0)

    def test_d2h_benchmark_returns_valid_metric(self) -> None:
        metric = self.suite.benchmark_device_to_host(16)
        self.assertEqual(metric.operation, "Device-to-Host (D2H) Physical Copy")
        self.assertEqual(metric.payload_size_mb, 16.0)
        self.assertGreater(metric.bandwidth_gbs, 0.0)

    def test_shm_zero_copy_benchmark_returns_valid_metric(self) -> None:
        metric = self.suite.benchmark_shared_memory_zero_copy(16)
        self.assertEqual(metric.operation, "POSIX SHM Zero-Copy Mount")
        self.assertEqual(metric.payload_size_mb, 16.0)

    def test_full_physical_suite(self) -> None:
        results = self.suite.run_full_physical_suite()
        self.assertEqual(len(results), 12)


if __name__ == "__main__":
    unittest.main()
