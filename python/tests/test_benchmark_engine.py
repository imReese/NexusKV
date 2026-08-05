import unittest

from nexuskv.benchmarks.runner import BenchmarkStrategy, BenchmarkStrategyRunner
from nexuskv.benchmarks.trace import BenchmarkTraceGenerator


class TestBenchmarkEngine(unittest.TestCase):
    def test_synthetic_trace_generation(self):
        generator = BenchmarkTraceGenerator(seed=123)
        trace = generator.generate_synthetic_trace(num_requests=10)
        self.assertEqual(len(trace), 10)
        self.assertEqual(trace.name, "synthetic_mixed_workload")
        self.assertTrue(all(r.context_length > 0 for r in trace.requests))

    def test_three_way_strategy_comparison(self):
        generator = BenchmarkTraceGenerator(seed=456)
        trace = generator.generate_synthetic_trace(num_requests=20)

        runner = BenchmarkStrategyRunner()

        recompute_report = runner.run_trace(trace, BenchmarkStrategy.PURE_RECOMPUTE)
        hit_driven_report = runner.run_trace(trace, BenchmarkStrategy.HIT_DRIVEN)
        nexus_report = runner.run_trace(trace, BenchmarkStrategy.NEXUSKV_COST_BASED)

        self.assertEqual(recompute_report.useful_reuses, 0)
        self.assertEqual(recompute_report.aggregate_effective_gain_ms, 0.0)

        self.assertGreaterEqual(hit_driven_report.total_hits, 0)
        self.assertGreaterEqual(nexus_report.total_hits, 0)

        # NexusKV Cost-Based Report must export clean JSON
        json_report = nexus_report.to_json()
        self.assertIn("nexuskv_cost_based", json_report)
        self.assertIn("aggregate_effective_gain_ms", json_report)


if __name__ == "__main__":
    unittest.main()
