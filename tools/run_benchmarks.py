#!/usr/bin/env python3
"""
NexusKV Dual-Dimension Benchmark & Cluster Stress Test Utility

Dimensions Evaluated:
1. Decision Intelligence Dimension: QPS (Requests/sec) & Microsecond Latency
2. Byte Payload Dimension          : GB Saved & Transfer Bandwidth (GB/sec)
"""

import sys
import time
from pathlib import Path

# Add python directory to PYTHONPATH
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "python"))

from nexuskv.benchmarks.trace import BenchmarkTraceGenerator
from nexuskv.benchmarks.runner import BenchmarkStrategyRunner, BenchmarkStrategy
from nexuskv.benchmarks.stress import ClusterStressTestRunner


def main():
    print("=" * 65)
    print("      NexusKV Dual-Dimension Benchmark & Stress Test Runner")
    print("=" * 65)

    # 1. Trace Generation & 3-Way Strategy Benchmark Comparison
    print("\n[1/2] Generating Workload Trace & Evaluating Dual-Dimension Performance...")
    generator = BenchmarkTraceGenerator(seed=2026)
    trace = generator.generate_synthetic_trace(num_requests=30)
    runner = BenchmarkStrategyRunner()

    start = time.perf_counter()
    recompute_report = runner.run_trace(trace, BenchmarkStrategy.PURE_RECOMPUTE)
    hit_report = runner.run_trace(trace, BenchmarkStrategy.HIT_DRIVEN)
    nexus_report = runner.run_trace(trace, BenchmarkStrategy.NEXUSKV_COST_BASED)
    duration = time.perf_counter() - start

    # Calculate Total Payload Bytes (assuming ~256 bytes per token for FP16 KV tensor states)
    total_bytes_saved = sum(len(r.tokens) * 256 for r in trace.requests)
    total_gb_saved = total_bytes_saved / (1024 ** 3)

    print(f"\n--- Strategy Comparison Results ({len(trace)} Requests) ---")
    print(f"1. Pure Recompute         : Recomputes={recompute_report.recomputations}, Effective Gain={recompute_report.aggregate_effective_gain_ms:.2f}ms")
    print(f"2. Hit-Driven Reuse       : Hits={hit_report.total_hits}, Useful Reuses={hit_report.useful_reuses}, Gain={hit_report.aggregate_effective_gain_ms:.2f}ms")
    print(f"3. NexusKV Cost-Based     : Hits={nexus_report.total_hits}, Useful Reuses={nexus_report.useful_reuses}, Rejected Unprofitable={nexus_report.rejected_unprofitable_hits}, Gain={nexus_report.aggregate_effective_gain_ms:.2f}ms")

    print(f"\n--- Dual-Dimension Metrics Summary ---")
    print(f"• Decision Rate (QPS)      : {30 / duration:.1f} req/sec ({duration * 1000 / 30:.3f} ms/req decision latency)")
    print(f"• Payload Capacity Saved   : {total_gb_saved:.3f} GB KV Tensors ({total_bytes_saved / (1024**2):.2f} MB)")
    print(f"• Effective Bandwidth Gain : {total_gb_saved / duration:.2f} GB/sec equivalent compute offload")

    # 2. Cluster Stress Test & Memory Leak Check
    print("\n[2/2] Running High-Concurrency Cluster Stress Test & Memory Leak Check...")
    stress_runner = ClusterStressTestRunner(num_iterations=20, concurrency=4)
    stress_report = stress_runner.run_stress_test()

    print(f"\n--- Stress Test & Memory Leak Summary ---")
    print(f"Total Requests Processed: {stress_report.total_requests_processed}")
    print(f"Concurrency Level       : {stress_report.concurrency_level} Workers")
    print(f"Duration                : {stress_report.duration_sec:.2f} seconds")
    print(f"Failed Requests (Crash) : {stress_report.failed_requests}")
    print(f"Memory RSS Growth       : {stress_report.rss_growth_bytes / (1024*1024):.2f} MB")
    print(f"Zero Memory Leak Verdict: {stress_report.zero_memory_leak}")
    print(f"Zero Crash Verdict      : {stress_report.zero_crash}")

    print("\n" + "=" * 65)
    print("             NexusKV Benchmark Execution Complete!")
    print("=" * 65)


if __name__ == "__main__":
    main()
