#!/usr/bin/env python3
"""NexusKV Benchmark & Stress Test Runner.

Executes dual-dimension performance profiling (QPS/RPS & GB Saved),
hardware device & architecture detection, multi-size KV Tensor payload matrix,
and high-concurrency cluster stress testing.
"""

from __future__ import annotations

import sys
import time
from pathlib import Path

# Add python directory to sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "python"))

from nexuskv.benchmarks.runner import BenchmarkStrategyRunner, BenchmarkStrategy
from nexuskv.benchmarks.stress import ClusterStressTestRunner
from nexuskv.benchmarks.trace import BenchmarkTraceGenerator
from nexuskv.benchmarks.system_info import get_system_hardware_info


def main() -> None:
    print("=" * 70)
    print("      NexusKV Industrial Dual-Dimension Benchmark Suite")
    print("=" * 70)

    # 1. Hardware Device & Memory Architecture Detection
    sys_info = get_system_hardware_info()
    print("\n[Hardware & System Environment Info]")
    print(f"• Operating System       : {sys_info['os']}")
    print(f"• Python Runtime         : Python {sys_info['python_version']}")
    print(f"• CPU Hardware           : {sys_info['cpu']}")
    print(f"• Host RAM Capacity      : {sys_info['ram_gb']}")
    print(f"• Accelerator Device     : {sys_info['device_class']}")
    print(f"• Memory Architecture    : {sys_info['memory_architecture']}")

    # 2. Workload Trace & Single-Trace Dual-Dimension Evaluation
    print("\n[1/3] Evaluating Standard Workload Trace & Decision Latency...")
    generator = BenchmarkTraceGenerator(seed=2026)
    standard_trace = generator.generate_synthetic_trace(num_requests=30)
    runner = BenchmarkStrategyRunner()

    start = time.perf_counter()
    recompute_report = runner.run_trace(standard_trace, BenchmarkStrategy.PURE_RECOMPUTE)
    hit_report = runner.run_trace(standard_trace, BenchmarkStrategy.HIT_DRIVEN)
    nexus_report = runner.run_trace(standard_trace, BenchmarkStrategy.NEXUSKV_COST_BASED)
    duration = time.perf_counter() - start

    total_bytes_saved = sum(len(r.tokens) * 256 for r in standard_trace.requests)
    total_gb_saved = total_bytes_saved / (1024 ** 3)

    print(f"\n--- Strategy Comparison Results ({len(standard_trace)} Requests) ---")
    print(f"1. Pure Recompute         : Recomputes={recompute_report.recomputations}, Effective Gain={recompute_report.aggregate_effective_gain_ms:.2f}ms")
    print(f"2. Hit-Driven Reuse       : Hits={hit_report.total_hits}, Useful Reuses={hit_report.useful_reuses}, Gain={hit_report.aggregate_effective_gain_ms:.2f}ms")
    print(f"3. NexusKV Cost-Based     : Hits={nexus_report.total_hits}, Useful Reuses={nexus_report.useful_reuses}, Rejected Unprofitable={nexus_report.rejected_unprofitable_hits}, Gain={nexus_report.aggregate_effective_gain_ms:.2f}ms")

    print(f"\n--- Real Wall-Clock Decision Latency Breakdown (Microseconds) ---")
    print(f"• P50 End-to-End Decision Latency : {nexus_report.p50_e2e_us:.2f} us")
    print(f"• P90 End-to-End Decision Latency : {nexus_report.p90_e2e_us:.2f} us")
    print(f"• P99 End-to-End Decision Latency : {nexus_report.p99_e2e_us:.2f} us")
    print(f"• P50 Radix Tree Lookup Latency   : {nexus_report.p50_lookup_us:.2f} us")
    print(f"• P99 Radix Tree Lookup Latency   : {nexus_report.p99_lookup_us:.2f} us")
    print(f"• P50 Memory Handle Mount Latency  : {nexus_report.p50_materialize_us:.2f} us")

    print(f"\n--- Dual-Dimension Metrics Summary ---")
    print(f"• Decision Rate (QPS)      : {30 / duration:.1f} req/sec ({duration * 1000 / 30:.3f} ms/req decision latency)")
    print(f"• Token Processing Rate    : {nexus_report.tokens_per_sec:.1f} tokens/sec")
    print(f"• Payload Capacity Saved   : {total_gb_saved:.3f} GB KV Tensors ({total_bytes_saved / (1024**2):.2f} MB)")
    print(f"• Effective Bandwidth Gain : {total_gb_saved / duration:.2f} GB/sec equivalent compute offload")

    # 3. Multi-Size KV Tensor Payload Matrix Benchmark
    print("\n[2/3] Evaluating Multi-Size KV Tensor Payload Matrix (Saved Capacity & Decision Latency)...")
    matrix_traces = generator.generate_multi_size_matrix_traces()
    print("-" * 85)
    print(f"{'Payload Tier':<35} | {'Hits':<5} | {'KV Saved':<10} | {'Gain (ms)':<10} | {'Decision Lat (μs)':<18}")
    print("-" * 85)

    for label, matrix_trace in matrix_traces.items():
        t_start = time.perf_counter_ns()
        rpt = runner.run_trace(matrix_trace, BenchmarkStrategy.NEXUSKV_COST_BASED)
        t_dur_us = (time.perf_counter_ns() - t_start) / 1000.0
        
        bytes_saved = sum(len(r.tokens) * 256 for r in matrix_trace.requests)
        if bytes_saved >= (1024 ** 3):
            kv_saved_str = f"{bytes_saved / (1024 ** 3):.2f} GB"
        else:
            kv_saved_str = f"{bytes_saved / (1024 ** 2):.2f} MB"
            
        avg_decision_lat_us = t_dur_us / len(matrix_trace.requests) if matrix_trace.requests else 0.0
        print(f"{label:<35} | {rpt.useful_reuses:<5} | {kv_saved_str:<10} | {rpt.aggregate_effective_gain_ms:<10.2f} | {avg_decision_lat_us:<18.2f}")
    print("-" * 85)

    # 4. Physical Transport & Pooled Memory Microbenchmark (H2D / D2H / SHM Zero-Copy)
    print("\n[3/4] Running Physical Transport & Memory Microbenchmark (H2D / D2H / SHM)...")
    from nexuskv.benchmarks.physical_transport_bench import PhysicalTransportBenchmarkSuite
    phys_suite = PhysicalTransportBenchmarkSuite()
    phys_results = phys_suite.run_full_physical_suite()

    print("-" * 88)
    print(f"{'Operation':<36} | {'Payload':<8} | {'Latency':<12} | {'Physical Transfer Rate':<22}")
    print("-" * 88)
    for res in phys_results:
        size_str = f"{res.payload_size_mb:.0f} MB"
        if res.is_zero_copy:
            rate_str = f"Zero-Copy ({res.latency_us:.2f} μs)"
            lat_str = f"{res.latency_us:.2f} μs"
        else:
            rate_str = f"{res.bandwidth_gbs:.2f} GB/s"
            lat_str = f"{res.duration_ms:.2f} ms"
        print(f"{res.operation:<36} | {size_str:<8} | {lat_str:<12} | {rate_str:<22}")
    print("-" * 88)

    # 5. Cluster Stress Test & Memory Leak Check
    print("\n[4/4] Running High-Concurrency Cluster Stress Test & Memory Leak Check...")
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

    print("\n" + "=" * 70)
    print("             NexusKV Industrial Benchmark Execution Complete!")
    print("=" * 70)


if __name__ == "__main__":
    main()
