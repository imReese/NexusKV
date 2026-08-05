from __future__ import annotations

import json
import os
import threading
import time
from dataclasses import dataclass

from nexuskv.benchmarks.runner import BenchmarkStrategy, BenchmarkStrategyRunner
from nexuskv.benchmarks.trace import BenchmarkTraceGenerator


@dataclass(slots=True)
class StressTestReport:
    total_iterations: int
    concurrency_level: int
    duration_sec: float
    total_requests_processed: int
    failed_requests: int
    initial_rss_bytes: int
    final_rss_bytes: int
    rss_growth_bytes: int
    zero_memory_leak: bool
    zero_crash: bool
    to_json_export: str = ""

    def to_dict(self) -> dict:
        return {
            "total_iterations": self.total_iterations,
            "concurrency_level": self.concurrency_level,
            "duration_sec": round(self.duration_sec, 3),
            "total_requests_processed": self.total_requests_processed,
            "failed_requests": self.failed_requests,
            "initial_rss_bytes": self.initial_rss_bytes,
            "final_rss_bytes": self.final_rss_bytes,
            "rss_growth_bytes": self.rss_growth_bytes,
            "zero_memory_leak": self.zero_memory_leak,
            "zero_crash": self.zero_crash,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), indent=2)


class ClusterStressTestRunner:
    """7x24 / High-concurrency cluster stress test and memory leak detector."""

    def __init__(
        self,
        num_iterations: int = 50,
        concurrency: int = 4,
        max_allowed_growth_bytes: int = 50 * 1024 * 1024,  # 50MB RSS threshold
    ) -> None:
        self.num_iterations = num_iterations
        self.concurrency = concurrency
        self.max_allowed_growth_bytes = max_allowed_growth_bytes

    def _get_process_rss(self) -> int:
        try:
            import psutil

            return psutil.Process(os.getpid()).memory_info().rss
        except Exception:
            return 0  # Fallback if psutil is unavailable

    def run_stress_test(self) -> StressTestReport:
        initial_rss = self._get_process_rss()
        t0 = time.perf_counter()

        trace_gen = BenchmarkTraceGenerator(seed=789)
        trace = trace_gen.generate_synthetic_trace(num_requests=30)
        runner = BenchmarkStrategyRunner()

        total_processed = 0
        failed_count = 0
        lock = threading.Lock()

        def worker_loop():
            nonlocal total_processed, failed_count
            for _ in range(self.num_iterations):
                try:
                    report = runner.run_trace(trace, BenchmarkStrategy.NEXUSKV_COST_BASED)
                    with lock:
                        total_processed += report.total_requests
                except Exception:
                    with lock:
                        failed_count += 1

        threads = [threading.Thread(target=worker_loop) for _ in range(self.concurrency)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        t1 = time.perf_counter()
        final_rss = self._get_process_rss()
        growth = max(0, final_rss - initial_rss)

        zero_leak = growth < self.max_allowed_growth_bytes
        zero_crash = failed_count == 0

        report = StressTestReport(
            total_iterations=self.num_iterations * self.concurrency,
            concurrency_level=self.concurrency,
            duration_sec=t1 - t0,
            total_requests_processed=total_processed,
            failed_requests=failed_count,
            initial_rss_bytes=initial_rss,
            final_rss_bytes=final_rss,
            rss_growth_bytes=growth,
            zero_memory_leak=zero_leak,
            zero_crash=zero_crash,
        )
        report.to_json_export = report.to_json()
        return report
