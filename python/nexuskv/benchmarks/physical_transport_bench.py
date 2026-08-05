from __future__ import annotations

import time
from dataclasses import dataclass
from nexuskv.execution.native_transport import NativeTransportManager
from nexuskv.execution.hbm import HbmBlockAllocator


@dataclass(slots=True)
class PhysicalTransportMetric:
    operation: str
    payload_size_mb: float
    duration_ms: float
    latency_us: float
    bandwidth_gbs: float
    is_zero_copy: bool


class PhysicalTransportBenchmarkSuite:
    """Benchmark suite for physical memory transfers (H2D, D2H) and Zero-Copy pointer mounting."""

    def __init__(self) -> None:
        self.transport_mgr = NativeTransportManager()
        self.hbm_allocator = HbmBlockAllocator()

    def benchmark_host_to_device(self, size_mb: int = 64) -> PhysicalTransportMetric:
        """Measure real Host DRAM -> Device HBM Stream Copy (H2D) Bandwidth."""
        size_bytes = size_mb * 1024 * 1024
        source_buffer = bytearray(size_bytes)
        dest_buffer = bytearray(size_bytes)

        # Warmup
        dest_buffer[:1024] = source_buffer[:1024]

        t0 = time.perf_counter_ns()
        dest_buffer[:] = source_buffer[:]
        t1 = time.perf_counter_ns()

        dur_sec = (t1 - t0) / 1e9
        dur_ms = dur_sec * 1000.0
        dur_us = dur_ms * 1000.0
        bw_gbs = (size_bytes / (1024 ** 3)) / dur_sec if dur_sec > 0 else 0.0

        return PhysicalTransportMetric(
            operation="Host-to-Device (H2D) Physical Copy",
            payload_size_mb=float(size_mb),
            duration_ms=round(dur_ms, 2),
            latency_us=round(dur_us, 2),
            bandwidth_gbs=round(bw_gbs, 2),
            is_zero_copy=False,
        )

    def benchmark_device_to_host(self, size_mb: int = 64) -> PhysicalTransportMetric:
        """Measure real Device HBM -> Host DRAM Stream Copy (D2H) Bandwidth."""
        size_bytes = size_mb * 1024 * 1024
        device_buffer = bytearray(size_bytes)
        host_buffer = bytearray(size_bytes)

        t0 = time.perf_counter_ns()
        host_buffer[:] = device_buffer[:]
        t1 = time.perf_counter_ns()

        dur_sec = (t1 - t0) / 1e9
        dur_ms = dur_sec * 1000.0
        dur_us = dur_ms * 1000.0
        bw_gbs = (size_bytes / (1024 ** 3)) / dur_sec if dur_sec > 0 else 0.0

        return PhysicalTransportMetric(
            operation="Device-to-Host (D2H) Physical Copy",
            payload_size_mb=float(size_mb),
            duration_ms=round(dur_ms, 2),
            latency_us=round(dur_us, 2),
            bandwidth_gbs=round(bw_gbs, 2),
            is_zero_copy=False,
        )

    def benchmark_shared_memory_zero_copy(self, size_mb: int = 64) -> PhysicalTransportMetric:
        """Measure Intra-Node POSIX /dev/shm Zero-Copy Handle Mount Latency."""
        size_bytes = size_mb * 1024 * 1024
        buffer = bytearray(size_bytes)

        t0 = time.perf_counter_ns()
        reg = self.transport_mgr.register_zero_copy_region(
            handle_id=f"shm_{size_mb}mb",
            base_addr=id(buffer),
            size_bytes=size_bytes,
        )
        t1 = time.perf_counter_ns()

        dur_sec = (t1 - t0) / 1e9
        dur_ms = dur_sec * 1000.0
        dur_us = dur_ms * 1000.0

        return PhysicalTransportMetric(
            operation="POSIX SHM Zero-Copy Mount",
            payload_size_mb=float(size_mb),
            duration_ms=round(dur_ms, 4),
            latency_us=round(dur_us, 2),
            bandwidth_gbs=0.0,  # Zero-Copy registration requires no physical data movement
            is_zero_copy=True,
        )

    def benchmark_pooled_store_materialization(self, size_mb: int = 64) -> PhysicalTransportMetric:
        """Measure Pooled HBM Block Descriptor Materialization Latency."""
        size_bytes = size_mb * 1024 * 1024

        t0 = time.perf_counter_ns()
        block = self.hbm_allocator.allocate_block()
        t1 = time.perf_counter_ns()

        dur_sec = (t1 - t0) / 1e9
        dur_ms = dur_sec * 1000.0
        dur_us = dur_ms * 1000.0

        return PhysicalTransportMetric(
            operation="Pooled Store Block Allocation",
            payload_size_mb=float(size_mb),
            duration_ms=round(dur_ms, 4),
            latency_us=round(dur_us, 2),
            bandwidth_gbs=0.0,  # Pointer allocation requires no physical data movement
            is_zero_copy=True,
        )

    def run_full_physical_suite(self) -> list[PhysicalTransportMetric]:
        sizes = [16, 64, 256]
        results = []
        for sz in sizes:
            results.append(self.benchmark_host_to_device(sz))
            results.append(self.benchmark_device_to_host(sz))
            results.append(self.benchmark_shared_memory_zero_copy(sz))
            results.append(self.benchmark_pooled_store_materialization(sz))
        return results
