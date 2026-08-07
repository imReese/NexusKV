#!/usr/bin/env python3
"""NexusKV Standalone CPU-Only Topology Matrix & Read/Write Precision Test Suite.

Runs on any CPU-only machine without GPU cards or LLM inference engines.
Tests bit-level KV Cache payload integrity (SHA-256) and topology resolution.
"""

from __future__ import annotations

import hashlib
import os
import sys
import time
from dataclasses import dataclass

# Add python directory to sys.path for direct execution
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "python")))

from nexuskv.contracts.generated import (
    SCHEMA_VERSION,
    AttentionStateDescriptor,
    BufferKind,
    CacheEntry,
    CompatibilityFlag,
    DeviceClass,
    EngineFamily,
    EntryIdentity,
    EntryLocation,
    EntryVersion,
    Granularity,
    KeyIdentity,
    LayoutMetadata,
    MaterializationCapability,
    MaterializationProfile,
    PolicyHint,
    QuantizationMetadata,
    StateSemanticType,
    TensorRole,
    TensorSpec,
    TierKind,
    TransferBackend,
    TransferCapability,
    TransferPath,
)
from nexuskv.execution.runner import BaselineExecutionRunner, ParallelTopologyPolicy
from nexuskv.execution.store import InMemoryEntryStore


@dataclass(slots=True)
class TopologyTestResult:
    name: str
    pp_size: int
    tp_size: int
    cp_size: int
    ep_size: int
    read_write_sha256_matched: bool
    latency_us: float
    status: str


def run_precision_and_topology_suite() -> bool:
    print("==========================================================")
    print("       NexusKV CPU-Only Standalone Verification Suite     ")
    print("==========================================================")
    print(" Hardware Mode : CPU-Only (No GPU / No Engine Required)")
    print(" Data Check    : Bit-Exact SHA-256 Read/Write Precision")
    print(" Topologies    : Single, TP=8, PP=4, CP=4, MoE Hybrid")
    print("==========================================================\n")

    # 1. Bit-Level Read/Write Precision Check
    print("[1/2] Verifying Bit-Level Read/Write Data Precision (SHA-256)...")
    store = InMemoryEntryStore()

    descriptor = AttentionStateDescriptor(
        schema_version=SCHEMA_VERSION,
        descriptor_id="standalone-verify-page",
        engine_family=EngineFamily.UNKNOWN,
        semantic_type=StateSemanticType.MHA_KV,
        granularity=Granularity.PAGE,
        tensor_specs=[
            TensorSpec(name="key", role=TensorRole.KEY, dtype="float16", shape=["16", "64"]),
            TensorSpec(name="val", role=TensorRole.VALUE, dtype="float16", shape=["16", "64"]),
        ],
        quantization=QuantizationMetadata(scheme="none", bits=16, group_size=1),
        layout=LayoutMetadata(layout="interleaved", page_tokens=16, block_tokens=64, packed=True),
        compatibility_flags=[CompatibilityFlag.EXACT_REUSE],
        transfer_paths=[
            TransferPath(
                backend=TransferBackend.BASELINE_TRANSPORT,
                capabilities=[TransferCapability.HOST_TO_DEVICE],
            )
        ],
        materialization=MaterializationProfile(
            capabilities=[MaterializationCapability.FULL],
            tier_kinds=[TierKind.HOST_DRAM],
            device_classes=[DeviceClass.CPU],
            buffer_kinds=[BufferKind.HOST_PAGEABLE],
        ),
        layout_metadata={},
    )

    # Generate 1MB of deterministic pseudo-random binary payload (simulating Float16 KV Tensors)
    raw_kv_payload = bytes([((i * 31 + 17) % 256) for i in range(1024 * 1024)])
    original_sha256 = hashlib.sha256(raw_kv_payload).hexdigest()

    key = KeyIdentity(
        tenant="test_tenant",
        namespace="test_ns",
        model="qwen-72b",
        engine_family=EngineFamily.UNKNOWN,
        semantic_type=StateSemanticType.MHA_KV,
        tokens=[101, 102, 103],
        block_id=None,
        page_id=None,
    )
    entry = CacheEntry(
        identity=EntryIdentity(
            key=key,
            entry_id="payload_test_001",
            version=EntryVersion(generation=1, lineage="bit_exact_test"),
        ),
        descriptor=descriptor,
        location=EntryLocation(tier=TierKind.HOST_DRAM, locator="memory://payload_test_001"),
        policy_hint=PolicyHint(reusable=True, admission_hint="test", eviction_hint="default"),
    )

    t0 = time.perf_counter()
    store.put(entry, raw_kv_payload)
    record = store.get_identity(key)
    write_read_latency_us = (time.perf_counter() - t0) * 1_000_000.0

    if record is None or record.payload_handle is None:
        print(" ❌ Precision Test Failed: Retrieved payload record is None")
        return False

    retrieved_payload = record.payload_handle
    retrieved_sha256 = hashlib.sha256(retrieved_payload).hexdigest()
    precision_passed = original_sha256 == retrieved_sha256

    if precision_passed:
        print(
            f" ✅ Precision Test PASSED: SHA-256 {retrieved_sha256[:16]}... (100% Bit-Exact Match)"
        )
        print(f"    Write -> Read Roundtrip Latency: {write_read_latency_us:.2f} μs\n")
    else:
        print(
            f" ❌ Precision Test FAILED: Original {original_sha256[:16]} != {retrieved_sha256[:16]}"
        )
        return False

    # 2. Topology Matrix Verification
    print("[2/2] Evaluating Parallel Topology Resolution Matrix...")
    topologies_to_test = [
        ("Single Node Fast-Path", 1, 1, 1, 1),
        ("Tensor Parallel (TP=8)", 1, 8, 1, 1),
        ("Pipeline Parallel (PP=4)", 4, 1, 1, 1),
        ("Context Parallel (CP=4)", 1, 1, 4, 1),
        ("DeepSeek/Kimi MoE Hybrid (PP=2, TP=4, CP=2, EP=8)", 2, 4, 2, 8),
    ]

    results: list[TopologyTestResult] = []

    for name, pp, tp, cp, ep in topologies_to_test:
        os.environ["PIPELINE_PARALLEL_SIZE"] = str(pp)
        os.environ["TENSOR_PARALLEL_SIZE"] = str(tp)

        t_start = time.perf_counter()
        policies = ParallelTopologyPolicy.resolve_topology_policy(
            pp_size=pp, tp_size=tp, cp_size=cp, ep_size=ep
        )
        runner = BaselineExecutionRunner()
        runner_policies = runner.resolve_topology_policy()
        elapsed_us = (time.perf_counter() - t_start) * 1_000_000.0

        # Assert expected policy flags
        valid_pp = (pp > 1) == policies["enable_pp_min_prefix_lock"]
        valid_tp = (tp > 1) == policies["enable_tp_stride_alignment"]
        valid_cp = (cp > 1) == policies["enable_cp_sequence_partitioning"]
        valid_ep = (ep > 1) == policies["enable_ep_cxl_slice_routing"]

        all_valid = valid_pp and valid_tp and valid_cp and valid_ep
        status = "PASSED" if all_valid else "FAILED"

        results.append(
            TopologyTestResult(
                name=name,
                pp_size=pp,
                tp_size=tp,
                cp_size=cp,
                ep_size=ep,
                read_write_sha256_matched=precision_passed,
                latency_us=elapsed_us,
                status=status,
            )
        )

        os.environ.pop("PIPELINE_PARALLEL_SIZE", None)
        os.environ.pop("TENSOR_PARALLEL_SIZE", None)

    # Print Report Table
    print("\n" + "=" * 80)
    print(f" {'Topology Name':<42} | {'PP/TP/CP/EP':<12} | {'Bit-Exact':<10} | {'Status'}")
    print("=" * 80)

    all_passed = True
    for res in results:
        topo_str = f"{res.pp_size}/{res.tp_size}/{res.cp_size}/{res.ep_size}"
        print(f" {res.name:<42} | {topo_str:<12} | {'100% SHA256':<10} | {res.status}")
        if res.status != "PASSED":
            all_passed = False

    print("=" * 80)
    if all_passed:
        print(" 🎉 ALL TOPOLOGY & READ/WRITE PRECISION TESTS PASSED CLEANLY!")
    else:
        print(" ❌ SOME TOPOLOGY TESTS FAILED!")
    print("=" * 80 + "\n")

    return all_passed


if __name__ == "__main__":
    success = run_precision_and_topology_suite()
    sys.exit(0 if success else 1)
