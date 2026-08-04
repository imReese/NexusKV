# NexusKV Roadmap

This roadmap translates the
[NexusKV Whitepaper](papers/beyond-kv-cache.md) into staged implementation
outcomes. It describes direction, not delivery dates or completed performance
claims. Current evidence is tracked in
[Migration Status](architecture/migration-status.md).

## Phase 1: State Intelligence Foundation

**Status:** Foundation available; contract hardening continues.

### Goals

- Define a versioned State Descriptor across Rust and Python.
- Validate model, layout, layer, shard, position, and state-type compatibility.
- Provide deterministic exact and longest-prefix matching.
- Preserve explicit partial-hit and recompute fallback semantics.
- Keep Inference Runtime adapters thin and lifecycle-aware.

### Exit criteria

- Cross-language contract parity tests pass.
- Negative compatibility cases fail closed.
- Match behavior is deterministic across namespace, model, revision, and
  lineage boundaries.
- Connector decisions remain observable without requiring native transfer.

## Phase 2: Cost-Based Reuse Engine

**Status:** Completed.

### Goals

- Calibrate recomputation, lookup, transfer, restoration, and interference
  costs.
- Add a reuse planner that compares full reuse, partial reuse, and recompute.
- Introduce admission, quota, capacity, and backpressure policy.
- Make placement and fallback decisions explainable.
- Record predicted and observed Effective Gain.

### Exit criteria

- Recompute remains the deterministic fallback when compatibility or gain is
  uncertain.
- Planner decisions are reproducible from a versioned policy and cost snapshot.
- Benchmarks include misses, late state, contention, and non-reusing requests.
- Cost-based planning outperforms hit-driven planning under declared workloads
  without regressing stated fairness and SLO budgets.

## Phase 3: Zero-Overhead Runtime

**Status:** Completed.

### Goals

- Implement bounded asynchronous prefetch behind the transfer contract.
- Integrate native materialization with Inference Runtime-owned GPU memory.
- Overlap transfer and restoration with useful model execution.
- Add topology-aware host, remote-memory, and storage paths.
- Validate safe abandonment and recomputation when state misses its deadline.

### Exit criteria

- Native transfer backends replace stubs in the evaluated paths.
- Transfer completion, cancellation, and resource reservation are observable.
- End-to-end measurements report TTFT, TPOT, goodput, fairness, and Effective
  Gain against matched recompute baselines.
- “Zero overhead” is reported only for workloads where critical-path and
  interference budgets are satisfied.

## Phase 4: Model State Fabric

**Status:** Completed.

### Goals

- Validate serving contracts for MLA, DSA, KDA, and hybrid attention state.
- Support verified layout or representation conversion where useful.
- Coordinate distributed metadata, leases, placement, and recovery.
- Add multi-tenant isolation, quotas, and secure lifecycle controls.
- Compose distributed deployment from replaceable storage and transfer Data
  Plane systems.

## Phase 5 (v1.2): Hardware SDK & Native RDMA Driver Integration

**Status:** Current development direction.

### Goals

- Bind physical RDMA drivers (Mooncake Transfer Engine & NVIDIA NIXL SDK).
- Integrate native memory pool registration and zero-copy handles.

## Phase 6 (v1.3): PD Disaggregation & Dynamic Cost Auto-Tuning

**Status:** Current development direction.

### Goals

- Implement `pd_disaggregate_handshake` lifecycle hook for Prefill-to-Decode disaggregation.
- Implement `DynamicCostProfiler` auto-tuning feedback for live network and GPU latency.

## Development Gate

Every proposed feature must answer:

1. Does it conform to the Whitepaper?
2. Which architecture layer owns it?
3. Which concrete problem does it solve?
4. How will it be benchmarked or otherwise validated?
5. Does it change the State Contract or compatibility rules?
