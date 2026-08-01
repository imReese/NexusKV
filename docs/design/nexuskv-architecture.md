# NexusKV Architecture

**Status:** Development contract · August 2026

This document translates the
[NexusKV Whitepaper](../papers/beyond-kv-cache.md) into implementation
boundaries. The Whitepaper defines the problem and research direction; this
document defines ownership, APIs, data flow, lifecycle, and change gates for
development. Delivery sequence is tracked in the
[Roadmap](../roadmap.md), and current evidence is tracked in
[Migration Status](../architecture/migration-status.md).

## 1. Architectural Objective

NexusKV is a Model State Intelligence Layer for LLM inference. It coordinates:

- State Identity and compatibility;
- exact and partial reuse discovery;
- cost-based reuse, placement, and transfer planning;
- bounded asynchronous prefetch;
- deterministic fallback and decision feedback.

It does not own model execution, general-purpose storage, or transport
implementation. The architecture must remain composable with Inference
Runtimes and replaceable Data Plane systems.

## 2. Layer Boundaries

| Layer | Owns | Does not own |
| --- | --- | --- |
| Inference Runtime | Request admission, device allocation, block/page tables, streams, kernels, and final state consumption | Cross-runtime state policy |
| Runtime adapter | Lifecycle translation, descriptor construction, planner invocation, and final safety handoff | Global policy or backend implementation |
| Intelligence Layer | Compatibility, matching, cost comparison, placement intent, transfer selection, deadlines, and fallback plan | Model kernels, storage capacity, or transport primitives |
| Control Plane | Versioned policy, topology, tenancy, quotas, capability configuration, rollout, and fleet observability | Per-request payload movement |
| Data Plane | Payload capacity, registration, transfer execution, completion, and backend-specific failure reporting | Semantic compatibility or reuse value |

The Inference Runtime remains the final authority over whether a payload can be
materialized into runtime-owned memory and consumed safely.

## 3. Component Boundaries

### 3.1 State Contract

The versioned State Contract describes semantic and physical compatibility. Its
current source of truth is the repository schema with generated Rust and Python
bindings.

Core types include:

- `AttentionStateDescriptor`;
- `KeyIdentity`, `ReuseKey`, and `QueryKey`;
- `CacheEntry`, `EntryVersion`, and `EntryLocation`;
- `TensorSpec`, `LayoutMetadata`, and `QuantizationMetadata`;
- `TransferPath` and `MaterializationProfile`.

The contract must identify model revision, engine family, semantic state type,
layer and parallel scope, token or checkpoint lineage, layout, dtype,
quantization, and materialization capabilities when those fields affect
correctness. See
[Attention State Descriptor](attention-state-descriptor.md) and
[Shared Schema](shared-schema.md).

### 3.2 State Index and Matcher

`nxradixtree` owns deterministic discovery inside an identity scope. It
provides exact lookup, longest-prefix lookup, matched extent, compatibility
signals, and partial-hit planning primitives.

It does not decide that a match is profitable, reserve destination memory, or
execute transfer. See [nxradixtree](nxradixtree.md).

### 3.3 Reuse Planner

The planner consumes a query, compatible match candidates, topology and policy
constraints, and cost observations. Its target output is an explainable choice
among:

- full reuse;
- partial reuse;
- route-to-state;
- transfer and materialize;
- recompute.

The current implementation proves the match and execution boundary. A fully
calibrated cost planner remains in progress.

### 3.4 Execution Boundary

The execution layer turns a planner result into deterministic actions while
keeping connector code policy-agnostic. The backend protocol exposes:

```text
materialize(request) -> result
prefetch(request)    -> result
store(request)       -> result
skip(request)        -> result
recompute(request)   -> result
```

Each result records the requested action, executed action, backend selection,
payload handle, transfer session, disposition, and fallback reason. Current
baseline, staged-copy, and remote-store behavior must be interpreted according
to [Execution Boundary](execution-boundary.md) and
[Transport Backend Catalog](transport-backend-catalog.md); stubs are not native
movement.

### 3.5 Payload and Transfer Contract

The transfer boundary uses explicit types rather than payload-less control
flow:

- `PayloadDescriptor` and `StateSliceDescriptor`;
- `PayloadLocation` and `PayloadHandle`;
- `TransferRequest`, `TransferResult`, and `TransferSession`.

Ownership values are currently descriptive hints, not a complete memory-safety
protocol. A real backend must define allocation authority, registration,
completion, cancellation, retry, and cleanup semantics. See
[Payload Transfer Contract](payload-transfer-contract.md).

### 3.6 Control Plane Policy

`nexuskv.execution_policy.v1` is the versioned operator-authored contract for:

- enabled backends and backend priority;
- allowed source and target tiers;
- allowed device and buffer classes;
- materialization capabilities;
- degraded-path and fallback behavior;
- backend capability overlays;
- tenancy, quota, and admission placeholders.

Go owns validation and export. Python owns consumption, last-known-good reload,
catalog filtering, and execution interpretation. Connectors do not evaluate
policy. See [Control Plane Execution Policy](controlplane-execution-policy.md).

### 3.7 Observability and Feedback

Every decision should connect:

```text
state identity
  -> match and compatibility
  -> predicted cost
  -> selected placement and backend
  -> completion or fallback
  -> observed latency, interference, and Effective Gain
```

Aggregate hit rate is diagnostic, not the optimization objective. Telemetry
must preserve tenant isolation and distinguish logical match, physical
availability, completed materialization, and useful reuse.

## 4. API Ownership

The current code exposes internal contracts rather than a stable public network
API.

| Contract | Current owner | Stability expectation |
| --- | --- | --- |
| State and planner schema | Repository IDL with generated Rust/Python bindings | Version before incompatible change |
| `lookup` and `plan_partial_hit` | Rust planner through the Python bridge | Narrow internal planning API |
| Connector lifecycle hooks | Python SGLang/vLLM adapters | Version-pinned compatibility surface |
| Execution backend protocol | Python execution layer | Extension point for real backends |
| Payload/transfer session types | Python execution contract and shared enums | Stable seam, not proof of transfer |
| Execution policy | Go producer and Python consumer | Versioned operator contract |

New public RPCs must not be inferred from these internal types. A network API
requires an explicit versioning, authentication, failure, and rollout design.

## 5. Request Data Flow

1. **Describe.** The adapter converts Inference Runtime context into a
   `QueryKey` and required State Descriptor.
2. **Match.** `nxradixtree` returns exact or prefix candidates and remaining
   work.
3. **Validate.** Identity, descriptor, lineage, tenant, and version checks
   reject unsafe candidates.
4. **Plan.** The planner compares reuse paths with recomputation under policy,
   capacity, topology, deadline, and uncertainty constraints.
5. **Reserve.** The Inference Runtime authorizes runtime-owned destination
   memory; the selected backend reserves other required resources.
6. **Execute.** The Data Plane materializes, prefetches, stores, skips, or
   reports rejection through a transfer session.
7. **Commit or fall back.** The Inference Runtime consumes ready compatible
   state or recomputes deterministically.
8. **Observe.** Completion and cost feedback update telemetry and future
   calibration.

## 6. State Lifecycle

The conceptual lifecycle is:

```text
described
  -> matched
  -> validated
  -> planned
  -> reserved
  -> materialized | recomputed | skipped
  -> observed
  -> retained | demoted | evicted | invalidated
```

Not every transition is implemented. In particular, distributed leases,
invalidation, native asynchronous completion, demotion, and eviction policy are
future work.

Lifecycle invariants:

- compatibility fails closed;
- missing or late state falls back without changing model correctness;
- metadata match, payload availability, and materialization completion remain
  distinct states;
- transfer intent is not reported as completed movement;
- the Inference Runtime authorizes final consumption;
- Control Plane updates do not mutate in-flight decisions without an explicit
  version boundary.

## 7. Current Implementation Map

| Area | Current state | Next boundary |
| --- | --- | --- |
| State Contract | Versioned schema and generated bindings | Richer conversion and compatibility rules |
| Matcher | Rust exact/prefix lookup and partial-hit plan | Cost and multi-extent planning |
| Store | Bounded Host DRAM payload store | Tiered lifecycle and persistence integrations |
| Adapters | Lifecycle-aware SGLang/vLLM surfaces | Version-pinned native conformance |
| Execution | Deterministic runner, catalog, fallback, and stubs | Native transfer backends and completion |
| Control Plane | Validated file policy handoff and overlays | Distributed rollout, quota, and admission |
| Evaluation | Methodology and scaffolding | Retained end-to-end benchmark artifacts |

## 8. Change Protocol

Every feature proposal must answer:

1. Does it conform to the Whitepaper?
2. Which architecture layer owns it?
3. Which concrete problem does it solve?
4. How will it be benchmarked or otherwise validated?
5. Does it change the State Contract or compatibility rules?

If ownership is ambiguous, the change should stop at a contract boundary until
the architecture decision is explicit. If correctness, timing, or capability
cannot be established, recomputation remains the default behavior.
