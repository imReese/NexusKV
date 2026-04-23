# Target Platform Architecture

## Proposed architecture

NexusKV should converge on a split architecture:

- Go control plane for orchestration, tenancy, policy, admin APIs, rollout, and fleet-level observability.
- Rust data plane for hot-path indexing, state descriptors, pooling, tiering, transfer orchestration, and `nxradixtree`.
- Python engine adapters for SGLang and vLLM lifecycle hooks, descriptor translation, compatibility shims, and feature rollout flags.

The hot path stays in Rust because that is where page matching, reuse planning, materialization, and transfer policy will become latency-sensitive. Go remains a management plane. Python remains the engine integration surface and must not become the source of truth for core cache mechanics.

## Language/module split

### Go

- `go/cmd/nexuskv-controlplane`: control-plane entrypoint
- `go/controlplane`: service assembly, health, readiness, admin surfacing
- `go/config`: versioned control-plane configuration

### Rust

- `rust/crates/nexus-state`: attention/state descriptors and validation
- `rust/crates/nxradixtree-core`: exact/prefix match core for reuse planning
- future crates:
  - `nexus-dataplane`
  - `nexus-transfer`
  - `nexus-tiering`
  - `nexus-observability`
  - `bindings-py`

### Python

- `python/nexuskv/adapters`: shared adapter abstractions and descriptor helpers
- `python/nexuskv/connectors/sglang`: SGLang connector
- `python/nexuskv/connectors/vllm`: vLLM connector
- future:
  - `python/nexuskv/compat`
  - `python/nexuskv/tests`

## Key abstractions/interfaces

### AttentionStateDescriptor

This is the compatibility contract for cache state, not only for classic KV tensors. It captures:

- semantic type
- engine family
- tensor/state roles
- dtype and shapes
- granularity
- partial materialization support
- future hooks for quantization, layout, conversion, and compatibility policy

The source of truth for this descriptor family now lives in a versioned schema file, with generated Rust and Python bindings, rather than manual duplicated definitions.

### Hardware and transfer contract

The same shared contract defines:

- `DeviceClass`
- `BufferKind`
- `TransferBackend`
- `TransferCapability`
- `TierKind`
- `MaterializationCapability`

This keeps NexusKV hardware-aware without hard-locking the architecture around CUDA-only assumptions.

### Reuse planner primitives

The `nxradixtree` layer must support:

- exact lookup
- longest-prefix lookup
- matched-length reporting
- future lineage and policy hints

This first patch only implements exact/prefix semantics. Eviction hints, snapshots, and tier-aware planning are deferred.

## First implementation milestones

1. Foundation scaffold
   - architecture docs
   - new repo layout
   - state descriptor skeleton
   - `nxradixtree` skeleton
   - Python connector skeletons
   - Go control-plane scaffold
2. Descriptor and connector hardening
   - compatibility metadata
   - quantization/layout fields
   - engine hook lifecycle coverage
   - config validation
3. Reuse and tiering interfaces
   - reuse result model
   - tier locator model
   - materialization plan model
4. End-to-end hit/miss path
   - connector lookup
   - Rust match plan
   - fallback-to-recompute behavior
   - control-plane observability

## Proposed first 5 PRs

### PR 1: Foundation scaffold

Create or modify:

- `README.md`
- `docs/architecture/repo-assessment.md`
- `docs/architecture/target-platform.md`
- `docs/design/attention-state-descriptor.md`
- `docs/design/nxradixtree.md`
- `docs/benchmarks/benchmark-methodology.md`
- `docs/ops/reliability-model.md`
- `docs/superpowers/plans/2026-04-24-foundation-scaffolding.md`
- `python/pyproject.toml`
- `python/nexuskv/**`
- `rust/Cargo.toml`
- `rust/crates/nexus-state/**`
- `rust/crates/nxradixtree-core/**`
- `go/go.mod`
- `go/cmd/nexuskv-controlplane/main.go`
- `go/controlplane/app/**`
- `go/config/**`
- `tests/**`

### PR 2: Legacy Go baseline stabilization

Modify:

- `go.mod`
- `pkg/health/health.go`
- `pkg/metrics/metrics.go`
- `pkg/wal/wal.go`
- `pkg/wal/segment.go`
- `pkg/wal/wal_test.go`
- `cmd/server/main.go`

Purpose:

- make the legacy prototype compile and test cleanly
- preserve it as a comparison baseline during migration

### PR 3: Descriptor protocol and Python/Rust parity

Modify or create:

- `rust/crates/nexus-state/**`
- `python/nexuskv/adapters/state.py`
- `python/nexuskv/connectors/base.py`
- `python/tests/test_state.py`
- `tests/compat/state-descriptor-matrix.md`

Purpose:

- tighten descriptor validation
- add quantization/layout metadata
- define parity expectations across language boundaries

### PR 4: `nxradixtree` match planner

Modify or create:

- `rust/crates/nxradixtree-core/**`
- `rust/benches/**`
- `tests/bench-smoke/**`
- `docs/design/nxradixtree.md`

Purpose:

- add lineage metadata
- partial-hit planning
- benchmark harness for prefix/exact lookup latency

### PR 5: Engine connector lifecycle integration

Modify or create:

- `python/nexuskv/connectors/sglang/**`
- `python/nexuskv/connectors/vllm/**`
- `python/nexuskv/adapters/**`
- `tests/integration/**`
- `tests/compat/**`

Purpose:

- define prefill/extend/decode hook points
- standardize safe fallback behavior
- keep engine-version-specific logic isolated
