# Materialization Execution Boundary

## Goal

Define a narrow execution-layer contract that consumes planner results and turns them into deterministic materialization, prefetch, store, skip, or recompute decisions without pushing those semantics into connector-specific branching.

## Position in the architecture

The current v1 request flow is:

1. connector builds engine lifecycle context
2. connector maps the request into `QueryKey`
3. Rust-backed planner returns `MatchResult` or `PartialHitPlan`
4. execution runner converts that planning result into execution decisions
5. connector applies the resulting decision at the engine boundary

This keeps the planner responsible for reuse discovery and the execution layer responsible for tier-aware and backend-aware action selection.

## Core request and result types

The execution layer is currently a narrow Python adapter surface:

- `MaterializationRequest`
- `MaterializationDecision`
- `MaterializationOutcome`
- `ExecutionDisposition`
- `FallbackReason`
- `SourceTier`
- `TargetTier`
- `TransferMode`
- `CapabilityCheckResult`

These types intentionally reuse generated shared-contract enums where possible:

- `TierKind`
- `BufferKind`
- `DeviceClass`
- `TransferBackend`
- `MaterializationCapability`

That keeps hardware and transfer semantics consistent with the shared schema while avoiding premature expansion of the Rust/Python bridge for execution-specific orchestration.

## Decision model

The baseline runner decides:

- whether the primary action is `materialize`, `recompute`, or `skip`
- whether a secondary prefetch action should be attempted
- whether a post-stage store action should be requested
- which source tier and target tier are implied
- which transfer backend is selected
- whether the selected path degraded from a preferred backend
- whether fallback happened because of unsupported capability, missing path, lookup policy, or cache miss

## Tier awareness

The execution boundary already models the conceptual distinction between:

- `device`
- `host_dram`
- `local_ssd`
- `remote_shared`
- recompute as a non-materialization path

The baseline runner derives the initial target tier from descriptor materialization metadata:

- prefer `device` when device materialization is declared
- otherwise prefer `host_dram`
- then `local_ssd`
- then `remote_shared`

Store intent currently prefers `remote_shared` when available, otherwise it falls back to the descriptor’s primary target tier.

## Hardware and transfer awareness

The runner consumes:

- declared `transfer_paths`
- declared `materialization.capabilities`
- declared `device_classes`
- declared `buffer_kinds`

Current behavior:

- if the preferred backend is available, use it
- if the preferred backend is unavailable but another supported backend exists, degrade to that simpler backend
- if no backend exists, return a structured fallback result
- if partial materialization is unsupported for the descriptor, degrade the primary action to recompute
- if prefetch is unsupported, return an explicit safe skip instead of pretending prefetch happened

## Implemented versus deferred

Implemented in PR 7:

- baseline execution runner
- tier-aware target selection
- structured degradation and fallback behavior
- connector integration for SGLang and vLLM
- deterministic unit tests for exact hit, partial hit, backend downgrade, and recompute/skip behavior

Deferred:

- real transport backend implementations
- async execution runtime
- RDMA and lower-copy paths
- GPU-direct or accelerator-specific direct materialization
- batching and throughput optimization
- policy-engine-driven execution decisions
- distributed execution coordination

## Why this boundary exists now

Without this layer, connectors become the long-term home of materialization policy. That would create drift between SGLang and vLLM behavior, make hardware and tier support inconsistent, and make future transport work much harder to integrate cleanly.

This boundary keeps connectors lifecycle-aware while centralizing execution semantics in one place.
