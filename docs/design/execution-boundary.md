# Materialization Execution Boundary

## Goal

Define a narrow execution-layer contract that consumes planner results and turns them into deterministic materialization, prefetch, store, skip, or recompute decisions without pushing those semantics into connector-specific branching.

## Position in the architecture

The current v1 request flow is:

1. connector builds engine lifecycle context
2. connector maps the request into `QueryKey`
3. Rust-backed planner returns `MatchResult` or `PartialHitPlan`
4. execution runner converts that planning result into execution decisions
5. execution backend receives concrete action requests
6. connector observes the resulting execution outcome at the engine boundary

This keeps the planner responsible for reuse discovery and the execution layer responsible for tier-aware and backend-aware action selection.

## Core request and result types

The execution layer is currently a narrow Python adapter surface:

- `MaterializationRequest`
- `MaterializationDecision`
- `MaterializationOutcome`
- `BackendActionRequest`
- `BackendActionResult`
- `ExecutionStepOutcome`
- `ExecutionDisposition`
- `BackendActionKind`
- `BackendActionStatus`
- `FallbackReason`
- `SourceTier`
- `TargetTier`
- `TransferMode`
- `CapabilityCheckResult`
- `PayloadHandle`
- `PayloadDescriptor`
- `PayloadLocation`
- `TransferRequest`
- `TransferResult`
- `TransferSession`

These types intentionally reuse generated shared-contract enums where possible:

- `TierKind`
- `BufferKind`
- `DeviceClass`
- `TransferBackend`
- `MaterializationCapability`

That keeps hardware and transfer semantics consistent with the shared schema while avoiding premature expansion of the Rust/Python bridge for execution-specific orchestration.

## Backend protocol

The execution backend protocol is intentionally small. A backend implementation must provide five operations:

- `materialize`
- `prefetch`
- `store`
- `skip`
- `recompute`

Each operation accepts a structured `BackendActionRequest` and returns a structured `BackendActionResult`.

The baseline guarantees in PR 8 are:

- every execution step becomes a concrete backend call
- backend calls are observable through recorded invocation state
- unsupported or missing backend paths return structured rejection results
- the runner converts rejected transfer actions into deterministic fallback execution
- connector code stays unaware of backend implementation details

PR 9 adds deterministic backend selection in front of those calls through a transport backend catalog.
PR 10 adds explicit payload handles and transfer-session metadata to those calls and results.

## Decision model

The baseline runner decides:

- whether the primary action is `materialize`, `recompute`, or `skip`
- whether a secondary prefetch action should be attempted
- whether a post-stage store action should be requested
- which source tier and target tier are implied
- which transfer backend is selected
- whether the selected path degraded from a preferred backend
- whether fallback happened because of unsupported capability, missing path, lookup policy, or cache miss
- which backend action was requested and which action was ultimately executed
- which payload handle is being moved or produced
- which transfer session metadata is visible to the caller

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
- if the selected transport backend is rejected by the execution backend, the runner degrades the step to recompute or skip and records that fallback outcome

## Baseline backend behavior

`BaselineExecutionBackend` is intentionally simple:

- it is in-memory
- it records every invocation for tests and debugging
- it validates that transfer actions carry a selected backend path
- it can be configured with a restricted set of supported transfer backends
- it returns structured `succeeded`, `rejected`, `skipped`, `recomputed`, or `fallback` outcomes

This is not yet a real transport engine. It is a deterministic protocol implementation that proves the execution boundary is real.

## Catalog and store model

PR 9 adds:

- a transport backend catalog with deterministic registration and selection rules
- a minimal in-memory store model behind the baseline backend
- backend stubs for staged-copy and remote shared-store execution paths

The current store model is only large enough to prove:

- store writes an entry
- exact materialize can retrieve that stored entry
- missing memory-backed materialize returns a structured miss and falls back safely
- namespace, model, and key identity remain isolated
- prefetch records intent without pretending a real transport runtime exists

PR 10 adds explicit payload and transfer-session metadata on top of that store model so:

- store actions carry a source payload handle
- materialize actions return a result payload handle
- prefetch actions return registered transfer sessions and ephemeral target handles
- missing payload sources produce structured fallback with transfer-session metadata

## Implemented versus deferred

Implemented in PR 7:

- baseline execution runner
- tier-aware target selection
- structured degradation and fallback behavior
- connector integration for SGLang and vLLM
- deterministic unit tests for exact hit, partial hit, backend downgrade, and recompute/skip behavior

Implemented in PR 8:

- narrow execution backend protocol
- baseline backend with observable invocation recording
- runner dispatch from decisions into backend actions
- deterministic fallback on backend rejection
- end-to-end tests from Rust planner hit -> execution runner -> backend invocation -> connector-visible outcome

Implemented in PR 9:

- deterministic backend catalog and registration model
- minimal baseline in-memory store semantics
- staged-copy and remote-shared-store stub backends
- runner-side backend selection and degraded backend routing
- tests that cover exact selection, degraded selection, missing backend fallback, store/materialize statefulness, and connector stability

Implemented in PR 10:

- explicit payload handle, descriptor, location, ownership, and slice types
- narrow transfer request/result/session contract
- payload propagation through runner, backend actions, and connector-visible outcomes
- staged-copy stub now exposes intermediate host-staging metadata
- remote shared-store stub now exposes remote payload handles

Deferred:

- real transport backend implementations
- async execution runtime
- RDMA and lower-copy paths
- GPU-direct or accelerator-specific direct materialization
- remote shared-store transport backend
- SSD or file-backed materialization backend
- batching and throughput optimization
- policy-engine-driven execution decisions
- distributed execution coordination

## Future backend extension points

The current backend protocol is the intended plug-in point for:

- host-staged transfer backends
- RDMA-capable backends
- device-aware backends
- remote shared-store backends
- local SSD or file-backed backends

Those implementations should not require connector changes. They should plug in behind the execution runner and consume the same structured action requests.

See also: [transport-backend-catalog.md](transport-backend-catalog.md)
See also: [payload-transfer-contract.md](payload-transfer-contract.md)

## Why this boundary exists now

Without this layer, connectors become the long-term home of materialization policy. That would create drift between SGLang and vLLM behavior, make hardware and tier support inconsistent, and make future transport work much harder to integrate cleanly.

This boundary keeps connectors lifecycle-aware while centralizing execution semantics in one place.
