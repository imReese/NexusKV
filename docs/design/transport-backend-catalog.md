# Transport Backend Catalog

## Goal

Provide a deterministic selection layer between execution decisions and concrete backend implementations.

The catalog exists so connectors and planner-facing code do not need to know:

- which backend implementation will run an action
- whether the preferred transfer backend is available
- whether a safe degraded backend path should be chosen

## Selection inputs

Backends are registered against a structured capability profile:

- `TransferBackend`
- supported action kinds
- source tier
- target tier
- device class
- buffer kind
- materialization capability

The runner builds a `BackendActionRequest`, and the catalog selects a backend by matching those fields in a deterministic order.

Since PR 10, `BackendActionRequest` also carries:

- optional source payload handle
- optional store payload handle
- optional transfer request metadata

## Selection rules

Current selection behavior in PR 9:

1. filter registrations by action kind, tier, device, buffer, and materialization capability
2. if a preferred `TransferBackend` exists, choose an exact transfer-backend match first
3. if no exact transfer-backend match exists, choose the first deterministic degraded candidate
4. if no candidate exists, return no selection and let the runner fall back to recompute or skip

Determinism is provided by:

- explicit registration priority
- stable registration insertion order

## Current backend set

### `baseline-execution-backend`

Purpose:

- baseline in-memory transport path
- minimal stateful store model
- exact store/materialize tests
- safe prefetch intent recording

Current semantics:

- supports baseline transport actions
- stores entries in an in-memory key-isolated map
- materialize reads from that map when the source locator is `memory://...`
- prefetch records intent in the same in-memory model
- preserves tenant / namespace / model / key isolation through the stored key identity
- returns stored payload handles on materialize when a matching in-memory entry exists

### `staged-copy-backend`

Purpose:

- deterministic stub for host-staged transfer paths
- selected for vLLM-style staged materialization and prefetch actions

Current semantics:

- no real transport runtime
- validates the selected backend path
- records successful transfer-like action execution
- returns intermediate host-staging metadata in the transfer session

### `remote-shared-store-backend`

Purpose:

- deterministic stub for remote shared-store store actions

Current semantics:

- selected for remote-target store actions where the staged-copy path matches
- records store execution but does not implement remote networking
- returns remote locator payload handles

## Minimal store state model

The in-memory store in PR 9 is intentionally small.

It supports:

- store write by key identity
- exact lookup by key identity
- prefetch intent recording
- overwrite of the same key with incremented write count

It does not support:

- persistence
- replication
- eviction
- distributed coordination
- prefix or partial retrieval
- metadata service responsibilities

## Deferred backend families

This catalog is the intended home for future:

- RDMA-capable backends
- GPU-direct or lower-copy backends
- real remote shared-store backends
- local SSD / file-backed materialization backends
- object-store backends

Those additions should register new backend implementations and capability profiles without requiring connector rewrites.

They should also consume and produce the payload/transfer contract described in [payload-transfer-contract.md](payload-transfer-contract.md).
