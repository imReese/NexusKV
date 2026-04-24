# Migration Status

## PR 2: Legacy Root Go Baseline

### What was broken

- missing Prometheus dependency in the root Go module
- incomplete `pkg/health` implementation
- incomplete `pkg/wal` implementation and broken WAL tests
- nonexistent legacy Raft symbols referenced by `cmd/server`
- duplicate-registration risk in `pkg/metrics`

### What was fixed

- root `go test ./...` now passes under Go `1.23.9`
- `pkg/health` now returns a minimal serving response
- `pkg/wal` now has a minimal working segmented-log baseline sufficient for the legacy tests
- `pkg/raft` now has explicit no-op legacy placeholders so the old server binary compiles
- `pkg/metrics` registration is guarded with `sync.Once`

### What remains intentionally legacy

- the root Go tree is still a legacy prototype, not the future architecture
- Raft remains a compile-time placeholder baseline, not a production implementation
- the legacy storage engine remains stubby and is not being evolved into the new data plane

## PR 3: Shared Cross-Language Contract

Completed:

- versioned schema source of truth
- generated Python and Rust bindings
- first-class hardware, transfer, tier, and materialization abstractions

## PR 4: nxradixtree Planning Boundary

Completed:

- stable identity model for tree scope and entries
- rich match result for exact and partial reuse
- explicit partial-hit planner boundary
- structured lineage/version, location, and policy placeholders
- deterministic matching tests across identity boundaries and related prefixes

## PR 5: Engine Lifecycle Integration

Completed:

- explicit SGLang lifecycle surface: `prefill`, `extend`, `decode`
- explicit vLLM lifecycle surface: `request_start`, `block_table_extend`, `decode_step`
- connector operation semantics for lookup, materialize, store, prefetch, fallback, and unsupported-path handling
- connector mapping onto generated `QueryKey` / `ReuseKey` / `MatchResult` / `PartialHitPlan`
- clean degrade-to-recompute or simpler-transfer fallback behavior

## Current Repo Status

- legacy Go baseline is stable enough for comparison
- shared schema now covers descriptor and planner boundary types
- `nxradixtree` is a real planning/index subsystem
- Python connectors are lifecycle-aware and planner-driven
- Python planner protocol is now backed by a real Rust `nxradixtree` execution path through a thin extension bridge
- Python connectors now route planner results through a tier-aware execution boundary instead of keeping materialization policy in connector-local branches

## Next Planned Step

PR 8 should start turning the execution boundary into a real transport/materialization surface: narrow execution backend protocol, baseline transfer/materialization stub implementation, and end-to-end planner-to-executor integration around concrete materialization actions.

## PR 6: Rust-Backed Planner Bridge

Completed:

- added a thin Python-to-Rust planner bridge using PyO3
- extended the shared contract to include planner boundary types
- kept the Python `ReusePlanner` protocol stable while replacing fake planner behavior with Rust-backed execution
- added Python integration tests that exercise connectors against the real Rust planner
- documented the binding mechanism and its current limitations

## PR 7: Tier-Aware Materialization Execution Boundary

Completed:

- introduced a narrow execution-layer contract for materialize, prefetch, store, recompute, and skip decisions
- centralized tier-aware and backend-aware execution semantics in a baseline runner
- integrated SGLang and vLLM connectors with the execution boundary while preserving their lifecycle differences
- added deterministic tests for exact hits, partial hits, backend downgrade, unsupported prefetch, and recompute fallback
- documented what is implemented now versus what remains deferred to future transport and tiering work
