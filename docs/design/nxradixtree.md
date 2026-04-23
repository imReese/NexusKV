# nxradixtree Design Direction

## Goal

Build a Rust-first reuse index that now serves as a real reuse-planning boundary, not only a toy prefix matcher.

## Responsibilities

- stable identity boundary for reusable cache entries
- exact and longest-prefix matching inside an identity scope
- rich match result for reuse planning
- explicit partial-hit planning boundary
- structured placeholders for lineage/version, location, and policy hints

## Why Rust

`nxradixtree` is on the hot path for match planning. Implementing it in Python would create unnecessary interpreter overhead and would make future concurrency and memory-layout work harder. Implementing it in Go would complicate reuse with Rust-owned data-plane structures.

## Implemented in PR 4

The current version adds a narrow but real planning API:

- `KeyIdentity` for deterministic tenant/namespace/model/engine/state/token identity
- `ReuseKey` and `QueryKey` as explicit tree boundaries
- `CacheEntry` with `EntryVersion`, `EntryLocation`, and `PolicyHint`
- `MatchResult` with:
  - classification
  - matched identity
  - matched extent
  - remaining work
  - compatibility signal
- `PartialHitPlan` as the first planner-facing reuse result

### Invariants

- identity scopes are isolated by tenant, namespace, model, engine family, semantic type, and optional block/page ids
- longest-prefix behavior is deterministic because the tree keeps the deepest terminal entry in a scope
- location, policy, and lineage/version are explicit fields, not comments
- the tree is still Rust-first and does not assume one engine or one attention layout

## Deferred scope

- compressed edge representation
- concurrent reader/writer optimization
- persistence and snapshot/export
- full policy engines
- tier-aware scoring and transfer scheduling
- block/page multi-extent planning beyond the current identity placeholders
