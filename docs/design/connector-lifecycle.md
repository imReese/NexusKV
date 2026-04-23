# Connector Lifecycle Integration

## Goal

Define engine-facing lifecycle surfaces that are explicit in code, use the shared contract types, and consume the `nxradixtree` planning boundary instead of inventing connector-local match logic.

## Canonical planning boundary

Both connectors now operate against generated planning types:

- `QueryKey`
- `ReuseKey`
- `MatchResult`
- `PartialHitPlan`
- `CacheEntry`

The planner interface exposed to Python is intentionally small:

- `lookup(query)`
- `plan_partial_hit(query)`

That keeps `nxradixtree` as the canonical reuse-planning boundary while leaving Rust bindings and transport work for a later PR.

## SGLang lifecycle model

SGLang uses:

- `prefill`
- `extend`
- `decode`

### Current semantics

- `prefill`: planner lookup is allowed. Exact reuse can be fully materialized through the descriptor’s available transfer path.
- `extend`: planner lookup is allowed. Partial hits are recognized, but the default token-granular descriptor does not support partial materialization, so fallback is recompute.
- `decode`: planner lookup is skipped in v1. Decode is treated as local-state continuation rather than a remote reuse hook.

### Capability shape

- exact reuse: yes
- prefix reuse: yes
- partial materialization: no in v1
- prefetch: no in v1
- decode lookup: no in v1

## vLLM lifecycle model

vLLM uses:

- `request_start`
- `block_table_extend`
- `decode_step`

### Current semantics

- `request_start`: planner lookup is allowed. Partial hits can become partial materialization plus optional prefetch.
- `block_table_extend`: planner lookup and partial-hit planning are allowed. This is the first lifecycle point shaped for page/block-oriented growth.
- `decode_step`: planner lookup remains allowed in v1 because vLLM’s paged/block cache model makes staged reuse/prefetch more plausible here than in the SGLang token-granular path.

### Capability shape

- exact reuse: yes
- prefix reuse: yes
- partial materialization: yes
- prefetch: yes
- decode lookup: yes

## Fallback behavior

Fallback is explicit and safe:

- if the preferred transfer backend is unsupported but another supported backend exists, degrade to a simpler transfer path
- if the descriptor exposes no viable path for the requested operation, degrade to recompute
- unsupported decode/prefetch paths return explicit skip or recompute decisions rather than silently pretending reuse happened

## Deferred

- Python-to-Rust bindings for live `nxradixtree` execution
- remote transport implementations
- RDMA and zero-copy execution paths
- policy-engine integration beyond placeholders
