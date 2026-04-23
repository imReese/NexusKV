# Python to Rust Planner Bridge

## Goal

Provide the smallest real execution path from Python connectors into the Rust `nxradixtree` planner without introducing a broad FFI framework.

## Binding mechanism

NexusKV uses a narrow PyO3 extension module for the planner bridge.

### Why this choice

- it gives Python a direct in-process call path into Rust
- it keeps planner semantics in Rust rather than in a Python shim
- it is smaller and more maintainable at this stage than a general C ABI layer
- it avoids prematurely designing a large binding surface before the planner API is stable

### Tradeoffs

- this is not yet a polished packaging story like `maturin develop`
- the current wrapper loads the built extension from `rust/target/{debug,release}`
- payload exchange uses JSON over the extension boundary, which is correct and simple now but not optimized

## Bridge shape

Rust extension crate:

- `bindings-py`
- exposes:
  - planner construction
  - `insert(reuse_key_json, entry_json)`
  - `lookup(query_json)`
  - `plan_partial_hit(query_json)`

Python wrapper:

- `nexuskv.planner.rust_backend.RustPlanner`
- keeps the `ReusePlanner` protocol shape used by connectors
- converts generated dataclasses to and from JSON-compatible primitives at the edge

## Why JSON on the boundary for now

- shared contract types are already generated and now serializable in Rust
- JSON keeps the bridge narrow and auditable
- correctness matters more than speed at this stage
- the conversion layer stays small and documented instead of leaking Rust internals into Python

## Deferred

- wheel/dev install workflow
- binary compatibility hardening
- lower-copy bridge payloads
- direct PyO3 mappings for generated contract types
- async planner execution
