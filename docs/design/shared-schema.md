# Shared Schema Contract

## Choice

NexusKV uses a versioned repo-local JSON IDL as the source of truth for the cache-state contract, with checked-in generated bindings for Python and Rust.

## Why this over protobuf right now

- It removes Python/Rust drift immediately without requiring `protoc`, gRPC coupling, or extra build plumbing before the wire protocol is stable.
- The current need is a shared type contract, not a finalized RPC envelope.
- The schema is easy to review in git, easy to regenerate, and simple to extend during early platform evolution.

## Tradeoffs

- Protobuf would be stronger once the transport and control/data-plane protocol stabilizes.
- The JSON IDL here is intentionally narrow and does not try to be a general schema language.
- We still need a later transport-level IDL for network protocol compatibility.

## Scope

The shared v1 contract covers:

- `AttentionStateDescriptor`
- planning boundary types such as `QueryKey`, `ReuseKey`, `MatchResult`, and `PartialHitPlan`
- hardware classes and buffer kinds
- transfer backends and capabilities
- tier kinds
- materialization capabilities

This means Python connector logic and Rust `nxradixtree` logic can now share both descriptor metadata and the planner I/O model without manual drift.
- compatibility flags
