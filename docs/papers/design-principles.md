# Appendix C: NexusKV Design Principles

This appendix records the design invariants behind the
[NexusKV Whitepaper v1.0](a-survey-of-kv-cache-systems-for-llm-inference.md).
The whitepaper is the canonical architecture narrative; this document is a
review checklist for future implementations.

## C.1 Semantic state identity

**Principle.** A cache entry identifies reusable Model State, not only a tensor
buffer or token prefix.

**Rationale.** Equal bytes or tokens do not establish compatibility across
model revisions, attention mechanisms, parallel layouts, position conventions,
or recurrent checkpoints.

**Implementation consequence.** Every reuse path must carry a versioned State
Descriptor and fail closed when required identity or dependency fields are
unknown. Hashing is an indexing mechanism; compatibility is a semantic rule.

## C.2 One cost model for all decisions

**Principle.** Reuse, placement, transfer, prefetch, admission, and eviction
must be evaluated against the same end-to-end cost model.

**Rationale.** Independent policies can optimize hit rate, storage occupancy,
or link utilization while reducing throughput or increasing tail latency.

**Implementation consequence.** Decision records must expose the estimated
recompute cost, visible cache cost, uncertainty, resource budget, and chosen
fallback. The Effective Gain definition in Section 2 of the whitepaper is the
canonical comparison.

## C.3 Cache work stays off the critical path

**Principle.** Cache management is admitted only when it can complete before
consumption or when its remaining visible cost is lower than recomputation.

**Rationale.** Asynchronous APIs alone do not hide work. Queueing, device-memory
reservation, synchronization, and interference can remain visible even when
the transfer call is non-blocking.

**Implementation consequence.** Prefetch requires a deadline, completion
signal, cancellation or abandonment rule, and resource budget. A late or
uncertain operation falls back to recomputation without corrupting scheduler or
allocator state.

## C.4 The Inference Runtime retains execution authority

**Principle.** NexusKV advises and coordinates; the Inference Runtime owns
allocation, request admission, kernel execution, and final safety checks.

**Rationale.** Only the Inference Runtime has authoritative knowledge of active
requests, block tables, stream ordering, attention backend constraints, and the
exact consumption point.

**Implementation consequence.** Adapters translate a portable decision into a
runtime-native action. A planner result is not authorization to overwrite or
consume device memory.

## C.5 Data Plane components remain replaceable

**Principle.** Storage and transfer capabilities are selected through explicit
contracts instead of embedded in the Intelligence Layer.

**Rationale.** Mooncake, NIXL, InfiniStore, local Host DRAM, and future backends
have different transport, registration, durability, and failure properties.
No single backend is appropriate for every topology.

**Implementation consequence.** A backend advertises capabilities and measured
cost. The planner selects among supported paths; it does not infer zero-copy,
durability, or remote availability from a backend name.

## C.6 Attention semantics are extensible, not implicit

**Principle.** MHA, MLA, DSA, and KDA share a lifecycle framework but do not
share one payload or restoration rule.

**Rationale.** Latent, sparse-selection, and recurrent states introduce
dependencies that a conventional K/V-page identity cannot represent safely.

**Implementation consequence.** Each semantic state type registers its
identity, compatibility, materialization, conversion, and fallback rules.
Unsupported state types recompute by default.

## C.7 Decisions are observable and reproducible

**Principle.** Every cache decision must be explainable after execution.

**Rationale.** Aggregate hit rate cannot distinguish a compatible useful reuse
from a late transfer, a forced fallback, or an incorrect match rejected by the
runtime.

**Implementation consequence.** Traces should connect state identity, lookup,
cost estimate, placement, transfer, completion, fallback, and observed outcome
without exposing tenant data. Evaluation reports follow Section 9 and
[Appendix E](research-appendix.md).
