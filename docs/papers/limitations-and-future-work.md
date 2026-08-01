# Appendix D: Limitations and Future Work

This appendix expands Section 10 of the
[NexusKV Whitepaper v1.0](a-survey-of-kv-cache-systems-for-llm-inference.md).
It separates current implementation evidence from architectural hypotheses so
that future progress does not retroactively turn a proposal into a measured
result.

## D.1 Current implementation boundary

The repository currently contains:

- versioned shared contracts for Model State identity and transfer metadata;
- a Rust exact/prefix reuse planner and a bounded Host DRAM payload store;
- Python adapters and an execution boundary for materialize, prefetch, store,
  skip, and recompute decisions;
- a Go Control Plane scaffold and file-based policy handoff.

These components establish a testable software boundary. They do **not** yet
establish:

- production RDMA, GPUDirect, remote shared-store, or SSD transfer paths;
- measured overlap of transfer with native GPU execution;
- cluster-wide metadata consistency and recovery;
- production integrations validated against supported vLLM, SGLang, or
  TensorRT-LLM releases;
- the performance claims defined by the zero-overhead condition.

An intent record, registered transfer session, stub backend, or unit test must
not be reported as completed device movement.

## D.2 Integration and version coupling

Inference Runtimes expose different allocation, prefix-index, stream, and
connector lifecycles. Internal interfaces may change faster than a shared
protocol. A portable descriptor therefore reduces semantic ambiguity but does
not remove adapter maintenance.

The compatibility surface should be versioned per Inference Runtime and tested
against concrete releases. When an adapter cannot prove layout, lifecycle, or
synchronization compatibility, the safe decision is recomputation.

## D.3 State generalization

MHA, MLA, DSA, and KDA do not have interchangeable restoration rules. Sparse
selection may be query-dependent; recurrent state requires a valid terminal
checkpoint; hybrid models may combine multiple state families in one request.
The initial schema may not capture every cross-layer or kernel-specific
dependency.

Future work should add semantic types through conformance suites rather than
loosening compatibility checks. Each extension needs reference materialization,
negative compatibility cases, and numerical-equivalence thresholds.

## D.4 Cost-model calibration and drift

Transfer and recomputation costs depend on topology, concurrency, queue depth,
payload shape, registration state, and kernel mix. Measurements become stale as
load or hardware changes. A planner can therefore make a locally rational but
globally harmful decision.

Useful future work includes online calibration with bounded exploration,
confidence intervals, change-point detection, and conservative fallback when
uncertainty is high. Learned predictors are candidates only if deterministic
budgets and an auditable non-learned fallback remain available.

## D.5 Distributed metadata and failure semantics

A metadata match can outlive its payload, location, lease, or producer. Network
partitions and partial failures can also leave transfer completion ambiguous.
NexusKV needs explicit rules for ownership, epochs, leases, invalidation,
replication, garbage collection, and recovery.

The metadata system should prefer false misses over unsafe hits. Storage
durability does not imply semantic freshness, and semantic compatibility does
not imply physical availability.

## D.6 Multi-tenant isolation

Prefix hashes and timing can reveal workload similarity even when payloads are
not readable. Shared capacity also creates interference and denial-of-service
risks.

Future deployments require tenant namespaces, authorization at lookup and
materialization, optional keyed hashes or salts, quotas, secure deletion, and
telemetry redaction. Cross-tenant reuse should be disabled unless explicitly
authorized by deployment policy.

## D.7 Resource interference

Prefetch consumes links, pinned host memory, device reservations, CPU cycles,
and storage IOPS. These resources can delay non-reusing requests. Optimization
must therefore include fairness and tail-latency budgets, not only the target
request's Effective Gain.

Future scheduling work should coordinate cache queues with request admission
and expose backpressure across the Control Plane, Intelligence Layer, and Data
Plane.

## D.8 Evaluation limits

Hit rate, transferred bytes, and microbenchmark bandwidth do not validate the
architecture. Results must include misses, fallbacks, competing traffic,
failures, Model State variants, and a recompute baseline under the same
Inference Runtime and workload.

Until the matrix in Section 9 is executed on native hardware, zero overhead is
an optimization target. It is not an empirical property of NexusKV v1.0.

## D.9 Future directions

The following directions are consistent with the architecture but remain
hypotheses:

1. topology-aware selection among direct, staged, storage-backed, and
   route-to-state paths;
2. verified conversion between compatible layouts or quantized representations;
3. joint cache/request scheduling with explicit fairness and SLO constraints;
4. cross-engine descriptor conformance and trace replay;
5. attention-aware partial materialization for sparse or hybrid state;
6. a shared Model State fabric built from replaceable storage and transfer
   components.

Each direction must preserve safe recomputation as the default when evidence,
compatibility, or timing is insufficient.
