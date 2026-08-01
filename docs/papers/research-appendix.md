# Appendix E: Research and Evaluation Notes

This appendix supports the evidence and evaluation process for the
[NexusKV Whitepaper v1.0](a-survey-of-kv-cache-systems-for-llm-inference.md).
It does not repeat the system survey. Source categories are maintained in
[Appendix A](related-work.md), and version-sensitive capability claims are
maintained in [Appendix B](kv-cache-system-coverage.md).

## E.1 Evidence hierarchy

Architectural claims should use the strongest available evidence:

1. current source code or versioned API documentation for implemented behavior;
2. official design documentation for component boundaries and intended use;
3. peer-reviewed or preprint papers for evaluated mechanisms and historical
   claims;
4. project announcements only when the preceding sources are unavailable.

A paper result is not automatically evidence for current repository behavior,
and current documentation is not evidence that a performance result reproduced
in the NexusKV environment.

## E.2 Source-level review questions

For every Inference Runtime, middleware, storage, or transfer integration,
record answers to the following questions:

- What is the unit of identity: token prefix, block, chunk, object, descriptor,
  or checkpoint?
- Which component owns metadata truth and physical availability?
- Which component allocates destination memory and authorizes consumption?
- What signals completion, failure, cancellation, and retry?
- Are layout, dtype, model revision, position, and parallel scope checked?
- Can movement overlap execution, and what resource reservations make it
  visible?
- Who chooses among route-to-state, transfer, conversion, and recomputation?
- What happens when a match is partial, stale, late, or unsupported?

These questions prevent a transport feature from being misclassified as a
reuse policy or an index match from being reported as restored Model State.

## E.3 Claim ledger

Whitepaper claims should be labeled internally with one of four evidence states:

| State | Meaning | Permitted wording |
| --- | --- | --- |
| Implemented | Present in the current NexusKV tree and covered by an executable check | "implements" with the checked scope |
| Integrated | Exercised against a named external version in the target environment | "integrates" with versions and conditions |
| Measured | Produced by a reproducible experiment with artifacts | numerical claim with confidence and limitations |
| Proposed | Architecture or research direction not yet validated end to end | "proposes", "targets", or "may" |

The v1.0 zero-overhead architecture is **proposed**. Current descriptor,
planning, store, adapter, and policy components are **implemented** only within
the boundaries documented in the repository.

## E.4 Experiment record

Each reported experiment should preserve:

```text
revision:
hardware_topology:
software_versions:
model_and_attention_state:
dataset_or_trace:
request_arrival_process:
context_and_output_lengths:
reuse_distribution:
cache_tiers_and_capacities:
transfer_backends:
policy_and_cost_model:
warmup_and_trial_count:
baseline:
raw_artifact_location:
known_limitations:
```

The record must be sufficient to distinguish a warm-cache result from a cold
start and a simulated backend from native transfer.

## E.5 Required comparisons

The evaluation matrix in Section 9 should include these comparisons:

- recomputation with external reuse disabled;
- Inference Runtime-native prefix reuse;
- the selected external middleware or hierarchy with its native policy;
- NexusKV compatibility and cost decisions over the same Data Plane;
- oracle decisions using measured future costs, reported only as an upper
  bound.

Ablations should independently remove semantic validation, cost-based reuse,
asynchronous prefetch, cache-aware routing, admission budgets, and fallback.
This separates the contribution of the Intelligence Layer from the underlying
storage or transfer backend.

## E.6 Correctness validation

Performance evaluation begins only after correctness checks pass:

- incompatible model, layout, dtype, position, layer, shard, and state-type
  descriptors are rejected;
- exact and partial matches restore the expected token or checkpoint boundary;
- restored outputs meet declared numerical-equivalence thresholds;
- late, missing, corrupt, or cancelled state falls back deterministically;
- metadata and payload failures do not expose cross-tenant state;
- recurrent and sparse states validate their required dependencies.

## E.7 Performance reporting

Reports should publish distributions rather than only averages for TTFT, TPOT,
request latency, queue delay, lookup time, visible transfer, restore,
synchronization, and fallback. They should also include goodput, fairness,
resource utilization, transferred bytes, recomputed tokens, and the fraction of
hits that produced positive Effective Gain.

The central result is the change relative to recomputation under matched load.
A higher hit rate or isolated link bandwidth is supporting evidence, not the
optimization objective.

## E.8 Maintenance protocol

Before a whitepaper release:

1. re-check moving documentation links and record the review date in Appendix B;
2. compare current public interfaces with every capability mark in the matrix;
3. distinguish repository evidence from target architecture;
4. add new systems only when they materially change a surveyed boundary;
5. keep the main paper concise and place source details in the appendices;
6. run Markdown, local-link, SVG, spelling, grammar, and bibliography checks.
