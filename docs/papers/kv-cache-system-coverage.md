# Appendix B: Model State Infrastructure Coverage

This appendix is the version-sensitive coverage ledger for the
[NexusKV Whitepaper v1.0](a-survey-of-kv-cache-systems-for-llm-inference.md).
The main paper is the narrative entry point. This ledger records architectural
scope based on public source and design documentation reviewed on 1 August 2026.

## B.1 Capability definitions

| Capability | Meaning in this survey |
| --- | --- |
| Inference Runtime | Allocates, consumes, and releases state as part of model execution. |
| Middleware | Exposes cache lifecycle outside one Inference Runtime process. |
| Hierarchy | Manages residency across two or more memory or storage tiers. |
| Storage | Owns payload capacity, metadata, and object/block lifecycle. |
| Transfer | Registers and moves buffers across devices, hosts, or storage. |
| Scheduling | Selects a request, worker, or compute stage using cache information. |
| Intelligence | Applies an explicit compatibility, locality, placement, or cost decision. |

## B.2 Coverage matrix

| System | Inference Runtime | Middleware | Hierarchy | Storage | Transfer | Scheduling | Intelligence | Primary evidence |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| vLLM | ● | ○ | ◐ | — | ◐ | ● | ◐ | [Prefix caching](https://docs.vllm.ai/en/stable/design/prefix_caching/) |
| TensorRT-LLM | ● | ○ | ◐ | — | ◐ | ● | ◐ | [KV Cache System](https://nvidia.github.io/TensorRT-LLM/features/kvcache.html) |
| SGLang | ● | ○ | ○ | — | ◐ | ● | ◐ | [SGLang](https://github.com/sgl-project/sglang) |
| SGLang HiCache | ○ | ◐ | ● | ○ | ● | ◐ | ◐ | [HiCache](https://lmsys.org/blog/2025-09-10-sglang-hicache/) |
| LMCache | ○ | ● | ● | ◐ | ● | ◐ | ◐ | [MP architecture](https://docs.lmcache.ai/mp/) |
| Mooncake Store | — | ○ | ◐ | ● | ● | ◐ | — | [Store design](https://github.com/kvcache-ai/Mooncake/blob/main/docs/source/design/mooncake-store.md) |
| NIXL | — | ○ | — | ○ | ● | — | — | [NIXL design](https://github.com/ai-dynamo/nixl/blob/main/docs/nixl.md) |
| Dynamo / KVBM | ○ | ● | ● | ○ | ● | ● | ● | [Router](https://docs.nvidia.com/dynamo/latest/design-docs/component-design/router-design), [KVBM](https://docs.nvidia.com/dynamo/dev/knowledge-base/modular-components/kvbm/overview) |
| InfiniStore | — | ○ | ● | ● | ● | — | — | [Architecture](https://bytedance.github.io/InfiniStore/design.html) |
| FlexKV | ○ | ● | ● | ● | ● | ◐ | ◐ | [Repository design](https://github.com/taco-project/FlexKV) |
| AIBrix offload | ○ | ● | ● | ○ | ● | ◐ | ◐ | [Offload framework](https://aibrix.readthedocs.io/latest/designs/aibrix-kvcache-offloading-framework.html) |
| llm-d KV management | ○ | ● | ◐ | ○ | ○ | ● | ● | [Architecture](https://llm-d.ai/docs/0.7/architecture/advanced/kv-management) |
| NexusKV | ○ | ● | △ | — | — | △ | △ | This whitepaper and current repository contracts |

**Legend:** `●` primary responsibility; `◐` substantial built-in capability;
`○` adapter or ecosystem integration; `—` outside the primary scope; `△`
proposed NexusKV direction.

## B.3 Interpretation notes

### vLLM and TensorRT-LLM

Both Inference Runtimes implement local block reuse and scheduling. Both now
expose connector or event surfaces for external state. The `○` middleware mark
means an integration boundary exists; it does not mean the Inference Runtime is
itself a standalone cache service. Hierarchy is `◐` because host/native offload
exists but distributed storage remains connector-dependent.

### SGLang and HiCache

SGLang is separated from HiCache to distinguish the base Inference Runtime from
the optional hierarchy. HiCache owns Runtime-integrated promotion, demotion,
backup, prefetch, and storage attachment. Its Intelligence mark is `◐` because
decisions use radix lifecycle and tier state, but are not a cross-engine Model
State contract.

### LMCache

The matrix reflects LMCache MP mode, not the deprecated in-process architecture.
The standalone server owns L1/L2 lifecycle and asynchronous controllers;
Inference Runtimes attach through connectors. Scheduling is `◐` because LMCache
schedules cache work and prefetch, while request/worker admission remains with
the serving layer.

### Mooncake

The row describes Mooncake Store plus its Transfer Engine integration, not every
feature of the Mooncake serving platform in the FAST paper. The Store performs
metadata, placement, and lifecycle decisions for immutable objects, but does not
natively validate attention semantics or compare transfer with recomputation.

### NIXL

NIXL is intentionally narrow. It provides heterogeneous transfer abstractions
and asynchronous completion. Storage plugins are `○` because NIXL can reach
storage, but it does not own cache admission or object lifecycle.

### Dynamo and KVBM

Dynamo's router is an explicit Intelligence component for prefix locality and
worker-load cost. KVBM adds a tiered block layer over NIXL. The row does not
imply generalized MLA/DSA/KDA compatibility; the current public contract is
primarily KV Cache block and connector oriented.

### InfiniStore and FlexKV

InfiniStore focuses on registered shared capacity and transfer; engine-facing
semantics usually arrive through an integration layer. FlexKV includes more of
the index, hierarchy, transfer, and lifecycle stack, so it spans additional
columns. Neither distinction implies a performance ranking.

### NexusKV

NexusKV is marked as proposed where the repository has contracts and scaffolds
but not a production implementation. It has no `●` under Storage or Transfer
because the architecture is designed to compose Mooncake, NIXL, InfiniStore,
FlexKV, or other Data Plane capabilities.

## B.4 Future-direction criteria

A system belongs in the NexusKV future direction when it requires decisions
across at least three boundaries, for example:

- routing versus moving state;
- converting a state representation versus recomputing it;
- reserving device memory versus admitting another request;
- prefetching remote state versus preserving transfer bandwidth;
- validating a recurrent checkpoint versus accepting a token-prefix match.

Coverage should be updated when public interfaces materially change. Claims in
the main paper should cite a stable paper or current official design document,
not this matrix alone.
