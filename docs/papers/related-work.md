# Appendix A: Related Work and Source Map

This appendix supports the [NexusKV Whitepaper v1.0](a-survey-of-kv-cache-systems-for-llm-inference.md).
It records the boundary and primary evidence for each related-work category;
the main paper remains the narrative entry point. The map covers **Model State
Infrastructure**, not every optimization used by an Inference Runtime.

## A.1 Inference Runtime memory management

### PagedAttention and vLLM

PagedAttention maps logical sequence blocks to non-contiguous physical KV Cache
blocks. The key systems contribution is allocator indirection: memory can be
shared and reclaimed at block granularity without reserving one contiguous
buffer per request. vLLM later added hash-based automatic prefix caching,
hybrid-state coordination, native offload paths, and external KV connectors.

- Primary paper: [PagedAttention](https://arxiv.org/abs/2309.06180)
- Current design: [Automatic Prefix Caching](https://docs.vllm.ai/en/stable/design/prefix_caching/)
- Current API evidence: [KV Cache interface](https://docs.vllm.ai/en/latest/api/vllm/v1/kv_cache_interface/)
- NexusKV boundary: vLLM owns allocation and consumption; NexusKV would provide
  cross-engine identity and cost decisions.

### RadixAttention and SGLang

RadixAttention represents shared token prefixes in a radix tree and exposes that
structure to the scheduler. This couples prefix matching, reference management,
and eviction to the request lifecycle, which is useful for structured programs
and multi-turn workloads.

- Primary paper: [SGLang](https://arxiv.org/abs/2312.07104)
- Current project: [SGLang repository](https://github.com/sgl-project/sglang)
- NexusKV boundary: SGLang retains Inference Runtime scheduling authority; the
  shared contract should not replace radix-tree lifecycle.

### TensorRT-LLM

TensorRT-LLM uses block pools, radix lookup for cross-request reuse, priority and
duration-based retention, host offload, KV Cache lifecycle events, and an
external connector interface. Pool construction accounts for differing
attention windows and KV-head counts.

- Current design: [KV Cache System](https://nvidia.github.io/TensorRT-LLM/features/kvcache.html)
- Internal model: [KV Cache Management](https://nvidia.github.io/TensorRT-LLM/advanced/kv-cache-management.html)
- Connector boundary: [KV Cache Connector](https://nvidia.github.io/TensorRT-LLM/features/kv-cache-connector.html)
- NexusKV boundary: connector and event integration must preserve engine pool,
  layout, and retention semantics.

## A.2 Hierarchy and cache middleware

### SGLang HiCache

HiCache extends the SGLang radix lifecycle from GPU HBM into host memory and
external storage. Its advantage is timing knowledge: the Inference Runtime knows
which prefix is active and when a page will be consumed. The corresponding
trade-off is integration depth and version coupling.

- Design overview: [HiCache](https://lmsys.org/blog/2025-09-10-sglang-hicache/)
- Backend example: [Mooncake Store integration](https://github.com/sgl-project/sglang/blob/main/python/sglang/srt/mem_cache/storage/mooncake_store/README.md)

### LMCache

LMCache externalizes cache lifecycle through engine connectors. Its current
recommended multiprocess mode runs a standalone service with an L1 manager, L2
adapters, and asynchronous store, prefetch, and eviction controllers. This
supports process isolation and shared node-local capacity, but adds protocol,
hash, and layout consistency requirements.

- Primary paper: [LMCache](https://arxiv.org/abs/2510.09665)
- Current architecture: [LMCache MP Overview](https://docs.lmcache.ai/mp/)
- Legacy boundary: [In-process mode is deprecated](https://docs.lmcache.ai/legacy/index.html)

### FlexKV and AIBrix

FlexKV combines a distributed radix index, multi-tier storage, transfer
orchestration, leases, and asynchronous engine connectors. AIBrix provides a
cloud-native offload framework with L1/L2 placement and vLLM/SGLang integration.
Both illustrate convergence between middleware, hierarchy, and distributed
coordination.

- [FlexKV architecture](https://github.com/taco-project/FlexKV)
- [AIBrix KV Cache Offloading Framework](https://aibrix.readthedocs.io/latest/designs/aibrix-kvcache-offloading-framework.html)

## A.3 Distributed storage and transfer

### Mooncake Store and Transfer Engine

The Mooncake serving paper includes a KV Cache-centric scheduler and
prefill/decode disaggregation. The reusable open-source components have narrower
boundaries: Mooncake Store provides distributed object capacity and metadata;
the Transfer Engine moves registered buffers through high-performance transport
paths. Store clients participate directly in the Data Plane instead of sending
payload bytes through the metadata master.

- Primary paper: [Mooncake](https://arxiv.org/abs/2407.00079)
- Current store design: [Mooncake Store](https://github.com/kvcache-ai/Mooncake/blob/main/docs/source/design/mooncake-store.md)
- Project source: [Mooncake repository](https://github.com/kvcache-ai/Mooncake)

### NIXL

NIXL abstracts memory sections, transfer backends, and remote metadata. Its
asynchronous buffer-list interface spans device memory, host memory, and storage
plugins. It is a transfer substrate; the caller supplies Model State identity,
placement, and admission policy.

- Current design: [NIXL](https://github.com/ai-dynamo/nixl/blob/main/docs/nixl.md)

### InfiniStore

InfiniStore contributes RDMA-registered shared memory, DRAM/SSD capacity,
variable-length keys, local-copy preference, and layer-wise transfer hooks. Its
key/value contract carries identifiers chosen by the integrating middleware or
Inference Runtime rather than enforcing a universal state schema.

- Current design: [InfiniStore](https://bytedance.github.io/InfiniStore/design.html)
- LMCache integration: [InfiniStore backend](https://docs.lmcache.ai/kv_cache/storage_backends/infinistore.html)

## A.4 Scheduling and disaggregated serving

### Dynamo and llm-d

Dynamo's KV router combines prefix overlap with active prefill/decode load.
Worker lifecycle events populate a global KV Cache index; KVBM adds a tiered
block manager over NIXL. llm-d similarly composes approximate or precise
prefix-aware routing, event indexing, and native or external offloaders.

- [Dynamo Router Design](https://docs.nvidia.com/dynamo/latest/design-docs/component-design/router-design)
- [Dynamo KVBM](https://docs.nvidia.com/dynamo/dev/knowledge-base/modular-components/kvbm/overview)
- [llm-d KV Cache Management](https://llm-d.ai/docs/0.7/architecture/advanced/kv-management)

### Research systems

Mooncake, DistServe, and MemServe study prefill/decode disaggregation and shared
memory from different scheduling perspectives. Preble studies distributed
prompt scheduling with prefix locality and load. These systems establish that
routing to cached state can substitute for transfer, but that locality must be
balanced against queueing and fairness.

- [DistServe](https://arxiv.org/abs/2401.09670)
- [MemServe](https://arxiv.org/abs/2406.17565)
- [Preble](https://arxiv.org/abs/2407.00023)

## A.5 Representation and attention-state work

Representation methods reduce the capacity or movement cost that a lifecycle
system observes:

- [KIVI](https://arxiv.org/abs/2402.02750) quantizes KV Cache state;
- [H2O](https://arxiv.org/abs/2306.14048) retains heavy-hitter tokens;
- [CacheGen](https://arxiv.org/abs/2310.07240) compresses and streams KV Cache;
- [MLA](https://arxiv.org/abs/2405.04434) changes the per-token state into a
  compressed latent representation;
- [DSA](https://arxiv.org/abs/2512.02556) introduces learned sparse selection;
- [KDA](https://arxiv.org/abs/2510.26692) uses a recurrent finite state in a
  hybrid attention architecture.

These techniques can reduce `T_transfer` or `B_i` in the whitepaper cost model,
but may add conversion, accuracy, or selection constraints. NexusKV's role is to
describe and cost the chosen representation, not to define one compression or
attention algorithm.

## A.6 Excluded adjacent areas

The following areas are outside the primary survey boundary unless they change
Model State lifecycle:

- speculative decoding and draft-model scheduling;
- weight loading, weight caching, and model parallelism;
- attention kernel implementation without reusable-state changes;
- generic databases and object stores without an inference integration;
- request batching that does not use state locality.

This boundary prevents evidence about one optimization layer from being treated
as evidence about an entire serving stack.
