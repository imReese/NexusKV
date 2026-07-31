# Beyond KV Cache: Building a Zero-Overhead Model State Intelligence Layer for LLM Inference

## A Systematic Study of vLLM, SGLang HiCache, LMCache, Mooncake Store and the Future of KV Intelligence

## Abstract

Large language model inference is undergoing a fundamental transition. KV cache started as a local optimization inside a single inference engine, but has evolved into a distributed system resource shared across GPUs, hosts, and clusters.

Existing systems explore different parts of this design space. vLLM optimizes GPU-resident KV management, SGLang introduces radix-based reuse and hierarchical caching, LMCache provides cache lifecycle abstraction, and Mooncake Store builds a high-performance distributed KV data plane.

However, a fundamental problem remains unsolved:

> Cache reuse itself introduces overhead.

A cache hit does not necessarily improve performance. Lookup, metadata resolution, memory movement, synchronization, and scheduling interference may cost more than recomputing the missing computation.

This paper argues that future LLM cache infrastructure should evolve from storage-centric systems into a **zero-overhead model state intelligence layer**. Such a layer must understand state semantics, estimate reuse value, schedule asynchronous movement, and make cache operations invisible to inference execution.

NexusKV explores this missing abstraction layer.

---

# 1. Introduction

The original purpose of KV cache was simple:

```
Avoid recomputing previous attention states.
```

Modern workloads changed the problem:

- context lengths expanded from thousands to millions of tokens;
- multi-turn agents repeatedly reuse execution history;
- prefill/decode disaggregation separates computation stages;
- multiple inference engines need shared state infrastructure;
- new attention architectures introduce states beyond traditional K/V tensors.

The question is no longer:

> How can we store more KV cache?

The question becomes:

> How can we reuse model execution state without slowing inference?

---

# 2. Formalizing Cache Reuse

A cache object should not only represent a tensor. It represents reusable computation state.

We define a cache state:

```
C = (S, L, D, R)
```

where:

- S: state representation and size;
- L: physical location;
- D: dependency information;
- R: restoration cost.

A reuse decision has two possible paths.

Cache path:

```
T_cache = T_lookup + T_transfer + T_restore + T_sync
```

Recompute path:

```
T_compute = computation cost
```

Cache is beneficial only when:

```
T_cache < T_compute
```

Therefore:

> Hit rate is not the final optimization target. Useful reuse is.

---

# 3. Current KV Cache System Landscape

|System|Primary Optimization|Identity|Main Limitation|
|-|-|-|-|
|vLLM|GPU KV management|Block/Page|Limited global sharing|
|SGLang HiCache|Hierarchy and reuse|Radix prefix|Runtime coupling|
|LMCache|Lifecycle abstraction|Chunk|Weak state semantics|
|Mooncake|Distributed movement|Object|No model awareness|
|NexusKV|Intelligence layer|State descriptor|Research direction|

---

# 4. Existing Architecture Analysis

## 4.1 vLLM: GPU Resident Cache

vLLM's PagedAttention introduced a practical abstraction:

```
Logical KV blocks
        |
        v
Physical GPU blocks
```

Strengths:

- minimal overhead;
- excellent locality;
- production maturity.

Weakness:

The fastest cache is the cache that never leaves GPU memory. Once state moves outside GPU memory, new coordination problems appear.

---

## 4.2 SGLang HiCache: Hierarchical Runtime Cache

SGLang treats reuse as a prefix matching problem:

```
             root
            /    \
       user A    user B
```

HiCache extends storage hierarchy:

```
GPU HBM
 |
Host DRAM
 |
Remote Store
```

Strengths:

- strong prefix reuse;
- runtime-aware decisions;
- natural hierarchy.

Weakness:

The abstraction is still closely connected to one inference runtime.

---

## 4.3 LMCache: Cache Middleware

LMCache introduces an intermediate layer:

```
Inference Engine
        |
     LMCache
        |
 Backend Systems
```

Strengths:

- backend flexibility;
- engine integration;
- lifecycle management.

Weakness:

The primary abstraction remains chunk/token oriented. Future model states require richer semantics.

---

## 4.4 Mooncake Store: Distributed KV Data Plane

Mooncake focuses on movement and storage:

```
Client
 |
Metadata
 |
Placement
 |
Transfer Engine
 |
Storage Backend
```

Key technologies:

- RDMA;
- GPU-aware transfer;
- distributed memory pooling;
- prefill/decode disaggregation.

Strength:

Excellent data plane.

Limitation:

Storage systems understand objects, not model execution semantics.

They do not naturally understand:

- attention type;
- tensor layout;
- checkpoint dependency;
- restore cost.

---

# 5. Why Storage Is Not Enough

Existing systems optimize different layers:

```
vLLM       -> allocation
HiCache    -> hierarchy
LMCache    -> lifecycle
Mooncake   -> movement
```

The missing questions are:

```
What is this state?
Can it be reused?
Should it move?
Should it be recomputed?
How can the cost disappear?
```

This is the missing intelligence layer.

---

# 6. Toward Zero-Overhead Cache

## 6.1 Asynchronous Prefetch

Cache movement should leave the critical path.

```
Predict reuse
      |
Async transfer
      |
GPU continues execution
```

---

## 6.2 Cache-Aware Scheduling

Schedulers should consider:

```
request + cache locality + transfer latency
```

A request with local state may be more valuable than one requiring remote loading.

---

## 6.3 Cost-Based Reuse Planning

The runtime should choose:

```
reuse
transfer
recompute
```

according to actual cost.

Cache should become a decision, not an assumption.

---

# 7. Beyond KV Cache: Model State Cache

Future models challenge the traditional KV abstraction.

Traditional:

```
Token -> K,V tensors
```

Future:

```
Token -> Attention State

MHA KV
MLA latent state
DSA compressed state
KDA recurrent state
```

A future cache object requires:

```
StateDescriptor {
 model,
 attention_type,
 layer,
 state_type,
 tensor_layout,
 dependency,
 checkpoint,
 restore_cost
}
```

The future is not KV Cache.

It is Model State Cache.

---

# 8. NexusKV Architecture Proposal

NexusKV does not aim to replace Mooncake as a storage engine.

It provides the missing intelligence layer:

```
                 LLM Runtime

        vLLM / SGLang / Future Engines

                     |
                     |
               NexusKV API
                     |
       +-------------+-------------+
       |                           |
KV Intelligence              Data Plane

Descriptor                  Mooncake
Matcher                     RDMA/NIXL
Planner                     GPU/CPU Memory
Prefetch                    SSD
Policy
```

Core responsibilities:

- semantic state identity;
- compatibility checking;
- reuse planning;
- asynchronous prefetch;
- placement optimization;
- zero-overhead execution.

---

# 9. Evaluation Methodology

A future cache system should be evaluated by useful acceleration, not hit rate alone.

## Workloads

```
Context:
8K / 32K / 128K / 1M tokens

Reuse:
25% / 50% / 75% / 90% prefix overlap
```

## Storage tiers

```
GPU HBM
Host DRAM
Remote KV Store
SSD
```

## Metrics

```
TTFT
TPOT
Throughput
GPU utilization
Transfer overhead
Effective gain
```

The key metric:

```
Effective Gain = baseline latency - cache latency
```

---

# 10. Research Questions

## Cross-engine compatibility

Can one cache fabric support vLLM, SGLang, TensorRT-LLM and future runtimes?

## Attention-aware caching

How should systems cache different attention states?

## Learned cache intelligence

Can systems predict reuse probability and optimal placement automatically?

## Zero-overhead execution

Can cache movement become completely hidden behind GPU computation?

---

# 11. Conclusion

LLM cache infrastructure is evolving:

```
KV Buffer
   |
KV Cache
   |
Hierarchical KV Cache
   |
Distributed KV Fabric
   |
Model State Intelligence
```

Mooncake solved KV movement.

LMCache solved lifecycle management.

HiCache solved hierarchy.

The next challenge is intelligence:

> How can model state be reused everywhere while inference behaves as if cache operations were free?

NexusKV explores this missing layer: a zero-overhead, model-aware intelligence fabric for next-generation LLM inference.
