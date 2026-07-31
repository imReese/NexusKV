# A Survey of KV Cache Systems for LLM Inference

## From KV Storage to Zero-Overhead KV Intelligence

## Abstract

Large language model inference is increasingly limited by memory bandwidth, attention state management, and the cost of repeatedly computing long contexts. KV cache has become a first-class system resource. Recent systems including vLLM prefix caching, SGLang RadixAttention and HiCache, LMCache, and Mooncake Store explore different points in the design space.

This survey argues that the next generation of KV cache infrastructure should not be viewed as a storage problem alone. The central challenge is achieving **zero-overhead cache reuse**: cache operations must not stall model execution, waste GPU compute capacity, or introduce latency larger than the computation they replace.

We analyze existing architectures, identify their design trade-offs, and motivate a new direction: a KV intelligence layer that combines semantic state description, reuse planning, prefetch scheduling, placement optimization, and heterogeneous storage backends.

---

# 1. Introduction

Early LLM serving systems treated KV cache as an internal optimization of a single inference engine. Modern workloads changed this assumption:

- context lengths increased from thousands to millions of tokens;
- multi-turn conversations require repeated prefix reuse;
- agent workloads generate highly repetitive state;
- model architectures now include MLA, DSA, and other attention variants with non-traditional attention states.

KV cache is no longer only a tensor buffer. It has become a distributed runtime resource.

The key question has changed:

> How can we store more KV?

into:

> How can we reuse model state without slowing down inference?

---

# 2. The Hidden Cost of Cache

A cache hit does not automatically mean acceleration.

A naive pipeline:

```
Request
  |
Lookup
  |
Transfer KV
  |
Synchronize
  |
Attention compute
```

introduces additional costs:

- metadata lookup;
- network transfer;
- CPU/GPU memory copy;
- synchronization barriers;
- scheduler interference.

If recomputing the missing prefix costs 5ms but loading remote KV costs 8ms, the cache hit is a regression.

Therefore cache efficiency must consider:

```
benefit = recompute_cost - (lookup_cost + transfer_cost + synchronization_cost)
```

The objective is not maximum hit rate. The objective is maximum useful reuse.

---

# 3. Existing System Taxonomy

## 3.1 vLLM Prefix Cache

vLLM introduced practical KV memory management through PagedAttention.

Core idea:

```
Logical KV blocks
        |
        v
Physical GPU blocks
```

Strengths:

- extremely low overhead;
- GPU resident memory;
- mature runtime integration.

Limitations:

- mostly single-engine scope;
- limited cross-instance sharing;
- limited semantic understanding of cache state.

---

## 3.2 SGLang RadixAttention and HiCache

SGLang treats KV reuse as a prefix tree problem.

```
          root
         /    \
      userA   userB
        |
      prefix
```

HiCache extends this idea into hierarchical memory:

```
GPU HBM
  |
Host DRAM
  |
Remote KV Store
```

Strengths:

- strong prefix matching;
- runtime-aware placement;
- natural hierarchy model.

Limitations:

- tightly coupled with SGLang runtime;
- limited model-state abstraction.

---

## 3.3 LMCache

LMCache introduces a middleware abstraction:

```
Inference Engine
        |
     LMCache
        |
+-------+--------+
|                |
CPU Backend   Remote Backend
```

Strengths:

- clean backend abstraction;
- easy engine integration;
- flexible storage choices.

Limitations:

- cache identity is mainly token/chunk oriented;
- deeper attention-state semantics remain limited.

---

## 3.4 Mooncake Store

Mooncake approaches KV cache from the storage and transport perspective.

Architecture:

```
Client
 |
Metadata Service
 |
Placement
 |
Transfer Engine
 |
Storage Backend
```

Its major innovation is high-performance KV movement:

- RDMA;
- GPU direct transfer;
- distributed memory pooling;
- prefill/decode disaggregation.

Strengths:

- excellent data plane;
- large-scale distributed deployment;
- hardware-aware transfer.

Limitations:

A storage system sees objects, not model semantics.

It does not inherently know:

- attention type;
- tensor layout;
- state dependency;
- restore cost.

---

# 4. The Missing Layer: KV Intelligence

Existing systems optimize different layers:

|System|Primary Problem|
|-|-|
|vLLM|GPU KV management|
|HiCache|hierarchical placement|
|LMCache|cache lifecycle|
|Mooncake|distributed KV movement|

The missing abstraction is:

```
What is this state?

Can it be reused?

Where should it live?

Should we load it or recompute it?
```

This requires a semantic layer.

---

# 5. Toward Zero-Overhead Cache

A future KV system should provide:

## 5.1 Async Prefetch

Cache movement should happen before computation requires it.

```
Predict future KV need
        |
        v
Prefetch asynchronously
        |
        v
GPU computation continues
```

---

## 5.2 Compute-Aware Scheduling

The scheduler should understand both requests and cache locations.

Instead of:

```
FIFO request scheduling
```

use:

```
request + KV locality aware scheduling
```

---

## 5.3 Cost-Based Reuse Planning

A cache system should decide:

```
reuse
 |
transfer
 |
recompute
```

based on actual cost.

---

## 5.4 Attention-State Awareness

Future models require generalized state descriptors.

Example:

```
StateDescriptor {
  model,
  attention_type,
  layer,
  state_type,
  tensor_layout,
  checkpoint,
  restore_cost
}
```

KV is becoming a broader concept: model execution state.

---

# 6. NexusKV Design Direction

NexusKV should not compete with Mooncake as a storage engine.

The proposed architecture:

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
Planner                     CPU/GPU Memory
Prefetch                    SSD
Policy
```

The goal is:

> Make KV reuse invisible to inference execution.

---

# 7. Conclusion

The future of KV cache infrastructure is not another Redis-like storage layer.

The important evolution is:

```
KV Buffer
   |
KV Cache
   |
Hierarchical KV Cache
   |
KV Intelligence Fabric
```

Mooncake solves movement.

LMCache solves lifecycle.

HiCache solves hierarchy.

The next challenge is solving intelligence: deciding what to cache, when to move it, and how to reuse it without slowing down the model.

NexusKV explores this missing layer.
