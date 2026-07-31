# A Survey of KV Cache Systems for LLM Inference

## From KV Storage to Zero-Overhead Model State Intelligence

## Abstract

The rapid evolution of large language model inference has transformed KV cache from a local optimization technique into a distributed system resource. Modern inference engines such as vLLM and SGLang, cache middleware such as LMCache, and distributed KV systems such as Mooncake Store are exploring different parts of this emerging infrastructure.

However, existing approaches expose a fundamental limitation: **cache reuse itself introduces overhead**. A cache hit is not necessarily an acceleration. Metadata lookup, memory movement, synchronization, scheduling interference, and state restoration can consume more time than recomputing the missing computation.

This paper argues that the next generation of LLM cache infrastructure should move beyond storage-centric design toward a **zero-overhead cache intelligence layer**. Such a system must understand model state semantics, estimate reuse value, schedule asynchronous movement, and coordinate heterogeneous memory tiers without interrupting GPU execution.

We analyze current KV cache architectures, identify their strengths and limitations, and propose a future direction represented by NexusKV: a model-state-aware intelligence layer between inference engines and storage systems.

---

# 1. Introduction: KV Cache Is Becoming a System Resource

Early LLM serving systems treated KV cache as an implementation detail inside a single inference engine.

Modern workloads changed the problem:

- context windows expanded from thousands to millions of tokens;
- conversational workloads repeatedly reuse prefixes;
- agent systems maintain long-lived execution state;
- distributed inference separates prefill and decode workloads;
- new architectures such as MLA, DSA, and KDA introduce non-traditional attention states.

The fundamental question changed:

> How can we store more KV cache?

into:

> How can we reuse model execution state without slowing inference?

---

# 2. The Cache Paradox: A Hit Can Become a Miss

A traditional view assumes:

```
cache hit = performance gain
```

But the real execution path is:

```
Request
  |
  v
Cache lookup
  |
  v
Metadata resolution
  |
  v
Memory/network transfer
  |
  v
Synchronization
  |
  v
Attention execution
```

The effective benefit is:

```
useful_gain = recomputation_cost
              - lookup_cost
              - transfer_cost
              - synchronization_cost
              - scheduling_cost
```

Therefore, maximizing cache hit rate is not the final objective.

The objective is:

> maximize useful reuse while keeping cache overhead invisible to inference.

---

# 3. The Current Design Space

## 3.1 vLLM: GPU Resident KV Management

vLLM introduced practical large-scale KV management through PagedAttention.

Core abstraction:

```
Logical KV blocks
        |
        v
Physical GPU blocks
```

Advantages:

- extremely low runtime overhead;
- excellent GPU locality;
- mature production deployment.

Limitations:

- mainly bounded inside one runtime instance;
- limited cross-engine sharing;
- weak semantic understanding of model state.

vLLM optimizes the fastest cache: the cache that never leaves GPU memory.

---

## 3.2 SGLang RadixAttention and HiCache: Hierarchical Reuse

SGLang treats prefix reuse as a radix-tree problem.

```
             root
            /    \
       user A    user B
          |
       shared prefix
```

HiCache extends the idea into multiple memory tiers:

```
GPU HBM
  |
Host DRAM
  |
Remote KV Storage
```

Advantages:

- strong prefix matching;
- runtime-aware placement;
- natural hierarchy management.

Limitations:

- closely coupled with SGLang runtime;
- limited abstraction for future attention states.

---

## 3.3 LMCache: KV Cache Middleware

LMCache introduces a middleware layer between engines and storage backends.

```
Inference Engine
        |
     LMCache
        |
 +------+------+
 |             |
CPU Backend  Remote Backend
```

Advantages:

- clean storage abstraction;
- easier integration with inference engines;
- flexible backend ecosystem.

Limitations:

- cache identity is mainly chunk/token oriented;
- semantic state modeling remains limited.

LMCache solves how to manage cache, but not fully what the cache represents.

---

## 3.4 Mooncake Store: Distributed KV Data Plane

Mooncake approaches KV from a storage and transport perspective.

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

Major technologies:

- RDMA-based transfer;
- GPU-aware movement;
- distributed memory pooling;
- prefill/decode disaggregation.

Advantages:

- excellent data plane performance;
- scalable distributed architecture;
- hardware-aware transport.

Limitations:

A storage engine understands objects, not model semantics.

It does not naturally understand:

- attention type;
- tensor layout;
- checkpoint dependency;
- restoration cost.

---

# 4. Why Existing Systems Are Not Enough

Current systems optimize different layers:

|System|Primary Optimization|
|-|-|
|vLLM|GPU KV allocation|
|HiCache|Memory hierarchy|
|LMCache|Cache lifecycle|
|Mooncake|KV movement|

The missing question is:

```
What state is this?

Can it be reused?

Where should it live?

Should we move it or recompute it?

How can we hide the cost?
```

This is the missing KV intelligence layer.

---

# 5. Toward Zero-Overhead Cache

## 5.1 Asynchronous Prefetch

The cache system should predict future needs and load state before computation requires it.

```
Predict future reuse
        |
        v
Async prefetch
        |
        v
GPU continues computing
```

Cache movement becomes background activity instead of a synchronization point.

---

## 5.2 Compute-Aware Scheduling

Traditional schedulers understand requests.

Future schedulers must understand:

```
request + cache locality + transfer latency
```

A request with GPU-resident cache may be preferable to one requiring remote fetch, even if both arrive simultaneously.

---

## 5.3 Cost-Based Reuse Planning

The system should decide among:

```
reuse
transfer
recompute
```

based on actual execution cost.

Cache should become an optimization decision, not a mandatory path.

---

## 5.4 Model-State-Aware Cache

Future models require a generalized state abstraction.

Example:

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

The future abstraction may not be KV Cache anymore.

It is a Model State Cache.

---

# 6. NexusKV: The Missing Intelligence Layer

NexusKV should not compete with Mooncake as another distributed object store.

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
Planner                     GPU/CPU Memory
Prefetch                    SSD
Policy
```

NexusKV focuses on:

- state identity;
- semantic compatibility;
- reuse planning;
- prefetch scheduling;
- placement decisions;
- zero-overhead execution.

---

# 7. Future Research Questions

Important open problems:

## 7.1 Beyond KV

How should cache systems represent:

- MHA KV tensors;
- MLA latent states;
- DSA compressed attention state;
- KDA recurrent state?

## 7.2 Cache vs Recomputation

When is loading state slower than recomputing it?

## 7.3 Cross-Engine Compatibility

Can one cache fabric support vLLM, SGLang, TensorRT-LLM, and future runtimes?

## 7.4 Cache Intelligence

Can systems automatically learn:

- reuse probability;
- transfer cost;
- optimal placement;
- eviction policy?

---

# 8. Conclusion

The evolution of LLM cache infrastructure can be summarized as:

```
KV Buffer
   |
KV Cache
   |
Hierarchical KV Cache
   |
KV Intelligence Fabric
```

Mooncake solved KV movement.

LMCache solved KV lifecycle management.

HiCache solved KV hierarchy.

The next challenge is solving intelligence:

> How can model state be reused everywhere while inference behaves as if cache operations were free?

NexusKV explores this missing layer: a zero-overhead, model-aware cache intelligence fabric for next-generation LLM inference.
