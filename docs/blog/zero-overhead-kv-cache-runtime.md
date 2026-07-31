# Zero-Overhead KV Cache: Why the Next Generation Cache System Must Hide Itself

## Introduction

LLM inference is entering a new phase. KV Cache is no longer just a memory optimization technique; it is becoming a distributed system problem involving memory hierarchy, scheduling, transfer, and model state management.

Projects such as vLLM prefix caching, SGLang HiCache, LMCache, and Mooncake Store solve different parts of this problem. However, they share a fundamental challenge:

> A cache that blocks GPU computation is not a real acceleration.

The ultimate goal is not simply storing more KV. The goal is **zero-overhead cache**: cache operations should be hidden behind computation whenever possible.

---

## The hidden cost of KV reuse

Without external cache:

```
Request
  |
  v
Prefill / Decode
  |
  v
GPU compute
```

With cache:

```
Request
  |
  v
Lookup
  |
  v
Load KV
  |
  v
Synchronize
  |
  v
GPU compute
```

The cache introduces new costs:

- metadata lookup
- index traversal
- memory movement
- DMA synchronization
- network transfer
- GPU pipeline stalls

A cache hit does not automatically mean faster inference.

---

## Existing systems and their tradeoffs

## vLLM Prefix Cache

vLLM's approach is optimized for the lowest latency path.

The KV cache remains inside GPU memory and uses paged memory management:

```
Logical KV blocks
        |
        v
Physical GPU blocks
```

Advantages:

- minimal overhead
- mature runtime integration
- excellent single-engine performance

Limitation:

- limited cache capacity
- no global KV intelligence

---

## SGLang HiCache

HiCache introduces a memory hierarchy:

```
GPU HBM
  |
CPU DRAM
  |
Remote KV Store
```

It solves the capacity problem by moving KV between tiers.

The key problem becomes scheduling:

- when should KV move?
- where should KV live?
- which requests deserve GPU residency?

HiCache is strong because it is close to the inference runtime, but it is tightly coupled with the engine.

---

## LMCache

LMCache acts as a KV cache middleware layer.

Its strength is abstraction:

```
Inference Engine
       |
       v
   LMCache
       |
 +-----+------+
 |            |
CPU        Mooncake
Storage   Backend
```

Advantages:

- multiple backend support
- engine integration
- reusable cache lifecycle management

Limitation:

The cache identity model is still mainly token-prefix oriented. Future models introduce more state types:

- MLA latent states
- DSA states
- KDA recurrent attention states

A future cache system needs a richer state descriptor.

---

## Mooncake Store

Mooncake focuses on the data plane.

Its core idea:

> Treat KV cache as a distributed object that can be transferred efficiently.

Key technologies:

- distributed memory pool
- RDMA-based transfer
- GPU-aware movement
- prefill/decode disaggregation

Mooncake is extremely strong at moving data.

However, it does not answer higher-level questions:

- Is this KV compatible with this model state?
- Is recomputation cheaper than transfer?
- Should this KV be prefetched now?

---

# The real problem: hiding cache latency

The next generation cache system needs to move from:

```
Store KV
Load KV
Use KV
```

into:

```
Predict KV need
       |
       v
Prefetch asynchronously
       |
       v
Overlap with GPU computation
       |
       v
Consume without stall
```

This is similar to CPU cache design.

Modern CPUs are fast not only because caches are large, but because hardware predicts future access patterns.

LLM inference needs the same idea:

**KV prefetching and scheduling.**

---

# Toward a KV Cache Intelligence Layer

A future KV system should contain:

## 1. State Descriptor

A cache object should describe itself:

```
model
attention type
layer
layout
precision
checkpoint
compatibility
```

KV is not just bytes.

It is model execution state.

---

## 2. Intelligent Planner

The system should decide:

- reuse or recompute?
- partial reuse or full restore?
- GPU, CPU, or remote placement?

---

## 3. Prefetch Scheduler

The most important missing component:

```
Future request
      |
      v
Predict required state
      |
      v
Async transfer
      |
      v
GPU compute continues
```

---

# NexusKV direction

NexusKV should not become another Mooncake Store.

The differentiation is not faster object storage.

The opportunity is a KV intelligence layer:

```
              Inference Engine

          vLLM / SGLang / ...

                    |
                    v

               NexusKV

        Descriptor
        Index
        Planner
        Prefetch Scheduler
        Placement Policy

                    |

          Mooncake / RDMA / CPU / SSD
```

The future competition will not only be about who stores the most KV.

It will be about who can make KV invisible to the GPU.

That is the definition of zero-overhead cache.
