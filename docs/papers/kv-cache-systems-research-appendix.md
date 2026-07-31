# Appendix: Deep Research Extensions for KV Cache Intelligence

This appendix extends **Beyond KV Cache: Building a Zero-Overhead Model State Intelligence Layer for LLM Inference** with deeper implementation analysis, architecture diagrams, and research methodology.

---

# 1. Source-Level Architecture Analysis

## 1.1 Mooncake Store: Data Plane First

Mooncake approaches KV infrastructure as a distributed storage and transport problem.

Conceptually:

```
Application / Runtime
        |
        v
      Client
        |
        v
 Metadata Service
        |
        v
 Placement Manager
        |
        v
 Transfer Engine
        |
        v
 Storage Backend
```

The key design philosophy:

> Move model state as efficiently as possible.

Important technology choices:

- RDMA-oriented transfer path;
- GPU-aware memory movement;
- distributed memory pooling;
- prefill/decode disaggregation support.

The limitation is abstraction level. A storage layer sees objects:

```
object_id -> bytes
```

but an inference system needs:

```
model state -> semantic meaning -> reuse decision
```

Mooncake optimizes movement, but does not decide whether movement is worthwhile.

---

## 1.2 LMCache: Middleware and Lifecycle

LMCache inserts a management layer between inference engines and storage backends.

```
Inference Engine
        |
        v
    LMCache
        |
 +------+------+------+
 |             |      |
CPU Cache   Remote   Disk
```

Its strength is abstraction:

- engine integration;
- backend flexibility;
- lifecycle management.

The remaining challenge is cache identity.

A chunk abstraction is useful today, but future models require richer state descriptions.

---

## 1.3 HiCache: Runtime Hierarchy

HiCache treats cache as a memory hierarchy problem.

```
GPU HBM
  |
  v
Host DRAM
  |
  v
Remote KV Storage
```

Its major contribution is not storage, but deciding:

- when to demote state;
- when to promote state;
- where state should reside.

The trade-off is runtime coupling.

---

# 2. Zero-Overhead Cache Architecture

The critical observation:

> Cache should not exist on the critical execution path.

A future architecture should look like:

```
                 Request Scheduler
                         |
                         v
                 KV Intelligence
                         |
        +----------------+----------------+
        |                                 |
        v                                 v
 Cost Model                         Prefetch Engine
        |                                 |
        +----------------+----------------+
                         |
                         v
                 Storage Fabric

        GPU HBM | Host RAM | Remote KV | SSD
```

Core components:

## State Descriptor

Defines what the cached state represents.

```
model
attention type
layer
layout
checkpoint
restore cost
```

## Reuse Planner

Chooses:

```
reuse
transfer
recompute
```

based on estimated cost.

## Prefetch Scheduler

Moves state before execution requires it.

The objective:

```
GPU compute time >= cache movement time
```

so transfer latency becomes hidden.

---

# 3. Evaluation Methodology

A serious KV cache system should not be evaluated by cache hit rate alone.

## Workload Matrix

### Context Length

```
8K
32K
128K
1M
```

### Reuse Ratio

```
25%
50%
75%
90%
```

### Storage Tier

```
GPU HBM
Host DRAM
Remote Memory
SSD
```

---

## Metrics

Traditional:

```
Cache hit rate
```

is insufficient.

Required metrics:

```
TTFT
TPOT
Throughput
GPU utilization
Transfer overhead
Effective acceleration
```

The key metric should be:

```
Effective Gain
=
Computation Saved
-
Cache Overhead
```

A cache system that saves computation but reduces throughput has failed its purpose.

---

# Research Position

Existing systems each solve an important layer:

```
vLLM      -> GPU memory management
HiCache   -> hierarchy
LMCache   -> lifecycle
Mooncake  -> distributed movement
```

The missing layer is:

```
NexusKV -> intelligence
```

The goal is not another KV store.

The goal is a model-state intelligence fabric that makes cache reuse invisible to inference execution.
