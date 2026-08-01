# Limitations and Future Work

## Why NexusKV Is Not a Replacement for Existing Cache Systems

NexusKV does not attempt to replace inference engines, transport frameworks, or distributed storage systems.

The proposed architecture intentionally separates responsibilities:

- vLLM/SGLang/TensorRT-LLM remain responsible for model execution;
- Mooncake-like systems remain responsible for high-performance data movement;
- storage backends remain responsible for persistence and capacity;
- NexusKV focuses on Cache Intelligence.

The goal is not another KV Store. The goal is making state reuse an invisible optimization.

---

## Current Limitations

### 1. Inference Runtime Integration Complexity

Different inference engines expose different cache abstractions:

- vLLM uses paged KV blocks;
- SGLang uses radix-based prefix structures;
- TensorRT-LLM integrates KV management deeply into Inference Runtime execution.

A universal Intelligence Layer requires stable interfaces without sacrificing performance.

---

### 2. Attention State Generalization

Traditional KV Cache assumes:

```
Token -> K,V tensors
```

Future models require:

```
Token -> Model Execution State
```

Supporting MLA, DSA, KDA, and future architectures requires richer descriptors and compatibility rules.

---

### 3. Accurate Cost Modeling

A practical system must estimate:

```
reuse cost
transfer cost
restore cost
recompute cost
```

The optimal decision depends on hardware topology, workload pattern, and model architecture.

---

### 4. Distributed Consistency

A shared Model State cache introduces challenges:

- ownership;
- invalidation;
- version compatibility;
- failure recovery.

These require database-like consistency mechanisms.

---

# Future Research Directions

## Learned Cache Intelligence

Future systems may learn:

- reuse probability;
- workload locality;
- optimal placement;
- eviction strategy.

---

## Hardware-Aware Execution

Future cache systems should exploit:

- GPU Direct RDMA;
- NVLink;
- CXL memory;
- accelerator-specific memory hierarchy.

---

## Model State Fabric

The long-term vision is a shared state fabric for AI systems:

```
Model
 |
Execution State
 |
State Intelligence
 |
Distributed Fabric
```

KV Cache is the first instance of this broader concept.
