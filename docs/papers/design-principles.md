# NexusKV Design Principles

## 1. Semantic State Identity

A cache entry should represent reusable model execution state, not only a tensor buffer.

The identity model must capture:

- model architecture;
- attention mechanism;
- tensor layout;
- layer scope;
- state dependency;
- checkpoint version.

The goal is to make reuse correctness explicit.

---

## 2. Cost-Based Reuse

Cache reuse is an optimization decision.

The system should compare:

```
reuse cost = lookup + transfer + restore + synchronization

compute cost = recomputation
```

A cache hit is valuable only when it reduces end-to-end latency.

---

## 3. Asynchronous Prefetch

Cache movement should not block inference.

The runtime should predict future state requirements and move data in parallel with GPU execution.

```
Predict
  |
Prefetch
  |
Compute continues
```

---

## 4. Compute-Centric Scheduling

Scheduling decisions should consider both requests and state locality.

The scheduler should understand:

- GPU residency;
- host residency;
- remote transfer latency;
- expected reuse value.

---

## 5. Attention-Aware Extensibility

Future models will expose diverse execution states:

- MHA KV tensors;
- MLA latent states;
- DSA compressed states;
- KDA recurrent states.

NexusKV should provide a generalized model-state abstraction rather than being limited to traditional KV tensors.
