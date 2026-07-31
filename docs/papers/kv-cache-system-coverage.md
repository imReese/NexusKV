# Coverage of Existing KV Cache Systems

This document tracks the major categories of modern LLM inference cache infrastructure.

## Covered Categories

| Category | Representative Systems |
|---|---|
| GPU resident KV management | vLLM PagedAttention, TensorRT-LLM KV cache |
| Prefix matching and runtime reuse | SGLang RadixAttention |
| Hierarchical KV cache | SGLang HiCache |
| Cache middleware | LMCache |
| Distributed KV storage | Mooncake Store |
| Transfer layer | NIXL |
| Model serving orchestration | NVIDIA Dynamo KV-aware components |

## Important Adjacent Areas

The following areas are related but not pure cache stores:

- speculative decoding systems (EAGLE, Medusa) optimize token generation rather than state storage;
- token compression methods optimize representation size rather than cache lifecycle;
- scheduler systems optimize request routing rather than state persistence.

## Remaining Research Gap

Most existing systems optimize one layer:

- allocation;
- movement;
- hierarchy;
- lifecycle.

The missing abstraction is a model-state intelligence layer that combines:

- semantic identity;
- reuse prediction;
- cost-based decisions;
- prefetch scheduling;
- attention-aware extensibility.
