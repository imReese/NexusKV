# Related Work

## Efficient LLM Inference Systems

Modern LLM inference systems increasingly treat memory management as a first-class performance concern. PagedAttention introduced a virtual-memory-inspired abstraction for KV Cache management, enabling efficient GPU memory utilization through block-based allocation.

RadixAttention extended prefix reuse by organizing cached states as a radix tree, allowing efficient matching of shared prompts in interactive workloads.

## KV Cache Hierarchy and Offloading

As context length grows, GPU memory becomes insufficient for retaining all reusable states. HiCache explores hierarchical KV Cache placement across GPU memory, host memory, and remote storage.

LMCache introduces a middleware abstraction that decouples inference engines from storage backends, enabling flexible KV lifecycle management.

## Distributed KV Cache Infrastructure

Mooncake proposes a KV-centric architecture for large-scale LLM serving. Its store and transfer engine focus on high-throughput distributed KV movement using modern networking technologies such as RDMA and GPU-aware data transfer.

NIXL provides a communication abstraction for efficient movement of large AI data objects across heterogeneous devices and networks.

## Emerging Model State Caching

Future attention architectures challenge the assumption that cache equals K/V tensors. MLA, DSA, and KDA introduce alternative execution states requiring richer descriptors, compatibility rules, and restoration semantics.

This motivates a transition from KV Cache systems toward generalized Model State Intelligence Layers.

## References

- PagedAttention: Efficient Memory Management for Large Language Model Serving with PagedAttention.
- RadixAttention: A Comprehensive Cache System for Large Language Model Serving.
- Mooncake: A KV Cache-centric Disaggregated Architecture for LLM Serving.
- LMCache: An Open-Source Library for LLM Inference Acceleration with KV Cache Management.
- SGLang HiCache Design Documentation.
- NVIDIA NIXL: NVIDIA Inference Xfer Library.
