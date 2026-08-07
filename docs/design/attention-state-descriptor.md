# 📐 Attention State Descriptor Specification

In modern Large Language Models (LLM), Vision-Language Models (VLM/DiT), and State Space Models (SSM/Mamba), inference intermediate state goes far beyond classic Multi-Head Attention Key/Value tensor pairs. **NexusKV** introduces a unified **Attention State Descriptor Contract** as the stable, strongly-typed interface shared across the Rust storage core, Go control plane, and Python engine connectors.

---

## 1. Core Contract Fields

Each descriptor contains the following immutable schema attributes:

```typescript
interface AttentionStateDescriptor {
  descriptor_id: string;              // Stable contract ID (e.g., "deepseek-v3-mla-page16")
  engine_family: EngineFamily;        // Engine adapter (VLLM, SGLANG, TENSORRT_LLM, CXX_FFI)
  semantic_type: StateSemanticType;  // Semantic type (MHA_KV, MLA_LATENT, DSA_SPARSE, KDA_RECURRENT, SSM_MAMBA2)
  granularity: Granularity;          // Granularity (TOKEN, PAGE, BLOCK, SEGMENT, CHECKPOINT)
  tensor_specs: TensorSpec[];        // Named role-bearing tensors (KEY, VALUE, LATENT_KV, ROPE_K, RECURRENT_STATE)
  layout: LayoutMetadata;            // Physical memory layout (interleaved, page_tokens, stride_bytes)
  quantization: QuantizationMetadata;// Quantization (scheme: fp8/int4/none, scale_factors)
  compatibility_flags: string[];     // Compatibility flags (EXACT_REUSE, COW_BRANCH, ZERO_COPY_RDMA)
}
```

---

## 2. 8 Modern Attention & Recurrent State Descriptors

### 1. MHA / GQA / MQA (Standard Attention)
Applies to LLaMA-3, Qwen-2.5, Mistral, and standard Transformer architectures.
* **Physical Tensors**:
  * Key Tensor $\mathbf{K} \in \mathbb{R}^{B \times S_{\text{page}} \times H_{kv} \times D}$
  * Value Tensor $\mathbf{V} \in \mathbb{R}^{B \times S_{\text{page}} \times H_{kv} \times D}$
* **Memory Stride**: $\delta_{\text{bytes}} = 2 \times S_{\text{page}} \times H_{kv} \times D \times b_{\text{elem}}$

### 2. DeepSeek MLA (Multi-Head Latent Attention)
Applies to DeepSeek-V2 / DeepSeek-V3 / DeepSeek-R1.
* **Physical Tensors**:
  * Compressed Latent KV Vector $\mathbf{c}_t^{KV} \in \mathbb{R}^{d_c}$ ($d_c = 512$)
  * Decoupled RoPE Key Vector $\mathbf{k}_t^R \in \mathbb{R}^{d_R}$ ($d_R = 64$)
* **Physical Efficiency**: Achieves 93.3% memory compression compared to standard MHA. NexusKV pools and transfers $c_t^{KV}$ latent pages directly.

### 3. DeepSeek DSA (DeepSeek Sparse Attention)
Applies to query-dependent sparse selection block regions in ultra-long contexts.
* **Physical Tensors**:
  * Sparse Block Data $\mathbf{X}_{\text{sparse}} \in \mathbb{R}^{K_{\text{topk}} \times S_{\text{block}} \times H \times D}$
  * Selector Routing Indices $\mathbf{I}_{\text{sparse}} \in \mathbb{Z}^{K_{\text{topk}}}$

### 4. Kimi KDA / Delta Attention (Recurrent Terminal Checkpoints)
Applies to Kimi K3 ultra-long contexts (10M+ tokens) with recurrent decay attention.
* **Physical Tensors**:
  * Terminal Recurrent State Matrix $\mathbf{h}_t \in \mathbb{R}^{d_{\text{state}} \times d_{\text{state}}}$
  * Sliding Window History Decay Factor $\boldsymbol{\gamma}_t \in \mathbb{R}^{d_{\text{state}}}$

### 5. Mamba2 / Selective SSM (State Space Models)
Applies to Jamba and Mamba-2-Hybrid architectures.
* **Physical Tensors**:
  * SSM Recurrent Hidden State $\mathbf{h}_t \in \mathbb{R}^{B \times H \times N \times D}$
  * 1D Convolutional Temporal Buffer $\mathbf{x}_{\text{conv}} \in \mathbb{R}^{B \times (d_{\text{inner}} + 2 \cdot d_{\text{state}}) \times K_{\text{width}}}$

### 6. DeepSeek NSA (Native Sparse Attention)
Applies to native sparse long-context models.
* **Physical Tensors**:
  * Compressed Summary Blocks $\mathbf{S}_{\text{summary}} \in \mathbb{R}^{\frac{L}{l_{\text{block}}} \times D}$
  * Selected Fine Blocks $\mathbf{S}_{\text{selected}} \in \mathbb{R}^{K \times l_{\text{block}} \times D}$

### 7. Multimodal Audio/Video Vision KV Cache (DiT / Qwen2-VL / Sora)
Applies to Qwen2-VL, LLaVA-NeXT, and Sora video generation and understanding models.
* **Physical Tensors**:
  * 2D/3D Spatio-Temporal RoPE Key Tensor $\mathbf{K}_{\text{vision}} \in \mathbb{R}^{T \times H \times W \times D}$
  * Cross-Attention Text-Vision Key/Value Tensors

### 8. Agentic Tree-of-Thought (ToT / MCTS Multi-Branching Agent CoW)
Applies to Agent reasoning chains, Tree-of-Thought, and MCTS multi-path exploration.
* **Physical Metadata**:
  * Parent Node Lineage Pointer `parent_entry_id`
  * Branch Depth `branch_depth`
  * Copy-on-Write Counter `cow_ref_cnt`

---

## 3. Descriptor Compatibility & Routing Matrix

| State Descriptor | Physical Storage Granularity | Transport Path | Effective Gain Evaluation |
| :--- | :--- | :--- | :--- |
| **MHA / GQA** | Paged Block ($S_{\text{page}}=16$) | POSIX SHM / Direct RDMA | $G = T_{\text{compute}} - T_{\text{cache}} > 0$ |
| **DeepSeek MLA** | Latent Page ($c_t^{KV} + k_t^R$) | Zero-Copy RDMA Descriptors | $G = T_{\text{prefill\_mla}} - T_{\text{rdma\_latent}}$ |
| **DeepSeek DSA** | Sparse Mask Block | On-Demand Block Pull | $G = T_{\text{sparse\_compute}} - T_{\text{sparse\_pull}}$ |
| **Kimi KDA** | Checkpoint Segment | Direct SHM Pointer | $G = T_{\text{recurrent\_recompute}} - T_{\text{h\_t\_load}}$ |
| **SSM Mamba2** | State Snapshot ($O(1)$) | Fast IPC Handle | $G = T_{\text{ssm\_scan}} - T_{\text{snapshot\_load}}$ |
| **Multimodal Vision**| Spatio-Temporal Tensor | Pinned Memory Pool | $G = T_{\text{vision\_encode}} - T_{\text{vision\_cache}}$ |
| **Agentic ToT** | CoW Radix Branch | $O(1)$ Page Pointer Reuse | $G = T_{\text{branch\_recompute}} - 0$ ($O(1)$ Hit) |

---

## 4. Conclusion

By modeling inference state as a strongly-typed **State Descriptor**, NexusKV eliminates hardcoded assumptions about classic MHA key/value tensors, enabling 100% physical support for trillion-parameter MoE (DeepSeek-V3), ultra-long context (Kimi K3), and multimodal Agent systems.
