# 📐 Attention 状态描述符规范 (Attention State Descriptor Specification)

在现代大语言模型（LLM）、多模态模型（VLM/DiT）以及线性/循环状态模型（SSM/Mamba）中，推理中间状态已经远远超出了传统“MHA Key/Value 张量对”的简单范畴。**NexusKV** 引入了统一的 **Attention State Descriptor（注意力状态描述符契约）**，作为 Rust 存储内核、Go 控制面以及 Python 引擎 Hook 之间的无缝契约。

---

## 1. 核心架构契约字段 (Core Contract Fields)

每个状态描述符包含以下不可变 Schema 属性：

```typescript
interface AttentionStateDescriptor {
  descriptor_id: string;              // 状态契约唯一标识符 (例如: "deepseek-v3-mla-page16")
  engine_family: EngineFamily;        // 引擎适配器 (VLLM, SGLANG, TENSORRT_LLM, CXX_FFI)
  semantic_type: StateSemanticType;  // 语义类型 (MHA_KV, MLA_LATENT, DSA_SPARSE, KDA_RECURRENT, SSM_MAMBA2)
  granularity: Granularity;          // 粒度 (TOKEN, PAGE, BLOCK, SEGMENT, CHECKPOINT)
  tensor_specs: TensorSpec[];        // 包含的角色张量列表 (KEY, VALUE, LATENT_KV, ROPE_K, RECURRENT_STATE)
  layout: LayoutMetadata;            // 物理内存布局 (interleaved, page_tokens, stride_bytes)
  quantization: QuantizationMetadata;// 量化参数 (scheme: fp8/int4/none, scale_factors)
  compatibility_flags: string[];     // 兼容性标志 (EXACT_REUSE, COW_BRANCH, ZERO_COPY_RDMA)
}
```

---

## 2. 现代注意力与循环状态描述符定义

### 1. MHA / GQA / MQA (标准注意力机制)
适用于 LLaMA-3、Qwen-2.5、Mistral 等传统 Transformer 架构。
* **物理张量**:
  * Key 张量 $\mathbf{K} \in \mathbb{R}^{B \times S_{\text{page}} \times H_{kv} \times D}$
  * Value 张量 $\mathbf{V} \in \mathbb{R}^{B \times S_{\text{page}} \times H_{kv} \times D}$
* **内存步长**: $\delta_{\text{bytes}} = 2 \times S_{\text{page}} \times H_{kv} \times D \times b_{\text{elem}}$

### 2. DeepSeek MLA (Multi-Head Latent Attention 多头潜空间注意力)
适用于 DeepSeek-V2 / DeepSeek-V3 / DeepSeek-R1。
* **物理张量**:
  * 压缩隐向量 $\mathbf{c}_t^{KV} \in \mathbb{R}^{d_c}$ ($d_c = 512$)
  * 解耦 RoPE 键向量 $\mathbf{k}_t^R \in \mathbb{R}^{d_R}$ ($d_R = 64$)
* **物理优势**: 相比标准 MHA 节省 93.3% 显存空间，NexusKV 直接对 $c_t^{KV}$ 隐向量进行页面池化与跨节点传输。

### 3. DeepSeek DSA (DeepSeek Sparse Attention 稀疏注意力)
适用于长文本稀疏注意力选择。
* **物理张量**:
  * 稀疏块数据 $\mathbf{X}_{\text{sparse}} \in \mathbb{R}^{K_{\text{topk}} \times S_{\text{block}} \times H \times D}$
  * Selector 路由索引表 $\mathbf{I}_{\text{sparse}} \in \mathbb{Z}^{K_{\text{topk}}}$
* **调度处理**: 结合动态 Top-K 索引匹配，按需拉取物理 Block。

### 4. Kimi KDA / Delta Attention (循环终端 Checkpoint)
适用于 Kimi K3 超长上下文 (10M+ Token) 循环衰减注意力。
* **物理张量**:
  * 终端循环状态矩阵 $\mathbf{h}_t \in \mathbb{R}^{d_{\text{state}} \times d_{\text{state}}}$
  * 滑动窗口历史衰减因子 $\boldsymbol{\gamma}_t \in \mathbb{R}^{d_{\text{state}}}$
* **存储特性**: 以 Token 级 Segment 为单位生成常驻 Checkpoint，复用时直接从终端 $h_t$ 增量推演。

### 5. Mamba2 / Selective SSM (选择性状态空间模型)
适用于 Jamba、Mamba-2-Hybrid 架构。
* **物理张量**:
  * SSM 循环隐状态 $\mathbf{h}_t \in \mathbb{R}^{B \times H \times N \times D}$
  * 1D 卷积时间窗口缓冲区 $\mathbf{x}_{\text{conv}} \in \mathbb{R}^{B \times (d_{\text{inner}} + 2 \cdot d_{\text{state}}) \times K_{\text{width}}}$
* **存储特性**: 无需保存全量序列 KV，仅保存固定 $O(N \cdot D)$ 字节的 State Checkpoint。

### 6. DeepSeek NSA (Native Sparse Attention 原生稀疏注意力)
适用于 2026/2027 原生稀疏长文本模型。
* **物理张量**:
  * 粗粒度摘要块 (Compressed Summary Blocks) $\mathbf{S}_{\text{summary}} \in \mathbb{R}^{\frac{L}{l_{\text{block}}} \times D}$
  * 精细度选择块 (Selected Fine Blocks) $\mathbf{S}_{\text{selected}} \in \mathbb{R}^{K \times l_{\text{block}} \times D}$

### 7. 多模态音视频 Vision KV Cache (DiT / Qwen2-VL / Sora)
适用于 Qwen2-VL、LLaVA-NeXT、Sora 类视频生成/理解模型。
* **物理张量**:
  * 2D/3D 时空位置编码 Key 张量 $\mathbf{K}_{\text{vision}} \in \mathbb{R}^{T \times H \times W \times D}$
  * Cross-Attention 文本-视觉交叉 Key/Value 张量
* **内存布局**: 支持按 Token 帧率 (Frame Rate) 与 Spatial Patch 切分物理 Handle。

### 8. Agentic Tree-of-Thought (ToT / MCTS 多分支 Agent 写时复制)
适用于 Agent 思考链、Tree-of-Thought、MCTS 多分支探索。
* **物理元数据**:
  * 父节点引用指针 `parent_entry_id`
  * 分支深度 `branch_depth`
  * 写时复制计数器 `cow_ref_cnt`
* **内存特性**: 多个 Agent 探索分支共享公共 Prompt KV 页，分叉时执行 $O(1)$ CoW 页表派生。

---

## 3. 描述符兼容性评估与路由矩阵

| 状态描述符 | 物理存储存储粒度 | 传输优化路径 | 复用评估函数 (Effective Gain) |
| :--- | :--- | :--- | :--- |
| **MHA / GQA** | Paged Block ($S_{\text{page}}=16$) | POSIX SHM / Direct RDMA | $G = T_{\text{compute}} - T_{\text{cache}} > 0$ |
| **DeepSeek MLA** | Latent Page ($c_t^{KV} + k_t^R$) | Zero-Copy RDMA Descriptors | $G = T_{\text{prefill-mla}} - T_{\text{rdma-latent}}$ |
| **DeepSeek DSA** | Sparse Mask Block | On-Demand Block Pull | $G = T_{\text{sparse-compute}} - T_{\text{sparse-pull}}$ |
| **Kimi KDA** | Checkpoint Segment | Direct SHM Pointer | $G = T_{\text{recurrent-recompute}} - T_{\text{h-t-load}}$ |
| **SSM Mamba2** | State Snapshot ($O(1)$) | Fast IPC Handle | $G = T_{\text{ssm-scan}} - T_{\text{snapshot-load}}$ |
| **Multimodal Vision**| Spatio-Temporal Tensor | Pinned Memory Pool | $G = T_{\text{vision-encode}} - T_{\text{vision-cache}}$ |
| **Agentic ToT** | CoW Radix Branch | $O(1)$ Page Pointer Reuse | $G = T_{\text{branch-recompute}} - 0$ ($O(1)$ Hit) |

---

## 4. 结论

通过将 Attention 状态建模为强类型的 **State Descriptor**，NexusKV 摆脱了传统缓存系统对单一“MHA Key/Value 张量”的硬编码假设，实现了对万亿 MoE (DeepSeek-V3)、超长上下文 (Kimi K3) 以及多模态 Agent 系统的 100% 物理支持。
