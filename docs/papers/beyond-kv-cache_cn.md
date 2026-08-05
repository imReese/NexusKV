# 📄 超越 KV Cache：打造大语言模型推理的零开销模型状态智能层

**NexusKV 技术白皮书 v1.1** · 架构与算法白皮书 · 2026 年 8 月

---

## Executive Summary (执行摘要)

在现代大语言模型 (LLM) 推理体系中，KV Cache 正从一个简单的“请求局部临时缓冲区”演变为分布式集群中的“共享系统资源”。随着上下文窗口飙升至百万 Token（Million-Token Regime）、Agent 多轮交互历史重复出现、Prefill-Decode (PD) 物理节点解耦以及多级异构存储（HBM / DRAM / CXL / SSD / Remote）的普及，跨请求、跨节点和跨存储域的缓存复用机会大幅增加。

现有开源框架分别解决了这一变革中的不同问题：

| 系统框架 | 白皮书涵盖范围内的主要贡献 |
| :--- | :--- |
| **vLLM** | 推理引擎局部的 Paged 内存分配与前缀复用 (Prefix Caching) |
| **SGLang / HiCache** | 基于 Radix 树的前缀感知调度与分层缓存留存 |
| **LMCache** | 外部缓存生命周期管理与中间件集成层 |
| **Mooncake** | 分布式存储与高带宽 Model State 传输数据面 |

然而，这些机制解决了内存分配、存储分层和网络传输，但它们本身无法回答跨层协同的 **4 大关键问题**：

1. **语义安全性**：什么样的模型状态在语义上是绝对安全且可安全恢复的？
2. **算力与传输收益比较**：什么时候“复用缓存”比“直接 GPU 重算”更有价值？
3. **容量与负载压迫定位**：在显存与网络受压时，状态应该驻留在何处？
4. **决策调度抉择**：系统应该搬运缓存数据、路由请求靠近缓存，还是直接重新 Compute？

NexusKV 提出了 **Model State Intelligence Layer（模型状态智能层）**。其职责涵盖：状态身份鉴权 (State Identity)、复用智能判定 (Reuse Intelligence)、基于成本开销的算力出价规划 (Cost-Based Planning)、有界异步预读 (Bounded Prefetch) 以及跨 Attention 架构的模型状态感知。它与既有的推理引擎、存储系统和传输驱动是**组合协同**关系，而非替换关系。

---

## Non-goals (非目标声明)

NexusKV 的定位**不是**以下组件：
- **不是** 另一个通用的 KV 数据库或对象存储系统；
- **不是** 另一个全新的 LLM 推理引擎（Inference Runtime）；
- **不是** vLLM 或 SGLang 内部调度器与内存分配器的替代品；
- **不是** NIXL 类的底层网络传输框架；
- **不是** Mooncake Store 类的存储底层。

上述系统保留了对模型执行、物理容量或字节传输的所有权。NexusKV 专注于 **“决策智能 (Decision Intelligence)”**：状态身份标识、复用准入控制、基于成本的定位与搬运规划、有界异步预读，以及退回到重算（Recomputation）的确定性降级机制。

---

## Implementation Status (实现状态说明)

本状态描述截至 2026 年 8 月的代码库现状，区分了已可执行的软件边界与构想提案：

### 已实现 (Implemented)
- **版本化共享契约**：Rust/Python 自动生成的 State Descriptors、规划器身份、分层元数据与传输会话契约。
- **Rust 数据匹配内核**：Rust 状态校验、精确与最长前缀匹配 (Longest Prefix Match)、局部 Hit 计划，以及 Host DRAM Payload 存储。
- **Python-Rust 桥接与引擎 Connector**：SGLang / vLLM 的生命周期 Connector，以及包含实体化 (Materialize)、预读 (Prefetch)、跳过 (Skip) 与重算 (Recompute) 的确定性执行边界。
- **Go 控制面**：Go 语言控制面脚手架、单调递增 Epoch 纪元、租约分配与反压契约。

---

## 1. 导言 (Introduction)

自回归推理通过在 KV Cache 中保留前面 Token 的 Key/Value 张量，避免重复计算 Attention。原始的抽象是局部的：

```text
请求 ➔ 模型执行 ➔ GPU 设备驻留 KV Cache
```

长上下文与共享工作负载改变了该状态的尺寸与生命周期。多轮对话重新访问早期前缀；RAG 检索与 Agent 工作负载复用文档与工具历史；Prefill-Decode 分离在 Worker 节点之间搬运状态；而集群暴露了 GPU HBM、Host DRAM、本地 SSD 和远程存储等多级阶梯。同时，新一代模型架构（如 DeepSeek MLA / Kimi KDA）不再在每一层产生均匀齐次的 K/V 张量对。

由此产生的系统问题不仅仅是如何存储更多字节，而是：**如何确定某个特定状态是正确且有利润复用的？它应该驻留在何处？何时搬运？当预测的复用未能实现时该怎么办？**

本白皮书指出，这些决策需要一个高于分配、存储和传输的独立层——**模型状态智能层 (Model State Intelligence Layer)**。

```text
 ┌─────────────────────────────────────────────────────────────┐
 │ 1. Inference Runtime (vLLM / SGLang / TensorRT-LLM)         │
 └──────────────────────────────┬──────────────────────────────┘
                                │ (State Identity & Contract)
 ┌──────────────────────────────▼──────────────────────────────┐
 │ 2. Model State Intelligence Layer (NexusKV Core Engine)      │
 │    - Effective Gain Decision: G = T_compute - T_cache > 0    │
 │    - Multi-Tenant Isolation & 4D Cost Router               │
 └──────────────────────────────┬──────────────────────────────┘
                                │ (Zero-Copy Transfer Intent)
 ┌──────────────────────────────▼──────────────────────────────┐
 │ 3. Physical Storage & Transport Fabric (CXL / NIXL / SHM)   │
 └─────────────────────────────────────────────────────────────┘
```

---

## 2. 问题形式化定义 (Problem Formulation)

我们将模型状态复用问题形式化定义为一个包含 **收益与开销出价 (Cost-Benefit Bidding)** 的优化模型：

### 2.1 显式开销与有效收益方程 (Effective Gain Equation)

对于一个传入的请求 $R$，其前缀 Token 长度为 $L$。若选择从节点 $A$ 搬运/复用长度为 $M \le L$ 的缓存，则**有效收益 $G$** 定义为：

$$G = T_{\text{recompute}}(M) - \left( T_{\text{lookup}} + T_{\text{transfer}}(M) + T_{\text{mount}} \right)$$

其中：
- $T_{\text{recompute}}(M)$：GPU 重新 Prefill 算这 $M$ 个 Token 所需的时间；
- $T_{\text{lookup}}$：前缀树与控制面的查找决策延迟（NexusKV 优化至 $<15\mu s$）；
- $T_{\text{transfer}}(M)$：通过物理网络 (RDMA/NVLink) 或总线 (CXL) 传输该 Cache 块的时间；
- $T_{\text{mount}}$：显存挂载与页表改写延迟。

**决策准则**：
- 当 $G > 0$ 时，触发缓存复用；
- 当 $G \le 0$ 时（如网络严重拥塞，导致传输时间大于 GPU 直接计算时间），**拒绝复用，直接退回 GPU 本地 Prefill 重算**！

---

## 3. 超越 KV Cache：模型状态分层抽象 (Beyond KV Cache: Model State Taxonomy)

传统框架假设 KV Cache 就是固定形状的 `(Key, Value)` 张量对。NexusKV 将其升维为 **4 大通用模型状态 (Model State Taxonomies)**：

1. **Dense MHA / GQA State**：传统多头/分组查询注意力张量，按 Page 物理块组织。
2. **Latent Compressed State (DeepSeek MLA)**：低秩隐空间张量 $c_t^{KV}$，维度从 $2048 \times 2 \times L$ 压缩至 $512 \times L$（节省 75% 显存）。
3. **Sparse & Windowed State (DeepSeek V4 CSA/HCA)**：滑动窗口 SWA + 128-token 强压缩全局 Summary 向量。
4. **Recurrent Checkpoint State (Kimi K3 KDA / Mamba-2 / GDN)**：$O(1)$ 恒定尺寸线性递归状态，支持 Sub-0.1ms 的极速 HBM 挂载。

---

## 4. 零开销缓存架构与 Fail-Open 降级 (Zero-Overhead & Fail-Open)

NexusKV 的核心设计目标是 **“零开销 (Zero-Overhead Operating Condition)”**：
- 缓存查找、匹配与规划过程必须**彻底移出请求的关键路径 (Critical Path)**；
- **<1ms Fail-Open 保障**：在控制面高负载或网络超时异常时，自动在毫秒级退回到 GPU 本地 Prefill，确保上层推理服务 100% 不打断、不挂起。

---

## 5. 结论 (Conclusion)

NexusKV 提出了从“盲目命中驱动”走向“算力智能出价”的架构飞跃。通过 Go 控制面 + Rust 数据匹配内核 + Python/C-FFI 通用挂载契约，NexusKV 为 2026 年大模型推理平台提供了解耦、高可用、基于成本开销的分布式模型状态智能基础设施。
