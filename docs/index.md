# NexusKV

### *超越 KV Cache：打造大语言模型推理的零开销模型状态智能层*

[![NexusKV Unified CI](https://github.com/imReese/NexusKV/actions/workflows/ci.yml/badge.svg)](https://github.com/imReese/NexusKV/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Go Version](https://img.shields.io/badge/Go-1.23%2B-00ADD8?logo=go)](go/)
[![Rust Workspace](https://img.shields.io/badge/Rust-2021%20%7C%202024-000000?logo=rust)](rust/)
[![Python Suite](https://img.shields.io/badge/Python-3.11%20%7C%203.12-3776AB?logo=python)](python/)

---

## 💡 什么是 NexusKV？

**NexusKV** 是专为大语言模型推理平台设计的下一代**通用模型状态智能与分布式内存基础设施层（Universal Model State Intelligence & Memory Fabric）**。

NexusKV 的核心原则是**引擎无关 (Engine-Agnostic)**、**模型架构无关 (Model-Agnostic)** 以及 **硬件无关 (Hardware-Agnostic)**。无论是主流开源推理引擎（如 vLLM、SGLang、TensorRT-LLM、LMDeploy、TGI）还是企业自研 C++/Rust 推理引擎与网关，均可通过统一 Schema 契约无缝接入。

随着 LLM 推理全面迈向 **Prefill-Decode (PD) 分离架构**、**Sliding Window Attention (SWA)**、**线性/递归状态卸载 (Recurrent State)** 以及 **低秩 Latent/稀疏 Attention**，传统将 KV Cache 视为盲目命中驱动（Hit-Driven）的存储方案暴露出了严重的缺陷：**网络传输导致 TTFT 恶化、内存碎片化以及缺乏物理算力感知**。

NexusKV 通过解耦 **Go 分布式控制面 (LeaseManager / EpochTracker)**、**Rust 数据与 Radix 匹配引擎 (`nxradixtree-core` / `nexus-store`)** 以及 **通用 C-FFI / Python 挂载协议** 解决了这一难题。它提供了基于 **有效收益评估方程 ($G = T_{compute} - T_{cache} > 0$)** 的智能决策引擎，支持 **Quota 主动反压**，并提供 **<1ms 极速 Fail-Open 平滑降级保障**。

---

## ⚡ 技术对比：NexusKV vs. 最新推理引擎 Cache 体系

| 架构维度 | 原生 Prefix Caching (vLLM V2 / SGLang Unified Radix) | HiCache & LMCache (多级缓存) | Mooncake Store & NIXL (传输/存储底层) | **NexusKV (模型状态智能层)** |
| :--- | :--- | :--- | :--- | :--- |
| **引擎目标** | vLLM V2 MRV2 / SGLang UnifiedRadix | 引擎 Sidecar 进程 | RDMA / NVLink 驱动 | **解耦的分布式控制与智能决策平台** |
| **缓存复用策略** | 本地盲目命中驱动 | 本地盲目命中驱动 | 存储块拉取 | **基于有效收益评估 ($G = T_{compute} - T_{cache} > 0$)** |
| **PD 分离握手** | 引擎进程间 IPC | 块级别传输 | 裸 RDMA 内存拷贝 | **`pd_disaggregate_handshake` 与动态 Cost 调优** |
| **Attention 体系** | Paged KV / Mamba States | 标准 MHA / Paged KV | 裸 Tensor Blobs | **原生支持 MLA ($c_t^{KV} + k_t^R$)、DSA 稀疏区及 KDA Checkpoint** |
| **系统过载表现** | 被动驱逐本地块 | 被动驱逐本地块 | 网络队列阻塞 | **`QuotaTracker` 活页内存与并发主动反压** |
| **系统韧性保障** | I/O 导致引擎挂起 | I/O 阻塞风险 | 传输挂起风险 | **<1ms Fail-Open 降级保障（毫秒级退回到 GPU 重算）** |
| **系统架构解耦** | 嵌入在 Worker 进程 | Python Sidecar | C++ 传输驱动 | **Go 控制面 + Rust 数据引擎 + Python FFI** |

---

## 🚀 核心架构与模块导航

- [📖 快速开始与部署指南](quickstart_cn.md)：单节点部署、vLLM/SGLang 适配器与集成说明。
- [🏛 架构全景指南](architecture_cn.md)：分布式 Radix 前缀树、租约协议、CXL 3.0/NIXL 数据面说明。
- [📚 Beyond KV-Cache 学术白皮书](papers/beyond-kv-cache_cn.md)：系统推导、数学证明与测试数据。
- [🗺 2026-2028 技术路线图](roadmap_cn.md)：NexusKV 长期演进计划。
