# NexusKV

### *通用大语言模型状态智能与分布式内存基础设施层*

[![NexusKV Unified CI](https://github.com/imReese/NexusKV/actions/workflows/ci.yml/badge.svg)](https://github.com/imReese/NexusKV/actions/workflows/ci.yml)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)
[![Go Version](https://img.shields.io/badge/Go-1.23%2B-00ADD8?logo=go)](go/)
[![Rust Workspace](https://img.shields.io/badge/Rust-2021%20%7C%202024-000000?logo=rust)](rust/)
[![Python Suite](https://img.shields.io/badge/Python-3.11%20%7C%203.12-3776AB?logo=python)](python/)

---

## 💡 什么是 NexusKV？

**NexusKV** 是专为大语言模型推理平台设计的下一代**通用模型状态智能与分布式内存基础设施层**。

系统基于统一状态契约与多语言协同架构设计：
- **分布式控制面 (Go)**：基于 Raft 协议提供分布式租约管理、自愈哈希环与多租户硬隔离配额（Quota）。
- **高性能匹配与存储引擎 (Rust)**：`nxradixtree-core` 提供高并发前缀树匹配，`nexus-store` 提供锁页内存与 CXL/POSIX SHM 零拷贝管理。
- **通用挂载与决策框架 (Python / C-FFI / C++)**：基于端到端有效收益评估方程（$G = T_{compute} - T_{cache} > 0$）决策传输路径，并提供 <1ms Fail-Open 降级保障。

---

## ⚡ 架构特性对比

| 维度 | 本地引擎缓存 | 进程外缓存 Sidecar | **NexusKV** |
| :--- | :--- | :--- | :--- |
| **系统架构** | 嵌入在推理 Worker 进程 | Python 进程外侧车 | **Go 控制面 + Rust 数据引擎 + C++/Python SDK** |
| **复用决策** | 本地盲目命中 | 盲目块拉取 | **基于有效收益方程 ($G = T_{compute} - T_{cache} > 0$) 智能路由** |
| **Attention 支持** | 密集型 MHA / Paged KV | 裸 Tensor Blobs | **原生支持 MLA (512维 Latent Vector)、DSA 稀疏区与 KDA Checkpoint** |
| **物理传输** | 引擎内部 IPC | 进程间通信 | **CUDA IPC P2P 零拷贝、POSIX SHM、CXL 3.0 与 RDMA** |
| **多租户与过载** | 无隔离 / 被动驱逐 | 阻塞队列 | **`TenantQuotaManager` 主动反压与硬隔离** |
| **系统降级保障** | 阻塞挂起 | 传输挂起 | **<1ms 极速 Fail-Open 平滑退回 GPU 重算** |

---

## 🚀 核心架构与模块导航

- [📖 快速开始与部署指南](quickstart_cn.md)：单节点部署、vLLM/SGLang 适配器与集成说明。
- [🏛 架构全景指南](architecture_cn.md)：分布式 Radix 前缀树、租约协议与数据面说明。
- [📚 Beyond KV-Cache 学术白皮书](papers/beyond-kv-cache_cn.md)：系统设计、数学证明与基准测试数据。
- [🗺 2026 季度演进路线图](roadmap_cn.md)：NexusKV 季度敏捷路线图。
