# 🧭 NexusKV 高速演进路线图 (2026 季度敏捷规划)

---

## 💡 一、 2026 年最新大模型推理前沿与硬件演进 (2026 Tech Landscape)

在大模型基础设施领域，推理架构正在经历由 **DeepSeek-V4** 与 **NVIDIA Vera Rubin / Blackwell Ultra** 引领的物理级变革：

1. **硬件辅助 Prefill-Decode (PD) 物理卸载**：随着 **NVIDIA Rubin CPX**（Prefill 专属加速处理器）与 Rubin GPU (HBM4) 的普及，Prefill 与 Decode 在物理硬件层实现彻底解耦，跨节点 0-Hop 内存传输与算力感知成为核心关隘。
2. **DeepSeek-V4 混合 Attention 架构 (CSA + HCA)**：DeepSeek-V4（1M+ 拓扑上下文）全面引入 **Compressed Sparse Attention (CSA)** 与 **Heavily Compressed Attention (HCA)**，相比 V3 降低 90% 的 KV Cache 内存负担，要求基础设施层原生支持混合压缩标度与动态 Scale 重映射。
3. **极速总线与开放内存织网 (NVLink 6 & UALink 2.0)**：物理传输全面拥抱 **6th-Gen NVLink**、**UALink 2.0 开放织网** 与 **CXL 3.1 柜级共享内存**，实现跨卡跨机 Fabric-Attached Memory 挂载。
4. **NVFP4 / FP4 硬件量化标度与 Agent 多分支 CoW**：在 NVFP4 低精度推理下，状态层需原生支持 FP4 量化 Scale 标度与 Agentic ToT / MCTS 搜索的零开销 Copy-on-Write 前缀共享。

---

## 🏛 二、 2026 季度演进里程碑 (2026 Quarterly Milestones)

```text
 ┌───────────────────────────────────────────────────────────────────────────────────┐
 │ 2026 Q1 (已完成): 核心多语言架构闭环与零开销契约                                    │
 │ - Go 控制面 / Rust `nxradixtree-core` 前缀树 / Python FFI / Apache 2.0 开源协议  │
 └─────────────────────────────────────────┬─────────────────────────────────────────┘
                                           │
                                           ▼
 ┌───────────────────────────────────────────────────────────────────────────────────┐
 │ 2026 Q2 (推进中): DeepSeek-V4 (CSA/HCA) 混合 Attention 与 NVFP4 量化标度支持       │
 │ - DeepSeek-V4 CSA/HCA 混合 Attention 状态重映射 / NVFP4 Scale 对齐 / Helm & PyPI│
 └─────────────────────────────────────────┬─────────────────────────────────────────┘
                                           │
                                           ▼
 ┌───────────────────────────────────────────────────────────────────────────────────┐
 │ 2026 Q3: Rubin CPX Prefill 解耦、C++ Client SDK 与 Consistent Hashing             │
 │ - Rubin CPX Prefill/Decode 硬件握手 / RCU 无锁 Radix Tree / C++ SDK / 节点自愈    │
 └─────────────────────────────────────────┬─────────────────────────────────────────┘
                                           │
                                           ▼
 ┌───────────────────────────────────────────────────────────────────────────────────┐
 │ 2026 Q4: UALink 2.0 开放织网、CXL 3.1 柜级内存池与 Agent 多分支 CoW                 │
 │ - UALink 2.0 / CXL 3.1 柜级共享内存挂载 / Agentic ToT 零拷贝 Copy-on-Write 树     │
 └───────────────────────────────────────────────────────────────────────────────────┘
```

---

### 2026 Q1：核心多语言架构闭环与零开销契约 (已完成)

- [x] **Go 分布式控制面**：Raft 状态同步、WAL 持久化日志与 `TenantQuotaManager` 多租户硬隔离。
- [x] **Rust 内核与匹配引擎**：`nxradixtree-core` 前缀树匹配、`nexus-store` 锁页内存与 POSIX SHM 零拷贝。
- [x] **通用 C-FFI / Python 挂载协议**：完全解耦底层推理引擎，原生适配 vLLM、SGLang 等主流引擎。
- [x] **Apache 2.0 开源协议**：全面升级许可契约，赋予显式专利授权保护。

---

### 2026 Q2：通用稀疏/分块 Attention 拓扑与 Block 量化标度 (已完成)

- [x] **Grafana & 可观测性面板**：提供预设仪表盘 `grafana-dashboard.json`，实时监控命中率、传输带宽 (GB/s) 与 Fail-Open 事件。
- [x] **PyPI Wheel 自动化构建**：配置 `.github/workflows/release.yml`，支持多平台 Maturin 自动化二进制构建。
- [x] **Speculative Intent 预取引擎**：实现 Decode 前的 Token Prefix 意图异步流水线预加载 (`prefetch.py`)。
- [x] **通用稀疏/分块 Attention 拓扑与 Block 量化标度**：抽象 `SPARSE_INDEXED_STATE` 通用稀疏索引原语与 `SCALE_TENSOR` 通用量化标度对齐契约（支持 FP8/FP4/INT4 分块标度对齐与反量化补偿）。

---

### 2026 Q3：Rubin CPX Prefill 解耦、C++ Client SDK 与 Consistent Hashing

- [ ] **NVIDIA Rubin CPX 硬件级 PD 握手**：针对 Rubin CPX (Prefill 专属加速处理器) 与 Rubin HBM4 节点，实现硬件级 `pd_disaggregate_handshake` 卸载。
- [ ] **Lock-Free Concurrent Radix Tree**：在 `nxradixtree-core` 中演进 RCU (Read-Copy-Update) 节点指针更新，大幅降低百核并发查找争用。
- [ ] **Header-Only C++ Client SDK**：为 TensorRT-LLM、LMDeploy 及 C++ 自研推理网关提供轻量级 C++ 原生客户端库 (`nexuskv_client.h`)。
- [ ] **Consistent Hash Ring 节点自愈**：在 Go 控制面增加虚拟节点一致性哈希环，应对 Worker 节点动态扩缩容时的状态路由抖动。

---

### 2026 Q4：UALink 2.0 开放织网、CXL 3.1 柜级内存池与 Agent 多分支 CoW

- [ ] **UALink 2.0 & CXL 3.1 柜级共享内存池**：集成 UALink 2.0 开放总线与 CXL 3.1 TraCT，支持 Fabric-Attached Memory 直接 Load/Store 挂载，实现跨机 0-Hop 零网络开销传输。
- [ ] **Agentic 多分支 CoW Radix 树**：针对 ToT / MCTS 搜索，实现 Copy-on-Write 零开销多分支状态共享。
