# 🧭 NexusKV 高速演进路线图 (2026 季度敏捷规划)

---

## 💡 一、 业界技术演进趋势 (Industry Trends)

在 2026 年大模型基础设施领域，推理架构正在经历高度敏捷的物理变革：

1. **Prefill-Decode (PD) 分离与节点解耦**：推理集群全面走向 Prefill/Decode 物理节点分离，跨节点 KV 传输开销已成为影响 TTFT 的首要瓶颈。
2. **新型 Attention 状态爆发**：以 DeepSeek-V3 / MLA（低秩潜空间）、Kimi-K1.5 / KDA（线性递归状态）为代表的多模态 Attention 架构快速普及，基础设施层需原生感知多种状态语义。
3. **硬件级物理总线跃迁**：物理传输从 TCP/RDMA 快速走向 NVLink P2P、CXL 3.0 柜级共享内存与 NIXL 跨机极速总线。
4. **Agentic 复杂多分支推演**：Agentic AI 与 Tree-of-Thought (ToT) 搜索要求底层提供 Copy-on-Write (CoW) 零开销多分支前缀状态共享与主动推测预取。

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
 │ 2026 Q2 (推进中): DeepSeek-V3 / MLA 深度优化与 Speculative 预取                    │
 │ - MLA 512维 Latent 压缩解耦 / Speculative Prefetch Engine / PyPI & Helm 自动化     │
 └─────────────────────────────────────────┬─────────────────────────────────────────┘
                                           │
                                           ▼
 ┌───────────────────────────────────────────────────────────────────────────────────┐
 │ 2026 Q3: 无锁并发前缀树、C++ Client SDK 与 Consistent Hashing                     │
 │ - RCU 无锁 Radix Tree 遍历 / C++ Header-Only SDK / Consistent Hash Ring 节点自愈  │
 └─────────────────────────────────────────┬─────────────────────────────────────────┘
                                           │
                                           ▼
 ┌───────────────────────────────────────────────────────────────────────────────────┐
 │ 2026 Q4: CXL 3.0 柜级内存池与 NIXL 硬件零拷贝                                       │
 │ - CXL 3.0 Shared Memory TraCT 挂载 / NIXL P2P 驱动集成 / Agent 多分支 CoW 树      │
 └───────────────────────────────────────────────────────────────────────────────────┘
```

---

### 2026 Q1：核心多语言架构闭环与零开销契约 (已完成)

- [x] **Go 分布式控制面**：Raft 状态同步、WAL 持久化日志与 `TenantQuotaManager` 多租户硬隔离。
- [x] **Rust 内核与匹配引擎**：`nxradixtree-core` 前缀树匹配、`nexus-store` 锁页内存与 POSIX SHM 零拷贝。
- [x] **通用 C-FFI / Python 挂载协议**：完全解耦底层推理引擎，原生适配 vLLM、SGLang 等主流引擎。
- [x] **Apache 2.0 开源协议**：全面升级许可契约，赋予显式专利授权保护。

---

### 2026 Q2：DeepSeek-V3 / MLA 深度优化与 Speculative 预取 (推进中)

- [x] **Grafana & 可观测性面板**：提供预设仪表盘 `grafana-dashboard.json`，实时监控命中率、传输带宽 (GB/s) 与 Fail-Open 事件。
- [x] **PyPI Wheel 自动化构建**：配置 `.github/workflows/release.yml`，支持多平台 Maturin 自动化二进制构建。
- [x] **Speculative Intent 预取引擎**：实现 Decode 前的 Token Prefix 意图异步流水线预加载 (`prefetch.py`)。
- [ ] **DeepSeek-V3 / MLA 原生状态重映射**：针对 DeepSeek-V3 的 MLA ($c_t^{KV} + k_t^R$) 512 维低秩潜向量进行层间 KV 压缩与量化 Scale 标度对齐。

---

### 2026 Q3：无锁并发前缀树、C++ Client SDK 与 Consistent Hashing

- [ ] **Lock-Free Concurrent Radix Tree**：在 `nxradixtree-core` 中演进 RCU (Read-Copy-Update) 节点指针更新，大幅降低百核并发查找争用。
- [ ] **Header-Only C++ Client SDK**：为 TensorRT-LLM、LMDeploy 及 C++ 自研推理网关提供轻量级 C++ 原生客户端库 (`nexuskv_client.h`)。
- [ ] **Consistent Hash Ring 节点自愈**：在 Go 控制面增加虚拟节点一致性哈希环，应对 Worker 节点动态扩缩容时的状态路由抖动。

---

### 2026 Q4：CXL 3.0 柜级内存池与 NIXL 硬件零拷贝

- [ ] **CXL 3.0 柜级共享内存池**：集成 CXL 3.0 TraCT，支持共享内存 Load/Store 直接挂载，实现跨机 0-Hop 零网络开销传输。
- [ ] **NVIDIA NIXL 原生传输引擎**：集成 NIXL P2P 驱动，实现跨卡跨机 GPU 内存硬件级传输。
- [ ] **Agentic 多分支 CoW Radix 树**：针对 ToT / MCTS 搜索，实现 Copy-on-Write 零开销多分支状态共享。
