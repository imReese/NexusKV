# 🧭 NexusKV 长期技术路线图与业界演进规划 (2026 - 2028 Roadmap)

---

## 💡 一、 业界技术演进趋势与深度思考 (Industry Landscape Analysis)

在 2026 年大模型基础设施领域，推理架构正在经历一场深刻的物理变革。通过对业界最新工程实践与学术前沿（NVIDIA NIXL、CXL 3.0 TraCT、UALink 2.0、SGLang HiCache、Mooncake Store 等）的深入调研，我们梳理出 **5 大核心技术趋势**：

### 1. Prefill-Decode (PD) 彻底分离与柜级 (Rack-Scale) 算力解耦
* **趋势**：LLM 推理从简单的单机推导，全面迈向 **Prefill-Decode 物理节点分离**。Prefill 节点关注 Compute 带宽，Decode 节点关注 Memory 带宽。
* **挑战**：跨节点 KV Cache 搬运的网络开销（TTFT / TBT）已取代单纯的计算延迟，成为影响大模型服务质量的首要瓶颈。

### 2. 存储与传输物理介质跃迁 (Transport Fabric Evolution)
* **趋势**：从传统的网络层传输（TCP / RDMA），快速走向 **CXL 3.0 柜级共享内存池 (Load/Store 直接挂载)** 与 **UALink 2.0 / NIXL 跨卡跨机极速总线**。
* **挑战**：控制面必须具备对物理介质拓扑（CXL/NVLink/RDMA/Shared DRAM）的动态识别与最优路径选择能力。

### 3. 多模型 Attention 状态通用化 (Universal State Taxonomy)
* **趋势**：Attention 架构呈爆发式多样化——密集型（MHA/GQA）、低秩潜空间（DeepSeek MLA）、滑动窗口与全局摘要（CSA/HCA/SWA）以及固定尺寸递归状态（Kimi KDA, Mamba-2, Gated DeltaNet, RWKV-7）并存。
* **挑战**：基础设施层不能再将 KV Cache 简单视作一堆无差异的二进制 Block，必须感知**状态语义与物理衰减开销**。

### 4. Agent 复杂推演与分支前缀树 (Agentic Multi-Branch KV Tree)
* **趋势**：大模型应用全面从单轮 Chat 转向 Agentic AI、Tree-of-Thought (ToT)、MCTS 搜索以及投机采样（Speculative Decoding）。
* **挑战**：大量的分支分叉生成导致传统的单链 Cache 匹配失效，亟需 **Copy-on-Write (CoW) 零开销多分支 Radix 前缀树**。

### 5. 机密计算与安全防护 (Confidential Memory Fabric)
* **趋势**：金融、医疗、政企等敏感场景对大模型推理提出了严苛的数据隐私保护要求。
* **挑战**：跨节点传输与共享 HBM/DRAM 内存池中的 KV Tensor 必须防止恶意节点窃听与内存 Dump 攻击。

---

## 🏛 二、 4 大战略演进阶段 (Strategic Execution Phases)

```text
 ┌───────────────────────────────────────────────────────────────────────────────────┐
 │ Phase 1: 生产级稳健落地与多引擎通用契约 (2026 Q3)                                     │
 │ - 完善 Go/Rust/Python 三层解耦，企业级多租户配额硬隔离                               │
 └─────────────────────────────────────────┬─────────────────────────────────────────┘
                                           │
                                           ▼
 ┌───────────────────────────────────────────────────────────────────────────────────┐
 │ Phase 2: 柜级硬件池化与下一代传输协议 (2026 Q4 - 2027 Q1)                           │
 │ - CXL 3.0 共享内存池 (TraCT) / 原生 NIXL SDK & UALink 2.0 传输支持                   │
 └─────────────────────────────────────────┬─────────────────────────────────────────┘
                                           │
                                           ▼
 ┌───────────────────────────────────────────────────────────────────────────────────┐
 │ Phase 3: Agentic 树状分叉与推测式预读智能中枢 (2027 Q2 - 2027 Q3)                     │
 │ - 多分支 Copy-on-Write (CoW) 树 / Speculative Agent 预读 / 动态出价算力-传输优化器 │
 └─────────────────────────────────────────┬─────────────────────────────────────────┘
                                           │
                                           ▼
 ┌───────────────────────────────────────────────────────────────────────────────────┐
 │ Phase 4: 自愈自治与机密计算安全网络 (2027 Q4+)                                      │
 │ - TEE 机密内存写加密 / 硬件故障零中断热迁移 / 基于热点分布的 PD 节点弹性伸缩           │
 └───────────────────────────────────────────────────────────────────────────────────┘
```

---

### 阶段 1：生产级稳健落地与多引擎通用契约 (Phase 1 - 2026 Q3)

* **核心目标**：完成 Go+Rust+Python 架构的稳健闭环，提供开箱即用的通用引擎接入契约，打磨千卡集群生产环境可用性。
* **关键里程碑**：
  - [x] **C-FFI / Native IPC 通用契约**：统一 Rust C-FFI / PyO3 / POSIX SHM 接口，完全解耦底层推理引擎。
  - [x] **Attention 全族系描述符**：完善 MHA, GQA, MLA, CSA/HCA, KDA, GDN, Mamba-2 状态描述符契约 ([state.py](file:///Users/reese/Code/imReese/NexusKV/python/nexuskv/adapters/state.py))。
  - [x] **4D 复合效用 Router**：支持基于端到端收益、节点并发负载、显存水位与网络开销的智能调度。
  - [ ] **企业级多租户配额硬隔离 (Multi-Tenant Hard Quota)**：完善 `TenantQuotaManager` 的硬隔离与软反压，防止单一租户刷爆 Shared Memory。
  - [ ] **可观测性 Dashboard**：提供基于 OpenTelemetry / Prometheus 的全局 Cache 命中率、物理搬运带宽 (GB/s) 与 Fail-Open 降级事件监控。

---

### 阶段 2：柜级硬件池化与下一代传输协议 (Phase 2 - 2026 Q4 ~ 2027 Q1)

* **核心目标**：全面拥抱 CXL 3.0 柜级共享内存池与 NIXL / UALink 传输原语，打破节点物理存储壁垒。
* **关键里程碑**：
  - [ ] **CXL 3.0 内存池集成 (CXL Shared Memory Fabric)**：实现对 CXL 3.0 柜级共享内存的直接 Load/Store 挂载，消除 RDMA 网络跳数（0-Hop Zero Network Transfer）。
  - [ ] **NIXL & UALink 2.0 原生传输引擎**：集成 NVIDIA NIXL SDK 原生驱动与开源 UALink 2.0 高速总线，实现跨厂商 Accelerators 的硬件级传输对接。
  - [ ] **国产算力专属 Page 引擎**：针对华为昇腾 (CANN HCCL)、寒武纪 (MLU Memory)、摩尔线程 (MUSA) 提供特定硬件对齐与内存碎片优化。

---

### 阶段 3：Agentic 树状分叉与推测式预读智能中枢 (Phase 3 - 2027 Q2 ~ 2027 Q3)

* **核心目标**：赋能 Agentic AI、MCTS 搜索与投机采样的复杂分叉 Cache 共享与主动推测式预读。
* **关键里程碑**：
  - [ ] **Multi-Branch Radix Tree (多分支并发 CoW 树)**：针对 Tree-of-Thought (ToT) 与 MCTS 搜索，实现 Copy-on-Write (CoW) 零开销多分支状态共享。
  - [ ] **Speculative Agent Prefetcher (推测式 Agent 预读引擎)**：根据 Agent 动作语义拓扑图，在后台提前 2~3 个 Step 将潜在冷 Cache 异步加载至 Host DRAM / Device HBM。
  - [ ] **动态算力-传输开销出价机制 (Dynamic Trade-off Bidding Optimizer)**：根据物理网络实时拥塞状态与 GPU 实时算力利用率，自动出价决策“直接网络搬运 Cache”还是“触发本地 GPU 重新 Compute”。

---

### 阶段 4：自愈自治与机密计算安全网络 (Phase 4 - 2027 Q4+)

* **核心目标**：打造千卡以上超大规模集群零人工干预的自治高可用网络与机密计算保护。
* **关键里程碑**：
  - [ ] **TEE 机密内存加密传输 (Confidential KV Fabric)**：结合 Intel TDX / AMD SEV / NVIDIA Confidential Compute，为 HBM/DRAM 共享池中的 KV Tensor 提供硬件级写入加密。
  - [ ] **零中断状态热迁移 (Zero-Downtime Live Migration)**：当某一 GPU 节点发生 ECC 报错或硬件故障时，控制面毫秒级完成 HBM 状态至热备节点的平滑迁移。
  - [ ] **基于缓存热度的 PD 节点弹性伸缩 (Cache-Aware Auto-Scaler)**：根据全局前缀树热点变化，自动动态调整 PD 分离集群中 Prefill 与 Decode 节点的物理 GPU 数量比例。
