# 🧭 NexusKV 高速演进路线图 (2026 季度规划)

---

## 💡 1. 2026 前沿大模型推理与硬件互联演进

在 2026 年，大模型推理生态正在经历由 **DeepSeek-V4** 与 **NVIDIA Vera Rubin / Blackwell Ultra** 平台主导的剧烈演进：

1. **硬件辅助 Prefill-Decode (PD) 分离**: 随着 **NVIDIA Rubin CPX**（专用 Prefill 加速芯片）和 Rubin HBM4 GPU 的推出，Prefill 与 Decode 在物理芯片层实现了真正的解耦。
2. **DeepSeek-V4 混合注意力 (CSA + HCA)**: DeepSeek-V4 引入了 **压缩稀疏注意力 (CSA)** 与 **重度压缩注意力 (HCA)** 架构，针对 100 万 Token 以上超长上下文，将 KV 显存开销降低 90%。
3. **极速互联与 Fabric 挂载内存**: 物理传输正向 **第 6 代 NVLink**、**UALink 2.0 开放 Fabric 协议** 以及 **CXL 3.1** 机架级共享内存池演进。
4. **NVFP4 量化与 Agent 多分支 CoW**: 在 NVFP4 低精度推理下，状态层原生对齐 FP4 量化 Scale 因子，并为 Tree-of-Thought (ToT) Agentic 搜索提供零开销 Copy-on-Write 前缀共享。

---

## 🏛 2. 2026 季度里程碑

```text
 ┌───────────────────────────────────────────────────────────────────────────────────┐
 │ 2026 Q1 (已完成): 核心多语言引擎与零开销契约抽象                                   │
 │ - Go 控制面 / Rust `nxradixtree-core` / Python FFI / Apache 2.0 许可证           │
 └─────────────────────────────────────────┬─────────────────────────────────────────┘
                                           │
                                           ▼
 ┌───────────────────────────────────────────────────────────────────────────────────┐
 │ 2026 Q2 (推进中): DeepSeek-V4 (CSA/HCA) 稀疏/块状注意力与 NVFP4 量化对齐            │
 │ - DeepSeek-V4 CSA/HCA 状态映射 / NVFP4 Scale 对齐 / Helm & PyPI                  │
 └─────────────────────────────────────────┬─────────────────────────────────────────┘
                                           │
                                           ▼
 ┌───────────────────────────────────────────────────────────────────────────────────┐
 │ 2026 Q3 (计划中): Rubin CPX Prefill 解耦、C++ SDK 与一致性哈希                        │
 │ - Rubin CPX Prefill 卸载 / RCU 无锁 Radix Tree / C++ SDK / 自愈机制               │
 └─────────────────────────────────────────┬─────────────────────────────────────────┘
                                           │
                                           ▼
 ┌───────────────────────────────────────────────────────────────────────────────────┐
 │ 2026 Q4 (计划中): UALink 2.0 开放 Fabric、CXL 3.1 内存池与 Agent 多分支 CoW        │
 │ - UALink 2.0 / CXL 3.1 Fabric 挂载内存 / Agentic ToT CoW 树                        │
 └───────────────────────────────────────────────────────────────────────────────────┘
```

---

### 2026 Q1: 核心多语言引擎与零开销契约抽象 (已完成)

- [x] **Go 分布式控制面骨架与本地 WAL**: 单节点选举状态机、顺序 WAL 日志追加/恢复，以及集群一致性哈希环拓扑。
- [x] **Rust 内核与匹配引擎**: `nxradixtree-core` 前缀树、`nexus-store` Host DRAM 内存分配器，以及契约内存结构。
- [x] **通用 C-FFI / Python 挂载协议**: 引擎无关架构设计；提供 vLLM 与 SGLang 的原生 C-FFI / PyO3 绑定与 Adapter 接口。
- [x] **Apache 2.0 开源许可证**: 明确的专利许可保护与开源合规。

---

### 2026 Q2: 通用稀疏/块状注意力拓扑与量化 Scale 对齐 (推进中)

- [x] **Grafana 与可观测性 Dashboard**: 预配置 `grafana-dashboard.json`，提供 Hit Rate 命中率 (Hits vs Misses)、GB/s 传输带宽以及 Fail-Open 降级事件监控。
- [x] **PyPI Wheel 自动化构建**: `.github/workflows/release.yml` 提供跨平台 Maturin 轮子打包流水线。
- [ ] **Speculative Intent 预取引擎**: Decode 阶段前的高并发 Pipeline 异步预加载 (`prefetch.py` 队列骨架)。
- [x] **通用稀疏/块状注意力与量化 Scale 原语**: 抽象 `SPARSE_INDEXED_STATE` 拓扑原语与 `SCALE_TENSOR` 量化 Scale 对齐契约。

---

### 2026 Q3: Rubin CPX Prefill 解耦、C++ SDK 与一致性哈希 (计划中)

- [ ] **NVIDIA Rubin CPX 硬件 PD 握手**: 针对 Rubin CPX Prefill 芯片的 `pd_disaggregate_handshake` 硬件卸载逻辑 (`rubin_handshake.py` 会话契约)。
- [ ] **无锁并发 Radix Tree**: `nxradixtree-core` 中的 RCU (Read-Copy-Update) 节点遍历，提升 CPU 多核并发。
- [x] ** Header-Only C++ Client SDK 增强**: 轻量级 C++ 客户端库 (`nexuskv_client.h`)，提供 Fail-Open 内存锁定与 `health_check()` 接口。
- [x] **一致性哈希环自愈机制**: Go 控制面虚拟节点哈希环 (`hashring.go`)，提供节点健康探测与平滑故障转移。

---

### 2026 Q4: UALink 2.0 开放 Fabric、CXL 3.1 内存池与 Agent 多分支 CoW (计划中)

- [ ] **UALink 2.0 与 CXL 3.1 机架级内存池**: Fabric 挂载内存直接 Load/Store 映射 (`CxlFabricMemoryPool` 与 `UALink2FabricTransport` 描述符模拟器)。
- [ ] **Agentic 多分支 CoW Radix Tree**: 用于 Tree-of-Thought (ToT) 与 MCTS 搜索的 Copy-on-Write 状态共享 (`fork_branch()`) 生命周期管理。
