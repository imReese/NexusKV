<div align="center">

# NexusKV

### *超越 KV Cache：打造大语言模型推理的零开销模型状态智能层*

[![NexusKV Unified CI](https://github.com/imReese/NexusKV/actions/workflows/ci.yml/badge.svg)](https://github.com/imReese/NexusKV/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Go Version](https://img.shields.io/badge/Go-1.23%2B-00ADD8?logo=go)](go/)
[![Rust Workspace](https://img.shields.io/badge/Rust-2021-000000?logo=rust)](rust/)
[![Python Suite](https://img.shields.io/badge/Python-3.11%20%7C%203.12-3776AB?logo=python)](python/)

[English](README.md) | [简体中文](README_CN.md)

</div>

---

## 💡 什么是 NexusKV？

**NexusKV** 是专为大语言模型（LLM）推理设计的下一代**模型状态智能层（Model State Intelligence Layer）**。不同于传统将 KV Cache 视为盲目命中驱动（Hit-Driven）的单体存储服务，NexusKV 引入了**成本驱动（Cost-Based）与零开销分层架构**，解耦了 Go 控制面、Rust 高性能数据引擎与 Python 推理引擎适配器。

NexusKV 的核心突破在于**超越传统的 KV Cache 范式**，原生统一支持现代异构 Attention 架构的状态描述符——包括 **DeepSeek MLA** (Multi-head Latent Attention 隐向量/位置切分)、**DeepSeek DSA** (DeepSeek Sparse Attention 稀疏选择区域) 以及 **Kimi KDA** (Kimi Delta Attention 循环终端 Checkpoint)。

---

## ⚡ 为什么选择 NexusKV？ (与现有方案对比)

| 特性 / 维度 | 引擎原生 Cache (vLLM/SGLang) | 多级缓存系统 (HiCache/LMCache) | 共享存储 (3FS/Mooncake) | **NexusKV (模型状态智能层)** |
| :--- | :--- | :--- | :--- | :--- |
| **缓存感知范围** | 单机/单 Worker 显存 | 单机多级 (HBM/DRAM/Disk) | 分布式存储 Blob | **跨节点全网 Model State 智能感知** |
| **复用决策依据** | 盲目命中驱动 | 盲目命中驱动 | 盲目存储拉取 | **基于有效收益评估 ($G = T_{compute} - T_{cache} > 0$)** |
| **Attention 架构** | 标准 MHA/GQA 页面 | 标准 MHA/GQA 页面 | 裸 Tensor 块 | **原生支持 MHA、DeepSeek MLA/DSA 及 Kimi KDA** |
| **过载保护机制** | 被动 Eviction 驱逐 | 被动 Eviction 驱逐 | 网络拥塞阻塞 | **Quota 准入跟踪与主动反压 (Backpressure)** |
| **执行稳定性** | N/A | I/O 阻塞风险 | 网络 IO 阻塞风险 | **<1ms Fail-Open 降级保障 (超时自动退回 GPU 重算)** |
| **系统架构设计** | 引擎单体内部 | 引擎子系统 | 存储底层 | **解耦 Go 控制面 + Rust 数据引擎 + Python FFI 拦截** |

---

## 🏗 系统架构设计

```text
 ┌─────────────────────────────────────────────────────────────────────────────┐
 │                      推理引擎运行时 (vLLM / SGLang Engine)                  │
 └──────────────────────────────────────┬──────────────────────────────────────┘
                                        │ Native FFI Hooks / Connector Lifecycle
                                        ▼
 ┌─────────────────────────────────────────────────────────────────────────────┐
 │                        NexusKV 智能决策层 (Intelligence Layer)              │
 │  • Cost Estimator: 有效收益评估  G = T_compute - T_cache                     │
 │  • Quota Admission Tracker: 活页内存与并发传输准入限制                      │
 │  • Prefetch Scheduler: 消费截止时间 (Deadline) 与 <1ms Fail-Open 降级       │
 └──────────────┬──────────────────────────────────────────────┬───────────────┘
                │ Key 匹配与查询                               │ Payload 存储与租约管控
                ▼                                              ▼
 ┌──────────────────────────────┐              ┌──────────────────────────────┐
 │       Rust 数据引擎          │              │        Go 分布式控制面       │
 │ • nxradixtree-core 匹配引擎  │              │ • LeaseManager 分布式租约    │
 │ • nexus-store Host DRAM 存储 │              │ • Monotonic EpochTracker     │
 │ • nexus-transfer 零拷贝句柄  │              │ • GarbageCollector 垃圾回收  │
 └──────────────────────────────┘              └──────────────────────────────┘
```

---

## 🚀 超越 KV Cache：新型 Attention 状态感知

NexusKV 为下一代 Attention 架构提供专门的状态描述符与兼容性校验：

- **MHA / GQA / MQA**：标准的连续或 Block/Page 对齐的 Key/Value 张量缓存。
- **DeepSeek MLA (Multi-Head Latent Attention)**：压缩隐向量状态 ($c_t^{KV}$) 与解耦的 RoPE 位置编码张量 ($k_t^R$)。
- **DeepSeek DSA (DeepSeek Sparse Attention)**：Query 相关的稀疏选择区域（Sparse Selection）与 Selector 辅助索引元数据。
- **Kimi KDA (Kimi Delta Attention)**：混合 Attention 模型的循环终端 Checkpoint ($h_t$)。

---

## ⚡ 快速上手

### 1. Python 快速对接示例

```python
from nexuskv.connectors.vllm.connector import VLLMConnector
from nexuskv.connectors.native_hooks import NativeEngineHookInterceptor
from nexuskv.connectors.base import VLLMLifecycleContext

# 1. 初始化 Connector 与 <1ms Fail-Open 强保障拦截器
connector = VLLMConnector()
interceptor = NativeEngineHookInterceptor(connector=connector)

# 2. 构建请求上下文
context = VLLMLifecycleContext(
    tenant="tenant_a",
    namespace="chat_production",
    model="deepseek-v3",
    tokens=[101, 2023, 2003, 1037, 3899],
    descriptor=connector.default_descriptor(),
)

# 3. 拦截请求生命周期事件
decision = interceptor.intercept_hook("request_start", context)

if decision.materialization_result.status == "completed":
    print("NexusKV 收益计算正向，通过准入！直接绑定缓存 Handle 消耗字节。")
else:
    print("Fail-Open 平滑降级：在毫秒级内自动退回到 GPU 本地 Prefill 重算。")
```

### 2. 一键运行基准测试与长压套件

使用单条命令一键运行策略收益对比评估与 7x24 内存泄漏检测：

```bash
python3 tools/run_benchmarks.py
```

---

## 🛠 构建与测试指南

### Go 控制面测试

```bash
GOTOOLCHAIN=go1.25.9 go test ./...
cd go && GOTOOLCHAIN=go1.25.9 go test ./...
```

### Rust 数据面 Workspace 检查与测试

```bash
cd rust
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
```

### Python 套件与 Native PyO3 C++ 扩展构建

```bash
# 编译构建 PyO3 Native 扩展模块
cd rust
cargo rustc -p bindings-py --crate-type cdylib
cd ..

# 运行 Python 全套单元测试 (65+ 测试用例)
PYTHONPATH=python python3 -m unittest discover -s python/tests -p "test_*.py"
```

---

## 📚 延伸阅读与架构文档

- 📄 **白皮书 (Whitepaper):** [Beyond KV Cache: Toward a Zero-Overhead Model State Intelligence Layer for LLM Inference](docs/papers/beyond-kv-cache.md)
- 🏛 **架构设计:** [NexusKV Platform Architecture](docs/design/nexuskv-architecture.md)
- 🗺 **演进 Roadmap:** [Roadmap & Milestone Status](docs/roadmap.md)
- 📝 **Migration 历史:** [PR Migration History](docs/architecture/migration-status.md)
- 📊 **基准测试契约:** [Benchmark Evaluation Methodology](docs/benchmarks/benchmark-methodology.md)

---

## 📄 开源许可证

NexusKV 采用 [MIT 许可证](LICENSE) 开源。
