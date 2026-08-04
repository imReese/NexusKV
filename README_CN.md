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

**NexusKV** 是专为前沿大语言模型推理平台（原生支持 **vLLM V2 引擎 (Model Runner V2 / Workflow Defined Engine)** 与 **SGLang (Unified Radix Cache / HiCache)**）设计的下一代**模型状态智能层（Model State Intelligence Layer）**。

随着 LLM 推理全面迈向 **Prefill-Decode (PD) 分离架构**、**Sliding Window Attention (SWA)**、**Mamba 状态卸载** 以及 **DeepSeek MLA / DSA 新型 Attention**，传统将 KV Cache 视为盲目命中驱动（Hit-Driven）的存储方案暴露出了严重的缺陷：**网络传输导致 TTFT 恶化、内存碎片化以及缺乏物理算力感知**。

NexusKV 通过解耦 **Go 分布式控制面 (LeaseManager / EpochTracker)**、**Rust 数据与 Radix 匹配引擎 (`nxradixtree-core` / `nexus-store`)** 以及 **Python 引擎 FFI 拦截器 (`NativeEngineHookInterceptor`)** 解决了这一难题。它提供了基于 **有效收益评估方程 ($G = T_{compute} - T_{cache} > 0$)** 的智能决策引擎，支持 **Quota 主动反压**，并提供 **<1ms 极速 Fail-Open 平滑降级保障**。

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

## 🏗 系统架构与集成拓扑

```text
 ┌─────────────────────────────────────────────────────────────────────────────┐
 │       vLLM V2 Engine (Workflow Defined Engine) / SGLang (UnifiedRadixCache)│
 └──────────────────────────────────────┬──────────────────────────────────────┘
                                        │ Native Fast FFI Interceptor (<1ms 保障)
                                        ▼
 ┌─────────────────────────────────────────────────────────────────────────────┐
 │                       NexusKV 智能决策层 (Intelligence Layer)               │
 │  • DynamicCostProfiler: GPU Prefill 算力与网络带宽实时自适应调优              │
 │  • Effective Gain Estimator: 有效收益评估  G = T_compute - T_cache           │
 │  • Quota Admission Tracker: 锁页内存与并发传输主动反压限制                    │
 │  • PD Disaggregation Handshake: Prefill 到 Decode 节点的异步状态挂载握手      │
 └──────────────┬──────────────────────────────────────────────┬───────────────┘
                │ Key 匹配与 Radix 前缀树查询                  │ Payload 存储与租约管控
                ▼                                              ▼
 ┌──────────────────────────────┐              ┌──────────────────────────────┐
 │       Rust 数据引擎          │              │        Go 分布式控制面       │
 │ • nxradixtree-core 匹配引擎  │              │ • LeaseManager 分布式租约    │
 │ • nexus-store Host DRAM 存储 │              │ • Monotonic EpochTracker     │
 │ • nexus-transfer 零拷贝句柄  │              │ • GarbageCollector 垃圾回收  │
 └──────────────────────────────┘              └──────────────────────────────┘
                ▲                                              ▲
                │ 物理 RDMA & NVLink 内存池注册                 │ 策略覆盖推发
 ┌──────────────┴──────────────────────────────────────────────┴───────────────┐
 │      物理传输驱动层 (Mooncake Transfer Engine / NVIDIA NIXL SDK)            │
 └─────────────────────────────────────────────────────────────────────────────┘
```

---

## 🚀 模型状态体系：超越标准 KV Cache

NexusKV 引入了统一的 **Attention State Taxonomy（状态描述符体系）**，理解下一代 Attention 架构的物理与数学结构：

1. **MHA / GQA / MQA**：标准的连续或 Block/Page 对齐 Key/Value 张量缓存。
2. **DeepSeek MLA (Multi-Head Latent Attention)**：
   - 压缩隐向量张量 ($c_t^{KV} \in \mathbb{R}^{d_{c}}$)；
   - 解耦的 RoPE 位置编码张量 ($k_t^R \in \mathbb{R}^{d_R}$)。
3. **DeepSeek DSA (DeepSeek Sparse Attention)**：
   - Query 相关的稀疏选择区域 (`dsa` backend)；
   - Selector 索引辅助元数据 ($top\_k$ 路由表)。
4. **Kimi KDA (Kimi Delta Attention)**：
   - Recurrent 终端状态 Checkpoint ($h_t$)；
   - 混合 Attention-Recurrent 边界校验。

---

## ⚡ 集成与代码示例

### 1. 与 vLLM V2 引擎及 SGLang Unified Radix Cache 的集成

```python
from nexuskv.connectors.vllm.connector import VLLMConnector
from nexuskv.connectors.native_hooks import NativeEngineHookInterceptor
from nexuskv.connectors.base import VLLMLifecycleContext, PDDisaggregateContext

# 1. 初始化 vLLM V2 / SGLang Connector 与 <1ms Fail-Open 强保障拦截器
connector = VLLMConnector()
interceptor = NativeEngineHookInterceptor(connector=connector)

# 2. 构建携带 DeepSeek MLA 描述符的请求上下文
context = VLLMLifecycleContext(
    tenant="production_tenant",
    namespace="chat_disaggregated",
    model="deepseek-v3-mla",
    tokens=[101, 2023, 2003, 1037, 3899, 5012],
    descriptor=connector.default_descriptor(),
)

# 3. 拦截请求 Lifecycle Hook
decision = interceptor.intercept_hook("request_start", context)

if decision.materialization_result.status == "completed":
    print("NexusKV 有效收益 G > 0！绑定缓存的 MLA 隐向量 Handle 消耗。")
else:
    print("Fail-Open 平滑降级：在毫秒级内自动退回到 GPU 本地 Prefill 重算。")

# 4. Prefill-Decode (PD) 分离握手
pd_context = PDDisaggregateContext(
    tenant="production_tenant",
    namespace="chat_disaggregated",
    model="deepseek-v3-mla",
    tokens=context.tokens,
    descriptor=context.descriptor,
    prefill_worker_id="prefill-gpu-node-01",
    decode_worker_id="decode-gpu-node-04",
)
pd_decision = connector.on_pd_disaggregate_handshake(pd_context, interceptor.planner)
```

### 2. 动态硬件 Cost 调优器与 RDMA 驱动注册

```python
from nexuskv.planner.autotune import DynamicCostProfiler
from nexuskv.execution.native_transport import MooncakeTransferEngineAdapter, NIXLDriverAdapter
from nexuskv.contracts.generated import TierKind

# 网络带宽与 GPU Prefill token 处理速度的实时自适应调优
profiler = DynamicCostProfiler()
profiler.record_prefill_sample(token_count=1000, duration_sec=0.001)  # 1us / token
profiler.record_bandwidth_sample(TierKind.HOST_DRAM, payload_bytes=1000000, duration_sec=0.0001)

# 注册物理 RDMA 内存池 (Mooncake Transfer Engine / NIXL)
mooncake = MooncakeTransferEngineAdapter()
reg = mooncake.register_rdma_pool(pool_id="pool_01", base_addr=0x7FFF0000, size_bytes=1048576)
print(f"已注册物理 RDMA 内存池 Handle: {reg.handle_id}, 状态: {reg.is_registered}")
```

### 3. 一键运行基准测试与长压套件

```bash
python3 tools/run_benchmarks.py
```

---

## 🛠 构建与测试指南

### 1. Go 控制面测试

```bash
GOTOOLCHAIN=go1.25.9 go test ./...
cd go && GOTOOLCHAIN=go1.25.9 go test ./...
```

### 2. Rust 数据引擎 Workspace

```bash
cd rust
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
```

### 3. Python 套件 (68+ 测试用例) 与 PyO3 Native 扩展构建

```bash
# 编译构建 PyO3 Native 扩展
cd rust
cargo rustc -p bindings-py --crate-type cdylib
cd ..

# 运行 Python 全套单元测试 (68+ 测试用例)
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
