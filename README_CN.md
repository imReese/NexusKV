<div align="center">

# NexusKV

### *超越传统 KV Cache：打造解耦的大模型推理状态智能层*

[![NexusKV Unified CI](https://github.com/imReese/NexusKV/actions/workflows/ci.yml/badge.svg)](https://github.com/imReese/NexusKV/actions/workflows/ci.yml)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)
[![Go Version](https://img.shields.io/badge/Go-1.23%2B-00ADD8?logo=go)](pkg/)
[![Rust Workspace](https://img.shields.io/badge/Rust-2021%20%7C%202024-000000?logo=rust)](rust/)
[![Python Suite](https://img.shields.io/badge/Python-3.11%20%7C%203.12-3776AB?logo=python)](python/)

[English](README.md) | [简体中文](README_CN.md) | [📖 快速上手与部署指南](docs/quickstart_cn.md) | [🏛 架构全景指南](docs/architecture_cn.md)

</div>

---

## 💡 什么是 NexusKV？

**NexusKV** 是专为大语言模型（LLM）推理平台设计的**引擎无关通用模型状态智能与分布式内存基础设施层（Universal Model State Intelligence & Memory Fabric）**。

NexusKV 的核心架构理念在于**三重解耦**：**引擎无关 (Engine-Agnostic)**、**模型架构无关 (Model-Agnostic)** 以及 **硬件无关 (Hardware-Agnostic)**。无论是主流开源推理引擎（如 vLLM、SGLang、TensorRT-LLM、LMDeploy、TGI）还是企业自研的 C++/Rust 推理引擎与 API 网关，均可通过统一的契约无缝接入。

随着 LLM 推理全面迈向 **Prefill-Decode (PD) 分离架构**、**滑动窗口注意力 (SWA)**、**线性/递归状态 (Recurrent State)** 以及 **低秩隐向量/稀疏 Attention**，传统将 KV Cache 视为“纯命中率驱动 (Hit-Driven)”的缓存方案暴露出了严重的瓶颈：**网络传输导致首 Token 延迟 (TTFT) 恶化、内存碎片化以及缺乏计算收益感知**。

NexusKV 通过三层解耦设计打破了这一局限：
1. **Go 分布式控制面 (`pkg/`)**：提供高可用的租约管理与拓扑路由（`LeaseManager` / `EpochTracker`）；
2. **Rust 高性能数据引擎 (`rust/`)**：基于并发前缀匹配与 Host DRAM 管理（`nxradixtree-core` / `nexus-store`）；
3. **通用接插件适配层 (`python/` & `csrc/`)**：提供无侵入的 Python Hook 与 C-FFI 拦截器。

基于 **计算收益评估方程 ($G = T_{\text{compute}} - T_{\text{cache}} > 0$)**，NexusKV 能够在微秒级内评估跨节点复用与本地重算成本，结合 **Quota 动态反压机制** 与 **<1ms Fail-Open 降级保障**，确保高并发推理下的极佳吞吐与系统韧性。

---

## ⚡ 技术对比：NexusKV vs. 传统 Cache 体系

| 架构维度 | 原生 Prefix Caching (vLLM V2 / SGLang Unified Radix) | HiCache & LMCache (多级缓存) | Mooncake Store & NIXL (传输/存储底层) | **NexusKV (模型状态智能层)** |
| :--- | :--- | :--- | :--- | :--- |
| **定位与目标** | 引擎内置缓存模块 | 引擎 Sidecar 进程 | 硬件传输与存储驱动 | **解耦的分布式控制与计算收益决策平台** |
| **缓存复用策略** | 纯命中率驱动 | 纯命中率驱动 | 存储块被动拉取 | **基于算力/带宽比的收益评估 ($G = T_{\text{compute}} - T_{\text{cache}} > 0$)** |
| **PD 分离握手** | 引擎进程间 IPC | 块级别传输 | 裸 RDMA 内存拷贝 | **`pd_disaggregate_handshake` 结合动态 Cost 调优** |
| **Attention 拓扑支持** | 仅标准 Paged KV / Mamba | 标准 MHA / Paged KV | 裸 Tensor Blobs | **原生支持 MLA ($c_t^{KV} + k_t^R$)、DSA 稀疏区及 KDA Checkpoint** |
| **过载保护能力** | 被动驱逐本地块 | 被动驱逐本地块 | 网络队列阻塞 | **`QuotaTracker` 锁页内存与并发主动反压限制** |
| **系统降级保障** | I/O 导致引擎挂起 | I/O 阻塞风险 | 传输超时挂起 | **<1ms Fail-Open 毫秒级自动降级至 GPU 重算** |
| **技术栈架构** | 绑定具体 Worker 进程 | Python 单体 Sidecar | C++ 传输驱动 | **Go 分布式控制面 + Rust 零开销引擎 + Python/C++ FFI** |

---

## 🏗 系统架构与集成拓扑

```text
 ┌─────────────────────────────────────────────────────────────────────────────┐
 │       vLLM V2 Engine (Workflow Defined Engine) / SGLang (UnifiedRadixCache)│
 └──────────────────────────────────────┬──────────────────────────────────────┘
                                        │ Native Fast FFI 拦截器 (<1ms 降级保障)
                                        ▼
 ┌─────────────────────────────────────────────────────────────────────────────┐
 │                       NexusKV 智能决策层 (Intelligence Layer)               │
 │  • DynamicCostProfiler: 实时自适应 Prefill 算力与传输带宽比                 │
 │  • Effective Gain Estimator: 算力收益评估  G = T_compute - T_cache           │
 │  • Quota Admission Tracker: 锁页内存与并发传输主动反压                      │
 │  • PD Disaggregation Handshake: Prefill 到 Decode 节点的异步状态挂载握手      │
 │  • Agentic CoW Radix Branch: ToT/MCTS 多分支写时复制零开销内存共享          │
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
                │ 物理 RDMA / NVLink / CXL3.1 内存池注册        │ 策略覆盖推发
 ┌──────────────┴──────────────────────────────────────────────┴───────────────┐
 │      物理传输驱动层 (Mooncake Transfer Engine / NVIDIA NIXL SDK / UALink2)  │
 └─────────────────────────────────────────────────────────────────────────────┘
```

---

## 🚀 统一模型状态体系：超越标准 KV Cache

NexusKV 引入了统一的 **Attention State Taxonomy（注意力状态描述符体系）**，原生适配下一代大模型架构：

1. **MHA / GQA / MQA**：标准的连续或 Block/Page 对齐 Key/Value 张量缓存。
2. **DeepSeek MLA (Multi-Head Latent Attention)**：
   - 压缩隐向量张量 ($c_t^{KV} \in \mathbb{R}^{d_{c}}$)；
   - 解耦的 RoPE 位置编码张量 ($k_t^R \in \mathbb{R}^{d_R}$)。
3. **DeepSeek DSA (DeepSeek Sparse Attention)**：
   - Query 相关的稀疏选择区域 (`dsa` backend)；
   - Selector 索引辅助元数据 ($top\_k$ 路由表)。
4. **Kimi KDA (Kimi Delta Attention)**：
   - 递归终端状态 Checkpoint ($h_t$)；
   - 混合 Attention-Recurrent 边界校验。
5. **Agentic ToT / MCTS 多分支**：
   - 基于写时复制 (Copy-on-Write) 的前缀树分支派生，共享公共 Prompt 显存。

---

## ⚡ 快速集成示例

### 1. 与 vLLM V2 及 SGLang 引擎集成

```python
from nexuskv.connectors.vllm.connector import VLLMConnector
from nexuskv.connectors.native_hooks import NativeEngineHookInterceptor
from nexuskv.connectors.base import VLLMLifecycleContext, PDDisaggregateContext

# 1. 初始化 Connector 与 <1ms Fail-Open 降级拦截器
connector = VLLMConnector()
interceptor = NativeEngineHookInterceptor(connector=connector)

# 2. 构建包含 DeepSeek MLA 描述符的请求上下文
context = VLLMLifecycleContext(
    tenant="production_tenant",
    namespace="chat_disaggregated",
    model="deepseek-v3-mla",
    tokens=[101, 2023, 2003, 1037, 3899, 5012],
    descriptor=connector.default_descriptor(),
)

# 3. 拦截请求 Lifecycle Hook 并执行收益评估
decision = interceptor.intercept_hook("request_start", context)

if decision.materialization_result.status == "completed":
    print("缓存命中：成功复用 KV Cache，跳过 GPU Prefill 重算！")
else:
    print("降级处理：未命中或传输不划算，退回本地 GPU 重新计算。")

# 4. Prefill-Decode (PD) 分离节点异步握手
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

### 2. 动态硬件 Cost Profiler 与物理 RDMA 注册

```python
from nexuskv.planner.autotune import DynamicCostProfiler
from nexuskv.execution.native_transport import MooncakeTransferEngineAdapter
from nexuskv.contracts.generated import TierKind

# 实时采样 GPU Prefill 吞吐与网络传输带宽
profiler = DynamicCostProfiler()
profiler.record_prefill_sample(token_count=1000, duration_sec=0.001)  # 1us / token
profiler.record_bandwidth_sample(TierKind.HOST_DRAM, payload_bytes=1000000, duration_sec=0.0001)

# 注册物理 RDMA 内存池 (支持 Mooncake Transfer Engine 与 NIXL SDK)
mooncake = MooncakeTransferEngineAdapter()
reg = mooncake.register_rdma_pool(pool_id="pool_01", base_addr=0x7FFF0000, size_bytes=1048576)
print(f"已成功注册物理 RDMA 内存池 Handle: {reg.handle_id}, 状态: {reg.is_registered}")
```

---

## 🛠 构建与测试

### 运行全量单元测试与基准套件

```bash
# 运行分布式 Go 控制面单元测试
go test ./...

# 使用标准 Makefile 一键构建与测试
make build   # 编译 Go 控制面与 Rust 动态扩展
make test    # 顺序运行 Go, Rust, Python 全量 89+ 项单元测试
make bench   # 运行性能 Benchmark 评估看板
```

---

## 📚 文档矩阵 (Documentation Matrix)

| 分类 | 📖 中文文档 | 🌐 英文 Specification | 核心主题 |
| :--- | :--- | :--- | :--- |
| **快速上手** | [开箱即用与部署指南](docs/quickstart_cn.md) | [Quickstart Guide](docs/quickstart.md) | 环境准备、3大部署形态与 `make` 命令 |
| **架构设计** | [核心架构与多后端全景](docs/architecture_cn.md) | [Platform Architecture](docs/design/nexuskv-architecture.md) | 三层解耦设计与异构硬件支持矩阵 |
| **路线规划** | [路线图与阶段规划](docs/roadmap_cn.md) | [Roadmap & Milestones](docs/roadmap.md) | 研发路线图与功能落地进展 |
| **状态契约** | [Attention 状态描述符](docs/design/attention-state-descriptor_cn.md) | [Attention Descriptor Spec](docs/design/attention-state-descriptor.md) | MLA / DSA / CSA / HCA 描述符结构 |
| **接插件周期**| [引擎接插件生命周期](docs/design/connector-lifecycle_cn.md) | [Connector Lifecycle](docs/design/connector-lifecycle.md) | vLLM / SGLang 挂载与 PD 分离握手 |
| **控制面策略** | [控制面执行策略](docs/design/controlplane-execution-policy_cn.md) | [Controlplane Policy](docs/design/controlplane-execution-policy.md) | Lease 租约、Epoch 纪元与 Quota 反压 |
| **基准测试** | [基准测试方法论](docs/benchmarks/benchmark-methodology_cn.md) | [Benchmark Methodology](docs/benchmarks/benchmark-methodology.md) | 微秒级打点与 QPS / GB 双维度测试 |
| **可靠性模型** | [系统可靠性与降级熔断](docs/ops/reliability-model_cn.md) | [Reliability Model](docs/ops/reliability-model.md) | <1ms 强保障 Fail-Open 平滑降级 |
| **技术剖析** | [零开销状态智能层剖析](docs/blog/zero-overhead-kv-cache-runtime_cn.md) | [Zero-Overhead Runtime](docs/blog/zero-overhead-kv-cache-runtime.md) | 算力收益评估方程 $G = T_{\text{compute}} - T_{\text{cache}}$ |
| **论文白皮书**| [Beyond KV Cache 论文中文版](docs/papers/beyond-kv-cache_cn.md) | [Beyond KV Cache Paper](docs/papers/beyond-kv-cache.md) | 系统技术架构白皮书 |

---

## 📄 开源许可证

NexusKV 采用 [Apache License 2.0](LICENSE) 许可证开源。
