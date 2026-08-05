<div align="center">

# NexusKV

### *Beyond KV Cache: Toward a Zero-Overhead Model State Intelligence Layer for LLM Inference*

[![NexusKV Unified CI](https://github.com/imReese/NexusKV/actions/workflows/ci.yml/badge.svg)](https://github.com/imReese/NexusKV/actions/workflows/ci.yml)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)
[![Go Version](https://img.shields.io/badge/Go-1.23%2B-00ADD8?logo=go)](go/)
[![Rust Workspace](https://img.shields.io/badge/Rust-2021%20%7C%202024-000000?logo=rust)](rust/)
[![Python Suite](https://img.shields.io/badge/Python-3.11%20%7C%203.12-3776AB?logo=python)](python/)

[English](README.md) | [简体中文](README_CN.md) | [📖 Quickstart Guide](docs/quickstart.md) | [🏛 Chinese Quickstart Guide](docs/quickstart_cn.md)

</div>

---

## 💡 What is NexusKV?

**NexusKV** is a next-generation **Model State Intelligence Layer** designed for cutting-edge LLM inference platforms (supporting **vLLM V2 Engine (Model Runner V2 / Workflow Defined Engine)** and **SGLang (Unified Radix Cache / HiCache)**).

As LLM inference scales into **Prefill-Decode (PD) Disaggregation**, **Sliding Window Attention (SWA)**, **Mamba State Offloading**, and **DeepSeek MLA/DSA Architectures**, traditional KV cache management relies on local, hit-driven block eviction. This results in **network transfer stalls, TTFT regressions, and memory fragmentation**.

NexusKV addresses this by decoupling the **Go Distributed Control Plane (LeaseManager / EpochTracker)**, **Rust Data & Radix Matching Engine (`nxradixtree-core` / `nexus-store`)**, and **Python Engine FFI Interceptors (`NativeEngineHookInterceptor`)**. It provides an intelligent decision engine that calculates **Cost-Based Effective Gain** ($G = T_{compute} - T_{cache} > 0$), enforces **Quota Backpressure**, and guarantees a **Sub-millisecond Fail-Open Fallback (<1ms)** to local GPU prefill computation.

---

## ⚡ Technical Comparison: NexusKV vs. Latest Native Engine Caches

| Architecture Dimension | Native Prefix Caching (vLLM V2 / SGLang Unified Radix) | HiCache & LMCache (Multi-Tier Cache) | Mooncake Store & NIXL (Raw Transfer / Storage) | **NexusKV (Model State Intelligence Layer)** |
| :--- | :--- | :--- | :--- | :--- |
| **Engine Target** | vLLM V2 MRV2 / SGLang UnifiedRadix | Engine Multiprocess Sidecar | RDMA / NVLink Driver | **Decoupled Control & Intelligence Platform** |
| **Cache Reuse Strategy** | Local Hit-Driven | Local Hit-Driven | Storage Block Pull | **Effective Gain Equation ($G = T_{compute} - T_{cache} > 0$)** |
| **PD Disaggregation Handshake** | Engine-level IPC Handshake | Block-Based Transfer | Raw RDMA Memory Copy | **`pd_disaggregate_handshake` with Dynamic Cost Profiling** |
| **Model State Taxonomy** | Paged KV / Mamba States | Standard MHA / Paged KV | Raw Tensor Blobs | **Native Support for MHA, DeepSeek MLA, DSA & Kimi KDA** |
| **Overloaded System Behavior** | Evicts Local Blocks | Evicts Local Blocks | Network Queue Stalls | **Quota Admission Tracker & Active Memory Backpressure** |
| **Execution Resilience** | Risk of I/O Stalls | Risk of I/O Stalls | Risk of Transport Hangs | **Sub-millisecond Fail-Open Guarantee (<1ms Fallback to Prefill)** |
| **Decoupled Stack** | Embedded in Worker | Python Sidecar | C++ Transport Driver | **Go Control Plane + Rust Radix Engine + Python FFI** |

---

## 🏗 System Architecture & Integration Workflow

```text
 ┌─────────────────────────────────────────────────────────────────────────────┐
 │       vLLM V2 Engine (Workflow Defined Engine) / SGLang (UnifiedRadixCache)│
 └──────────────────────────────────────┬──────────────────────────────────────┘
                                        │ Native Fast FFI Interceptor (<1ms Guarantee)
                                        ▼
 ┌─────────────────────────────────────────────────────────────────────────────┐
 │                         NexusKV Intelligence Layer                          │
 │  • DynamicCostProfiler: Live Auto-Tuning of GPU Prefill & Transport Bandwidth│
 │  • Effective Gain Estimator:  G = T_compute - T_cache                      │
 │  • Quota Admission Tracker: Active Pinned Memory & Concurrency Limits       │
 │  • PD Disaggregation Handshake: Prefill-to-Decode Asynchronous Mounting     │
 └──────────────┬──────────────────────────────────────────────┬───────────────┘
                │ Key & Radix Prefix Matching                  │ Payload & Lease Management
                ▼                                              ▼
 ┌──────────────────────────────┐              ┌──────────────────────────────┐
 │     Rust Data Engine         │              │      Go Control Plane        │
 │ • nxradixtree-core Matcher   │              │ • Distributed LeaseManager   │
 │ • nexus-store Host DRAM      │              │ • Monotonic EpochTracker     │
 │ • nexus-transfer Zero-Copy   │              │ • GarbageCollector & Policy  │
 └──────────────────────────────┘              └──────────────────────────────┘
                ▲                                              ▲
                │ Direct RDMA & NVLink Pool Registration       │ Policy Overlays
 ┌──────────────┴──────────────────────────────────────────────┴───────────────┐
 │        Physical Transport Drivers (Mooncake Transfer Engine / NIXL SDK)    │
 └─────────────────────────────────────────────────────────────────────────────┘
```

---

## 🚀 Model State Taxonomy: Beyond Standard KV Cache

NexusKV introduces a unified **Attention State Taxonomy** designed to parse the physical and mathematical structure of modern attention mechanisms:

1. **MHA / GQA / MQA**: Standard contiguous or page-aligned key/value tensor caches.
2. **DeepSeek MLA (Multi-Head Latent Attention)**:
   - Compressed Latent KV Tensors ($c_t^{KV} \in \mathbb{R}^{d_{c}}$).
   - Decoupled Positional RoPE Tensors ($k_t^R \in \mathbb{R}^{d_R}$).
3. **DeepSeek DSA (DeepSeek Sparse Attention)**:
   - Query-dependent sparse selection regions (`dsa` backend).
   - Selector index auxiliary metadata ($top\_k$ routing tables).
4. **Kimi KDA (Kimi Delta Attention)**:
   - Recurrent terminal state checkpoints ($h_t$).
   - Hybrid attention-recurrent boundary validation.

---

## ⚡ Integration Examples

### 1. Integration with vLLM V2 Engine & SGLang Unified Radix Cache

```python
from nexuskv.connectors.vllm.connector import VLLMConnector
from nexuskv.connectors.native_hooks import NativeEngineHookInterceptor
from nexuskv.connectors.base import VLLMLifecycleContext, PDDisaggregateContext

# 1. Initialize vLLM V2 / SGLang Connector & Native Hook Interceptor (<1ms Guarantee)
connector = VLLMConnector()
interceptor = NativeEngineHookInterceptor(connector=connector)

# 2. Construct Request Context with DeepSeek MLA Descriptor
context = VLLMLifecycleContext(
    tenant="production_tenant",
    namespace="chat_disaggregated",
    model="deepseek-v3-mla",
    tokens=[101, 2023, 2003, 1037, 3899, 5012],
    descriptor=connector.default_descriptor(),
)

# 3. Intercept Request Start Hook
decision = interceptor.intercept_hook("request_start", context)

if decision.materialization_result.status == "completed":
    print("NexusKV Effective Gain > 0! Binding cached MLA latent handle.")
else:
    print("Fail-Open Fallback: Executing GPU prefill recomputation locally.")

# 4. Prefill-Decode (PD) Disaggregation Handshake
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

### 2. Auto-Tuning Dynamic Cost Profiler & RDMA Driver Registration

```python
from nexuskv.planner.autotune import DynamicCostProfiler
from nexuskv.execution.native_transport import MooncakeTransferEngineAdapter, NIXLDriverAdapter
from nexuskv.contracts.generated import TierKind

# Live Auto-Tuning of Network Bandwidth & GPU Token Processing Speed
profiler = DynamicCostProfiler()
profiler.record_prefill_sample(token_count=1000, duration_sec=0.001)  # 1us / token
profiler.record_bandwidth_sample(TierKind.HOST_DRAM, payload_bytes=1000000, duration_sec=0.0001)

# Register Physical RDMA Memory Pools (Mooncake Transfer Engine / NIXL)
mooncake = MooncakeTransferEngineAdapter()
reg = mooncake.register_rdma_pool(pool_id="pool_01", base_addr=0x7FFF0000, size_bytes=1048576)
print(f"Registered RDMA Pool Handle: {reg.handle_id}, Active: {reg.is_registered}")
```

### 3. Run Synthetic Benchmarks & Cluster Stress Suite

```bash
python3 tools/run_benchmarks.py
```

---

## 🛠 Toolchain & Verification

### 1. Go Control Plane Tests

```bash
GOTOOLCHAIN=go1.25.9 go test ./...
cd go && GOTOOLCHAIN=go1.25.9 go test ./...
```

### 2. Standard Makefile One-Command Build

```bash
make build   # Full build of Go controlplane and Rust native PyO3 extension
make wheel   # Package native Python .whl wheel file (dist/*.whl)
make test    # Run Go, Rust, and Python 73+ unit test suite
make bench   # Run dual-dimension and multi-size payload benchmark matrix
```

---

## 📚 1-to-1 English & Chinese Documentation Sitemap Matrix

For global developers and teams, NexusKV provides a **100% 1-to-1 paired English and Chinese documentation matrix**:

| Category | 🌐 English Specification | 📖 Chinese Guide (中文指南) | Key Topics |
| :--- | :--- | :--- | :--- |
| **Quickstart** | [Quickstart Guide](docs/quickstart.md) | [开箱即用与部署指南](docs/quickstart_cn.md) | Prerequisites, 3 deployment topologies & `make` commands |
| **Architecture** | [Platform Architecture](docs/design/nexuskv-architecture.md) | [核心架构与多后端全景](docs/architecture_cn.md) | 3-layer decoupled design & multi-hardware matrix |
| **Roadmap** | [Roadmap & Milestones](docs/roadmap.md) | [路线图与阶段规划](docs/roadmap_cn.md) | Phase 1 - Phase 8 progress & development gates |
| **State Contract** | [Attention Descriptor Spec](docs/design/attention-state-descriptor.md) | [Attention 状态描述符](docs/design/attention-state-descriptor_cn.md) | MLA / DSA / CSA / HCA state descriptors |
| **Connector** | [Connector Lifecycle](docs/design/connector-lifecycle.md) | [引擎接插件生命周期](docs/design/connector-lifecycle_cn.md) | vLLM / SGLang hooks & PD disaggregation |
| **Control Policy** | [Controlplane Policy](docs/design/controlplane-execution-policy.md) | [控制面执行策略](docs/design/controlplane-execution-policy_cn.md) | Leases, monotonic epochs & quota backpressure |
| **Benchmarking** | [Benchmark Methodology](docs/benchmarks/benchmark-methodology.md) | [基准测试方法论](docs/benchmarks/benchmark-methodology_cn.md) | Microsecond Wall-Clock timing & QPS / GB dual metrics |
| **Reliability** | [Reliability Model](docs/ops/reliability-model.md) | [系统可靠性与降级熔断](docs/ops/reliability-model_cn.md) | <1ms Fail-Open fallback guarantees |
| **Tech Blog** | [Zero-Overhead Runtime](docs/blog/zero-overhead-kv-cache-runtime.md) | [零开销状态智能层剖析](docs/blog/zero-overhead-kv-cache-runtime_cn.md) | Cost equation $G = T_{compute} - T_{cache}$ |
| **Whitepaper** | [Beyond KV Cache Paper](docs/papers/beyond-kv-cache.md) | [Beyond KV Cache 论文中文版](docs/papers/beyond-kv-cache_cn.md) | Architectural whitepaper |

---

## 📄 License

NexusKV is licensed under the [Apache License 2.0](LICENSE).
