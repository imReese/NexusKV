<div align="center">

# NexusKV

### *Beyond KV Cache: Decoupled Model State Intelligence Layer for LLM Inference Engines*

[![NexusKV Unified CI](https://github.com/imReese/NexusKV/actions/workflows/ci.yml/badge.svg)](https://github.com/imReese/NexusKV/actions/workflows/ci.yml)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)
[![Go Version](https://img.shields.io/badge/Go-1.23%2B-00ADD8?logo=go)](pkg/)
[![Rust Workspace](https://img.shields.io/badge/Rust-2021%20%7C%202024-000000?logo=rust)](rust/)
[![Python Suite](https://img.shields.io/badge/Python-3.11%20%7C%203.12-3776AB?logo=python)](python/)

[English](README.md) | [简体中文](README_CN.md) | [📖 Quickstart Guide](docs/quickstart.md) | [🏛 Architecture Guide](docs/design/nexuskv-architecture.md)

</div>

---

## 💡 What is NexusKV?

**NexusKV** is an engine-agnostic **Universal Model State Intelligence Layer & Distributed Memory Fabric** engineered for enterprise LLM inference platforms (supporting vLLM V2 Engine, SGLang Unified Radix Cache, TensorRT-LLM, LMDeploy, TGI, and custom C++/Rust inference runtimes).

NexusKV operates on three foundational decoupling principles: **Engine-Agnostic**, **Model-Agnostic**, and **Hardware-Agnostic**. 

As modern LLM serving transitions toward **Prefill-Decode (PD) Disaggregation**, **Sliding Window Attention (SWA)**, **Recurrent State Offloading**, and **Low-Rank Latent / Sparse Attention**, traditional KV cache management—which relies on naive, hit-driven local block eviction—faces bottleneck challenges: **network transfer overhead degrading TTFT, memory fragmentation, and a lack of compute cost awareness**.

NexusKV solves these limitations via a three-layer decoupled architecture:
1. **Go Distributed Control Plane (`pkg/`)**: High-availability lease management and topology routing (`LeaseManager` / `EpochTracker`);
2. **Rust High-Performance Data Engine (`rust/`)**: Concurrent prefix matching and Host DRAM management (`nxradixtree-core` / `nexus-store`);
3. **Universal Engine Adapters (`python/` & `csrc/`)**: Zero-overhead Python hooks and Header-Only C-FFI interceptors.

Using an **Effective Gain Estimator ($G = T_{\text{compute}} - T_{\text{cache}} > 0$)**, NexusKV evaluates cross-node state reuse versus local prefill compute cost in microseconds. Coupled with **Quota Backpressure** and a **Sub-millisecond Fail-Open Fallback (<1ms)**, NexusKV ensures maximum throughput and resilience under high-concurrency workloads.

---

## ⚡ Technical Comparison: NexusKV vs. Native Engine Caches

| Feature Dimension | Native Prefix Caching (vLLM V2 / SGLang) | HiCache & LMCache (Sidecar Cache) | Mooncake Store & NIXL (Raw Transport) | **NexusKV (Model State Intelligence Layer)** |
| :--- | :--- | :--- | :--- | :--- |
| **System Scope** | Worker-embedded cache module | Multiprocess Python Sidecar | Hardware transfer/storage drivers | **Decoupled Control Plane & Cost-Based Intelligence Platform** |
| **Reuse Strategy** | Pure local hit ratio | Pure local hit ratio | Passive block pull | **Effective Gain Evaluation ($G = T_{\text{compute}} - T_{\text{cache}} > 0$)** |
| **PD Disaggregation** | In-process IPC | Block-based transfer | Raw RDMA memory copy | **`pd_disaggregate_handshake` with Dynamic Cost Profiling** |
| **Attention Support** | Standard Paged KV / Mamba | Standard MHA / Paged KV | Raw Tensor Blobs | **Native Support for MLA ($c_t^{KV} + k_t^R$), DSA Sparse & KDA Checkpoints** |
| **Overload Control** | Passive block eviction | Passive block eviction | Network queue stalls | **`QuotaTracker` Active Pinned Memory & Concurrency Backpressure** |
| **Resilience Guarantee** | Risk of I/O stalls | Risk of I/O stalls | Risk of transport hangs | **Sub-millisecond Fail-Open (<1ms Fallback to GPU Prefill)** |
| **Tech Stack** | Tied to Worker process | Python Sidecar | C++ Transport Driver | **Go Control Plane + Rust Engine + Python/C++ FFI** |

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
 │  • DynamicCostProfiler: Live Auto-Tuning of GPU Prefill & Bandwidth Ratio  │
 │  • Effective Gain Estimator:  G = T_compute - T_cache                      │
 │  • Quota Admission Tracker: Active Pinned Memory & Concurrency Limits       │
 │  • PD Disaggregation Handshake: Prefill-to-Decode Asynchronous Mounting     │
 │  • Agentic CoW Radix Branch: ToT/MCTS Zero-Overhead Memory Sharing          │
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
                │ Direct RDMA / NVLink / CXL 3.1 Registration  │ Policy Overlays
 ┌──────────────┴──────────────────────────────────────────────┴───────────────┐
 │    Physical Transport Drivers (Mooncake Transfer Engine / NIXL SDK / UALink2)│
 └─────────────────────────────────────────────────────────────────────────────┘
```

---

## 🚀 Model State Taxonomy: Beyond Standard KV Cache

NexusKV introduces a unified **Attention State Taxonomy** designed to parse the physical and mathematical structure of modern attention mechanisms:

1. **MHA / GQA / MQA**: Standard contiguous or page-aligned Key/Value tensor caches.
2. **DeepSeek MLA (Multi-Head Latent Attention)**:
   - Compressed Latent KV Tensors ($c_t^{KV} \in \mathbb{R}^{d_{c}}$).
   - Decoupled Positional RoPE Tensors ($k_t^R \in \mathbb{R}^{d_R}$).
3. **DeepSeek DSA (DeepSeek Sparse Attention)**:
   - Query-dependent sparse selection regions (`dsa` backend).
   - Selector index auxiliary metadata ($top\_k$ routing tables).
4. **Kimi KDA (Kimi Delta Attention)**:
   - Recurrent terminal state checkpoints ($h_t$).
   - Hybrid attention-recurrent boundary validation.
5. **Agentic ToT / MCTS Multi-Branching**:
   - Copy-on-Write Radix tree branching for zero-overhead prompt prefix sharing.

---

## ⚡ Integration Examples

### 1. Integration with vLLM V2 Engine & SGLang Unified Radix Cache

```python
from nexuskv.connectors.vllm.connector import VLLMConnector
from nexuskv.connectors.native_hooks import NativeEngineHookInterceptor
from nexuskv.connectors.base import VLLMLifecycleContext, PDDisaggregateContext

# 1. Initialize Connector & Interceptor with <1ms Guarantee
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

# 3. Intercept Lifecycle Hook and Evaluate Effective Gain
decision = interceptor.intercept_hook("request_start", context)

if decision.materialization_result.status == "completed":
    print("Cache Hit: Successfully reused KV Cache, skipping GPU Prefill!")
else:
    print("Fallback: Cache miss or low gain, recomputing on local GPU.")

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

### 2. Live Dynamic Cost Profiler & RDMA Driver Registration

```python
from nexuskv.planner.autotune import DynamicCostProfiler
from nexuskv.execution.native_transport import MooncakeTransferEngineAdapter
from nexuskv.contracts.generated import TierKind

# Profile live GPU prefill throughput and network transfer bandwidth
profiler = DynamicCostProfiler()
profiler.record_prefill_sample(token_count=1000, duration_sec=0.001)  # 1us / token
profiler.record_bandwidth_sample(TierKind.HOST_DRAM, payload_bytes=1000000, duration_sec=0.0001)

# Register physical RDMA memory pool (Mooncake Transfer Engine / NIXL SDK)
mooncake = MooncakeTransferEngineAdapter()
reg = mooncake.register_rdma_pool(pool_id="pool_01", base_addr=0x7FFF0000, size_bytes=1048576)
print(f"Registered RDMA Pool Handle: {reg.handle_id}, Active: {reg.is_registered}")
```

---

## 🛠 Toolchain & Verification

### Running Full Test & Benchmark Suite

```bash
# Run Go distributed control plane tests
go test ./...

# Standard Makefile commands
make build   # Build Go control plane & Rust dynamic PyO3 extensions
make test    # Execute Go, Rust, and Python unit test suites (89+ tests)
make bench   # Run performance benchmark suite
```

---

## 📚 Documentation Sitemap Matrix

| Category | 🌐 English Specification | 📖 Chinese Guide (中文指南) | Key Topics |
| :--- | :--- | :--- | :--- |
| **Quickstart** | [Quickstart Guide](docs/quickstart.md) | [开箱即用与部署指南](docs/quickstart_cn.md) | Prerequisites, 3 deployment topologies & `make` commands |
| **Architecture** | [Platform Architecture](docs/design/nexuskv-architecture.md) | [核心架构与多后端全景](docs/architecture_cn.md) | 3-layer decoupled design & multi-hardware matrix |
| **Roadmap** | [Roadmap & Milestones](docs/roadmap.md) | [路线图与阶段规划](docs/roadmap_cn.md) | Roadmap milestones & development progress |
| **State Contract** | [Attention Descriptor Spec](docs/design/attention-state-descriptor.md) | [Attention 状态描述符](docs/design/attention-state-descriptor_cn.md) | MLA / DSA / CSA / HCA state descriptors |
| **Connector** | [Connector Lifecycle](docs/design/connector-lifecycle.md) | [引擎接插件生命周期](docs/design/connector-lifecycle_cn.md) | vLLM / SGLang hooks & PD disaggregation |
| **Control Policy** | [Controlplane Policy](docs/design/controlplane-execution-policy.md) | [控制面执行策略](docs/design/controlplane-execution-policy_cn.md) | Leases, monotonic epochs & quota backpressure |
| **Benchmarking** | [Benchmark Methodology](docs/benchmarks/benchmark-methodology.md) | [基准测试方法论](docs/benchmarks/benchmark-methodology_cn.md) | Microsecond Wall-Clock timing & QPS / GB dual metrics |
| **Reliability** | [Reliability Model](docs/ops/reliability-model.md) | [系统可靠性与降级熔断](docs/ops/reliability-model_cn.md) | <1ms Fail-Open fallback guarantees |
| **Tech Blog** | [Zero-Overhead Runtime](docs/blog/zero-overhead-kv-cache-runtime.md) | [零开销状态智能层剖析](docs/blog/zero-overhead-kv-cache-runtime_cn.md) | Cost equation $G = T_{\text{compute}} - T_{\text{cache}}$ |
| **Whitepaper** | [Beyond KV Cache Paper](docs/papers/beyond-kv-cache.md) | [Beyond KV Cache 论文中文版](docs/papers/beyond-kv-cache_cn.md) | Technical whitepaper |

---

## 📄 License

NexusKV is licensed under the [Apache License 2.0](LICENSE).
