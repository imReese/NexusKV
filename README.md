<div align="center">

# NexusKV

### *Beyond KV Cache: Toward a Zero-Overhead Model State Intelligence Layer for LLM Inference*

[![NexusKV Unified CI](https://github.com/imReese/NexusKV/actions/workflows/ci.yml/badge.svg)](https://github.com/imReese/NexusKV/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Go Version](https://img.shields.io/badge/Go-1.23%2B-00ADD8?logo=go)](go/)
[![Rust Workspace](https://img.shields.io/badge/Rust-2021-000000?logo=rust)](rust/)
[![Python Suite](https://img.shields.io/badge/Python-3.11%20%7C%203.12-3776AB?logo=python)](python/)

[English](README.md) | [简体中文](README_CN.md)

</div>

---

## 💡 What is NexusKV?

**NexusKV** is a next-generation **Model State Intelligence Layer** designed for LLM inference. Rather than treating the KV cache as a monolithic, hit-driven storage service, NexusKV introduces a **cost-based, zero-overhead architecture** that decouples the Control Plane (Go), Data Plane (Rust), and Inference Engine Adapters (Python).

NexusKV goes **beyond standard KV cache** by unifying model state descriptors for modern attention architectures—including **DeepSeek MLA** (Multi-head Latent Attention), **DeepSeek DSA** (DeepSeek Sparse Attention), and **Kimi KDA** (Kimi Delta Attention Recurrent Checkpoints).

---

## ⚡ Why NexusKV? (Feature Comparison)

| Feature / Dimension | Native Engine Cache (vLLM/SGLang) | Multi-Tier Cache (HiCache/LMCache) | Shared Storage (3FS/Mooncake) | **NexusKV (Model State Intelligence Layer)** |
| :--- | :--- | :--- | :--- | :--- |
| **Cache Scope** | Local Worker / HBM Only | Local Multi-Tier (HBM/DRAM/Disk) | Distributed Storage Store | **Global Cross-Worker Model State Intelligence** |
| **Reuse Decision** | Naive Hit-Driven | Naive Hit-Driven | Naive Storage Fetch | **Cost-Based Effective Gain ($G = T_{compute} - T_{cache} > 0$)** |
| **Attention Support** | Standard MHA/GQA Pages | Standard MHA/GQA Pages | Raw Tensor Blobs | **Native Support for MHA, DeepSeek MLA, DSA & Kimi KDA** |
| **Overload Protection** | 被动 Eviction | 被动 Eviction | Network Congestion | **Quota Admission Tracker & Quota Backpressure** |
| **Execution Safety** | N/A | Risk of I/O Stalls | Risk of Network Stalls | **Sub-millisecond Fail-Open Guarantee (<1ms Fallback to Prefill)** |
| **Architecture** | Engine Monolith | Engine Subsystem | Storage System | **Decoupled Go Control Plane + Rust Data Engine + Python FFI** |

---

## 🏗 System Architecture

```text
 ┌─────────────────────────────────────────────────────────────────────────────┐
 │                       Inference Engine Runtime (vLLM / SGLang)              │
 └──────────────────────────────────────┬──────────────────────────────────────┘
                                        │ Native FFI Hooks / Connector Surface
                                        ▼
 ┌─────────────────────────────────────────────────────────────────────────────┐
 │                         NexusKV Intelligence Layer                          │
 │  • Cost Estimator: Effective Gain  G = T_compute - T_cache                  │
 │  • Quota Admission Tracker: Active Pinned Memory & Transfer Limits          │
 │  • Prefetch Scheduler: Deadline Expiration & Sub-1ms Fail-Open Fallback     │
 └──────────────┬──────────────────────────────────────────────┬───────────────┘
                │ Key & Matching Queries                       │ Payload & Lease Management
                ▼                                              ▼
 ┌──────────────────────────────┐              ┌──────────────────────────────┐
 │     Rust Data Engine         │              │      Go Control Plane        │
 │ • nxradixtree-core Matcher   │              │ • Distributed LeaseManager   │
 │ • nexus-store Host DRAM      │              │ • Monotonic EpochTracker     │
 │ • nexus-transfer Zero-Copy   │              │ • GarbageCollector & Policy  │
 └──────────────────────────────┘              └──────────────────────────────┘
```

---

## 🚀 Beyond KV Cache: Multi-Attention Taxonomy

NexusKV provides specialized state descriptors and validation for next-generation attention architectures:

- **MHA / GQA / MQA**: Standard contiguous and page-aligned key/value tensor caches.
- **DeepSeek MLA (Multi-Head Latent Attention)**: Compressed latent KV states ($c_t^{KV}$) and decoupled RoPE positional tensors ($k_t^R$).
- **DeepSeek DSA (DeepSeek Sparse Attention)**: Query-dependent sparse selection regions and selector index auxiliary metadata.
- **Kimi KDA (Kimi Delta Attention)**: Recurrent terminal state checkpoints ($h_t$) for hybrid attention-recurrent models.

---

## ⚡ Quick Start

### 1. Python Fast Integration

```python
from nexuskv.connectors.vllm.connector import VLLMConnector
from nexuskv.connectors.native_hooks import NativeEngineHookInterceptor
from nexuskv.connectors.base import VLLMLifecycleContext

# Initialize connector & native hook interceptor with <1ms fail-open guarantee
connector = VLLMConnector()
interceptor = NativeEngineHookInterceptor(connector=connector)

# Intercept engine lifecycle event
context = VLLMLifecycleContext(
    tenant="tenant_a",
    namespace="chat_production",
    model="deepseek-v3",
    tokens=[101, 2023, 2003, 1037, 3899],
    descriptor=connector.default_descriptor(),
)

decision = interceptor.intercept_hook("request_start", context)

if decision.materialization_result.status == "completed":
    print("NexusKV Cost-Based Reuse Approved! Binding cached state handle...")
else:
    print("Fail-Open Fallback: Executing GPU prefill recomputation locally.")
```

### 2. Run Benchmark Evidence & Stress Test Suite

Run the synthetic benchmark comparison and 7x24 memory leak stress suite with a single command:

```bash
python3 tools/run_benchmarks.py
```

---

## 🛠 Toolchain & Verification

### Go Control Plane

```bash
GOTOOLCHAIN=go1.25.9 go test ./...
cd go && GOTOOLCHAIN=go1.25.9 go test ./...
```

### Rust Data Plane Workspace

```bash
cd rust
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
```

### Python Suite & Native PyO3 Extension Build

```bash
# Build PyO3 native extension
cd rust
cargo rustc -p bindings-py --crate-type cdylib
cd ..

# Run Python unittest suite (65+ tests)
PYTHONPATH=python python3 -m unittest discover -s python/tests -p "test_*.py"
```

---

## 📚 Documentation Sitemap

- 📄 **Whitepaper:** [Beyond KV Cache: Toward a Zero-Overhead Model State Intelligence Layer for LLM Inference](docs/papers/beyond-kv-cache.md)
- 🏛 **Architecture Design:** [NexusKV Platform Architecture](docs/design/nexuskv-architecture.md)
- 🗺 **Evolution Roadmap:** [Roadmap & Milestone Status](docs/roadmap.md)
- 📝 **Migration History:** [PR Migration History](docs/architecture/migration-status.md)
- 📊 **Benchmark Methodology:** [Benchmark Evaluation Methodology](docs/benchmarks/benchmark-methodology.md)

---

## 📄 License

NexusKV is licensed under the [MIT License](LICENSE).
