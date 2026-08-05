# 🧭 NexusKV High-Velocity Technical Roadmap (2026 Quarterly Plan)

---

## 💡 1. 2026 State-of-the-Art LLM Inference & Hardware Interconnects

In 2026, the LLM inference ecosystem is undergoing rapid evolution led by **DeepSeek-V4** and the **NVIDIA Vera Rubin / Blackwell Ultra** platforms:

1. **Hardware-Assisted Prefill-Decode (PD) Offloading**: With the introduction of **NVIDIA Rubin CPX** (a dedicated prefill accelerator) and Rubin HBM4 GPUs, Prefill and Decode are physically decoupled at the silicon level.
2. **DeepSeek-V4 Hybrid Attention (CSA + HCA)**: DeepSeek-V4 introduces **Compressed Sparse Attention (CSA)** and **Heavily Compressed Attention (HCA)** for 1M+ token contexts, reducing KV memory burden by 90% compared to V3.
3. **High-Speed Interconnects & Fabric-Attached Memory**: Physical transport is shifting to **6th-Gen NVLink**, **UALink 2.0 open fabric**, and **CXL 3.1** rack-scale memory pools.
4. **NVFP4 Quantization & Agentic Multi-Branch CoW**: Under NVFP4 low-precision inference, the state layer natively aligns FP4 quantization scales and supports zero-overhead Copy-on-Write prefix sharing for Tree-of-Thought (ToT) agentic search.

---

## 🏛 2. 2026 Quarterly Milestones

```text
 ┌───────────────────────────────────────────────────────────────────────────────────┐
 │ 2026 Q1 (Completed): Core Polyglot Engine & Zero-Overhead Contracts               │
 │ - Go Control Plane / Rust `nxradixtree-core` / Python FFI / Apache 2.0 License    │
 └─────────────────────────────────────────┬─────────────────────────────────────────┘
                                           │
                                           ▼
 ┌───────────────────────────────────────────────────────────────────────────────────┐
 │ 2026 Q2 (In Progress): DeepSeek-V4 (CSA/HCA) Hybrid Attention & NVFP4 Alignment   │
 │ - DeepSeek-V4 CSA/HCA State Mapping / NVFP4 Scale Alignment / Helm & PyPI         │
 └─────────────────────────────────────────┬─────────────────────────────────────────┘
                                           │
                                           ▼
 ┌───────────────────────────────────────────────────────────────────────────────────┐
 │ 2026 Q3: Rubin CPX Prefill Disaggregation, C++ SDK & Consistent Hashing            │
 │ - Rubin CPX Prefill Offloading / RCU Lock-Free Radix Tree / C++ SDK / Self-Healing │
 └─────────────────────────────────────────┬─────────────────────────────────────────┘
                                           │
                                           ▼
 ┌───────────────────────────────────────────────────────────────────────────────────┐
 │ 2026 Q4: UALink 2.0 Open Fabric, CXL 3.1 Memory Pool & Agent Multi-Branch CoW     │
 │ - UALink 2.0 / CXL 3.1 Fabric-Attached Memory / Agentic ToT CoW Tree              │
 └───────────────────────────────────────────────────────────────────────────────────┘
```

---

### 2026 Q1: Core Polyglot Engine & Zero-Overhead Contracts (Completed)

- [x] **Go Distributed Control Plane**: Raft state synchronization, WAL logging, and `TenantQuotaManager` hard isolation.
- [x] **Rust Kernel & Matching Engine**: `nxradixtree-core` prefix tree, `nexus-store` pinned memory, and POSIX SHM zero-copy.
- [x] **Universal C-FFI / Python Attachment Protocol**: Decoupled from specific inference engines; native adapters for vLLM and SGLang.
- [x] **Apache 2.0 Open Source License**: Explicit patent licensing protection.

---

### 2026 Q2: DeepSeek-V4 (CSA/HCA) Hybrid Attention & NVFP4 Alignment (In Progress)

- [x] **Grafana & Observability Panel**: Pre-configured `grafana-dashboard.json` for hit rates, GB/s bandwidth, and Fail-Open events.
- [x] **PyPI Wheel Automated Build**: `.github/workflows/release.yml` with cross-platform Maturin binaries.
- [x] **Speculative Intent Prefetch Engine**: Asynchronous pipeline preloading before decode phase (`prefetch.py`).
- [ ] **DeepSeek-V4 (CSA + HCA) State Mapping**: Layer-wise state mapping and NVFP4 quantization scale alignment for DeepSeek-V4.

---

### 2026 Q3: Rubin CPX Prefill Disaggregation, C++ SDK & Consistent Hashing

- [ ] **NVIDIA Rubin CPX Hardware PD Handshake**: Hardware-level `pd_disaggregate_handshake` offloading for Rubin CPX prefill processors and Rubin HBM4 nodes.
- [ ] **Lock-Free Concurrent Radix Tree**: RCU (Read-Copy-Update) node traversal in `nxradixtree-core` for high CPU core concurrency.
- [ ] **Header-Only C++ Client SDK**: Lightweight C++ client library (`nexuskv_client.h`) for TensorRT-LLM, LMDeploy, and custom C++ gateways.
- [ ] **Consistent Hash Ring Self-Healing**: Virtual node hash ring in Go control plane for smooth worker node scaling.

---

### 2026 Q4: UALink 2.0 Open Fabric, CXL 3.1 Memory Pool & Agent Multi-Branch CoW

- [ ] **UALink 2.0 & CXL 3.1 Rack-Scale Memory Pool**: Fabric-Attached Memory direct Load/Store mapping for 0-hop network transfer.
- [ ] **Agentic Multi-Branch CoW Radix Tree**: Copy-on-Write state sharing for Tree-of-Thought (ToT) and MCTS search.
