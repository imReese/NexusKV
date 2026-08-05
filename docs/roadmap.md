# 🧭 NexusKV High-Velocity Technical Roadmap (2026 Quarterly Plan)

---

## 💡 1. Industry Trends

In 2026, LLM inference infrastructure is undergoing rapid physical evolution:

1. **Prefill-Decode (PD) Disaggregation**: LLM inference is moving to physical node separation between Prefill and Decode. Cross-node KV transfer latency is now the dominant bottleneck for TTFT.
2. **Next-Gen Attention Taxonomy**: Architectures such as DeepSeek-V3 / MLA (Low-Rank Latent Space) and Kimi-K1.5 / KDA (Recurrent State) require the infrastructure layer to natively understand state semantics.
3. **Physical Transport Fabric Shift**: Hardware transport is advancing from TCP/RDMA to NVLink P2P, CXL 3.0 shared memory pools, and NIXL fabrics.
4. **Agentic Multi-Branch Execution**: Agentic AI and Tree-of-Thought (ToT) search require zero-overhead Copy-on-Write (CoW) multi-branch prefix state sharing and speculative prefetching.

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
 │ 2026 Q2 (In Progress): DeepSeek-V3 / MLA Optimization & Speculative Prefetching    │
 │ - MLA 512D Latent Vector Decoupling / Speculative Prefetch Engine / Helm & PyPI   │
 └─────────────────────────────────────────┬─────────────────────────────────────────┘
                                           │
                                           ▼
 ┌───────────────────────────────────────────────────────────────────────────────────┐
 │ 2026 Q3: Lock-Free Concurrent Radix Tree, C++ SDK & Consistent Hashing            │
 │ - RCU Lock-Free Radix Tree / C++ Header-Only SDK / Consistent Hash Ring Self-Healing │
 └─────────────────────────────────────────┬─────────────────────────────────────────┘
                                           │
                                           ▼
 ┌───────────────────────────────────────────────────────────────────────────────────┐
 │ 2026 Q4: CXL 3.0 Memory Pool & NIXL Hardware Zero-Copy Fabric                      │
 │ - CXL 3.0 Shared Memory TraCT / NIXL P2P Driver Integration / Multi-Branch CoW   │
 └───────────────────────────────────────────────────────────────────────────────────┘
```

---

### 2026 Q1: Core Polyglot Engine & Zero-Overhead Contracts (Completed)

- [x] **Go Distributed Control Plane**: Raft state synchronization, WAL logging, and `TenantQuotaManager` hard isolation.
- [x] **Rust Kernel & Matching Engine**: `nxradixtree-core` prefix tree, `nexus-store` pinned memory, and POSIX SHM zero-copy.
- [x] **Universal C-FFI / Python Attachment Protocol**: Decoupled from specific inference engines; native adapters for vLLM and SGLang.
- [x] **Apache 2.0 Open Source License**: Explicit patent licensing protection.

---

### 2026 Q2: DeepSeek-V3 / MLA Optimization & Speculative Prefetching (In Progress)

- [x] **Grafana & Observability Panel**: Pre-configured `grafana-dashboard.json` for hit rates, GB/s bandwidth, and Fail-Open events.
- [x] **PyPI Wheel Automated Build**: `.github/workflows/release.yml` with cross-platform Maturin binaries.
- [x] **Speculative Intent Prefetch Engine**: Asynchronous pipeline preloading before decode phase (`prefetch.py`).
- [ ] **DeepSeek-V3 / MLA State Mapping**: Layer-wise KV compression and quantization scale alignment for DeepSeek-V3 MLA ($c_t^{KV} + k_t^R$).

---

### 2026 Q3: Lock-Free Concurrent Radix Tree, C++ SDK & Consistent Hashing

- [ ] **Lock-Free Concurrent Radix Tree**: RCU (Read-Copy-Update) node traversal in `nxradixtree-core` for high CPU core concurrency.
- [ ] **Header-Only C++ Client SDK**: Lightweight C++ client library (`nexuskv_client.h`) for TensorRT-LLM, LMDeploy, and custom C++ gateways.
- [ ] **Consistent Hash Ring Self-Healing**: Virtual node hash ring in Go control plane for smooth worker node scaling.

---

### 2026 Q4: CXL 3.0 Memory Pool & NIXL Hardware Zero-Copy Fabric

- [ ] **CXL 3.0 Rack-Scale Shared Memory Pool**: Direct Load/Store memory mapping via CXL 3.0 TraCT for 0-hop network transfer.
- [ ] **NVIDIA NIXL Native Engine**: NIXL P2P driver integration for hardware-level cross-node GPU memory transport.
- [ ] **Agentic Multi-Branch CoW Radix Tree**: Copy-on-Write state sharing for Tree-of-Thought (ToT) and MCTS search.
