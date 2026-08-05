# 🧭 NexusKV Long-Term Technical Roadmap (2026 - 2028)

[简体中文文档 (Chinese Version)](roadmap_cn.md)

---

## 💡 1. Industry Landscape Analysis & Strategic Vision

In 2026, LLM inference infrastructure is undergoing a fundamental paradigm shift. Through deep research into cutting-edge industry implementations (NVIDIA NIXL, CXL 3.0 TraCT, UALink 2.0, SGLang HiCache, Mooncake Store), we identify **5 key technological drivers**:

1. **Prefill-Decode (PD) Disaggregation at Rack-Scale**: Cross-node KV transfer latency (TTFT / TBT) has replaced raw compute as the primary SLA bottleneck.
2. **Transport Fabric Evolution**: Transitioning from traditional TCP/RDMA to **CXL 3.0 Shared Memory Pools (0-hop direct load/store)** and **UALink 2.0 / NIXL high-speed interconnects**.
3. **Universal State Taxonomy**: Unifying Dense (MHA/GQA), Latent (MLA), Sparse/Windowed (CSA/HCA/SWA), and Recurrent State (KDA, Mamba-2, GDN, RWKV-7).
4. **Agentic Multi-Branch KV Tree**: Copy-on-Write (CoW) branching Radix trees for Agentic AI, Tree-of-Thought (ToT), MCTS search, and Speculative Decoding.
5. **Confidential Memory Fabric**: Hardware-enforced encryption (TEE) for shared KV memory pools in enterprise financial & healthcare workloads.

---

## 🏛 2. Strategic Execution Phases

### Phase 1: Production Stabilization & Universal Engine Contracts (Q3 2026)
- **Goal**: Complete the Go + Rust + Python decoupled architecture with production-ready multi-tenant hard quotas and engine-agnostic contracts.
- **Milestones**:
  - [x] **C-FFI / Native IPC Universal Contract**: Decouple engine hooks from specific framework names.
  - [x] **Attention State Taxonomy**: Complete descriptors for MHA, GQA, MLA, CSA/HCA, KDA, GDN, Mamba-2 ([state.py](file:///Users/reese/Code/imReese/NexusKV/python/nexuskv/adapters/state.py)).
  - [x] **4D Utility Router**: Multi-dimensional decision engine balancing gain, load penalty, HBM watermark, and network latency.
  - [ ] **Multi-Tenant Hard Quota Isolation**: Prevent single tenants from exhausting shared memory buffers.
  - [ ] **Observability Dashboard**: OpenTelemetry / Prometheus metrics for hit rates, transfer bandwidth (GB/s), and Fail-Open events.

### Phase 2: Rack-Scale Memory Pooling & Next-Gen Transports (Q4 2026 - Q1 2027)
- **Goal**: Embrace CXL 3.0 shared memory pooling and NIXL / UALink 2.0 transport primitives.
- **Milestones**:
  - [ ] **CXL 3.0 Memory Pool Fabric**: Direct load/store memory mounting across rack-scale CXL pools (0-Hop zero network transfer).
  - [ ] **NIXL & UALink 2.0 Native Drivers**: Native support for NVIDIA NIXL SDK and open UALink 2.0 interconnects.
  - [ ] **Heterogeneous Hardware Alignment**: Specialized Page Block allocators for Huawei Ascend CANN, Cambricon MLU, and Moore Threads MUSA.

### Phase 3: Agentic KV Tree & Predictive Intelligence (Q2 2027 - Q3 2027)
- **Goal**: Power Agentic AI, MCTS search, and Speculative Decoding with Copy-on-Write branching trees and predictive prefetching.
- **Milestones**:
  - [ ] **Multi-Branch Copy-on-Write (CoW) Radix Tree**: Zero-copy state sharing for multi-path tree search.
  - [ ] **Speculative Agent Prefetcher**: Background prefetching of cold KV caches 2-3 steps ahead based on Agent action topology.
  - [ ] **Dynamic Trade-off Bidding Optimizer**: Real-time bidding decision between "Transfer KV Cache via Network" vs. "Recompute on Local GPU".

### Phase 4: Autonomous Self-Healing & Confidential KV Fabric (Q4 2027+)
- **Goal**: Autonomous zero-touch operations and hardware-encrypted confidential memory pools for 1,000+ GPU fleets.
- **Milestones**:
  - [ ] **TEE Confidential Memory Protection**: Hardware encryption (Intel TDX / AMD SEV / NVIDIA Confidential Compute) for shared KV pools.
  - [ ] **Zero-Downtime Live Migration**: Sub-millisecond state live migration upon GPU ECC errors or hardware faults.
  - [ ] **Cache-Aware Auto-Scaler**: Dynamic auto-scaling of Prefill vs. Decode GPU worker ratios based on global Radix tree heat maps.
