# ⚙️ NexusKV Control Plane Execution Policy & Consistency Architecture

This document details the architecture, reliability guarantees, and consistency mechanisms of the NexusKV Distributed Control Plane (Go Control Plane).

---

## 1. Architecture Overview

The control plane service (`nexuskv-controlplane`) operates in a 3-node or 5-node embedded Raft topology as the central intelligence engine for LLM inference clusters.

```
                     ┌──────────────────────────────────────┐
                     │    Go Distributed Control Plane      │
                     │                                      │
                     │   ┌──────────────────────────────┐   │
                     │   │   Embedded Raft Consensus    │   │
                     │   │   (Log Replication Commit)   │   │
                     │   └──────────────┬───────────────┘   │
                     │                  │                   │
                     │   ┌──────────────▼───────────────┐   │
                     │   │   Monotonic Epoch Generator  │   │
                     │   └──────────────┬───────────────┘   │
                     │                  │                   │
                     │   ┌──────────────▼───────────────┐   │
                     │   │    Lease & Heartbeat Engine  │   │
                     │   └──────────────────────────────┘   │
                     └──────────────────┬───────────────────┘
                                        │
                                        ▼
             ┌─────────────────────────────────────────────────────┐
             │  Worker Nodes (vLLM / SGLang GPU Instances)         │
             │  - Heartbeat Pulse Every 500ms                      │
             │  - Cache Block Pin / Unpin Lease Acquisition        │
             └─────────────────────────────────────────────────────┘
```

---

## 2. Four Pillars of Consistency & Reliability

### 2.1 Embedded Raft Consensus Protocol
* **Implementation**: Integrated embedded Raft consensus engine ([pkg/raft/](file:///Users/reese/Code/imReese/NexusKV/pkg/raft)).
* **Guarantee**: All worker node registrations, global cache metadata updates, and lease allocations require Quorum log commit before becoming active, guaranteeing Strong Consistency across cluster nodes.

### 2.2 Monotonic Epoch Counter (Anti-Stale Metadata Overwrite)
* **Implementation**: Maintains a global monotonic epoch counter `MonotonicEpoch`.
* **Verification**:
  $$\text{Verify}(E_{\text{event}}, E_{\text{current}}) = \begin{cases} \text{ACCEPT}, & \text{if } E_{\text{event}} \ge E_{\text{current}} \\ \text{DISCARD}, & \text{if } E_{\text{event}} < E_{\text{current}} \end{cases}$$
* **Guarantee**: Late-arriving RPC requests caused by network jitter or temporary partitions are discarded if their epoch is lower than the current active epoch, preventing stale metadata overwrites.

### 2.3 Lease & Worker Heartbeat Revocation Engine
* **Source Code**: [go/controlplane/fabric/discovery.go](file:///Users/reese/Code/imReese/NexusKV/go/controlplane/fabric/discovery.go)
* **Guarantee**: Workers receive time-bounded leases (default 5000ms TTL) and send 500ms heartbeat pulses. If a worker fails 3 consecutive heartbeats (1500ms), `RevokeLeasesForHolder` automatically reclaims its cache blocks.

### 2.4 Quota Admission Backpressure
* **Source Code**: [go/controlplane/fabric/metrics.go](file:///Users/reese/Code/imReese/NexusKV/go/controlplane/fabric/metrics.go)
* **Guarantee**: Monitors active transfer queue depth and memory high-water marks (85%). Rejects low-gain requests under heavy load, prompting client fallback to local GPU prefill to prevent cluster-wide cascading failure.
