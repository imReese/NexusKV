# ⚙️ NexusKV 控制面执行策略与一致性模型 (Controlplane Execution Policy)

本文档详细拆解 NexusKV 分布式控制面（Go Control Plane）在保障分布式集群**强一致性、租约管理、单调递增纪元防脏写以及配额反压**方面的核心架构与策略。

---

## 1. 核心架构概览

控制面服务 (`nexuskv-controlplane`) 采用 3-Node/5-Node 嵌入式 Raft 集群拓扑，作为整个 LLM 推理集群的“脑中枢”。

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
             │  - Heartbeat PulseEvery 500ms                       │
             │  - Cache Block Pin / Unpin Lease Acquisition        │
             └─────────────────────────────────────────────────────┘
```

---

## 2. 一致性与可靠性四大支柱

### 2.1 嵌入式 Raft 共识协议 (Raft Log Consistency)
* **实现逻辑**：控制面内部整合了嵌入式 Raft 共识引擎 ([pkg/raft/](file:///Users/reese/Code/imReese/NexusKV/pkg/raft))。
* **应用场景**：集群全局节点注册、全局 Cache Block 分配表更新、租约授予均须在 Raft 多数派（Quorum）成功提交日志后才对外生效。
* **保证**：即使发生少数派节点宕机，集群控制面仍具备强一致性保证（Strong Consistency）。

### 2.2 递增纪元与防脏写机制 (Monotonic Epoch Tracker)
* **实现逻辑**：控制面维护全局递增纪元计数器 `MonotonicEpoch`。
* **防护逻辑**：
  $$\text{Verify}(E_{\text{event}}, E_{\text{current}}) = \begin{cases} \text{ACCEPT}, & \text{if } E_{\text{event}} \ge E_{\text{current}} \\ \text{DISCARD}, & \text{if } E_{\text{event}} < E_{\text{current}} \end{cases}$$
* **防护效果**：当高延迟或死锁导致的迟到 RPC 到达时，控制面校验发现其纪元低于当前最新纪元，便会自动物理丢弃该包，**从根本上消除了网络乱序导致的过期覆盖（Stale Metadata Overwrite）**。

### 2.3 租约与心跳撤销引擎 (Lease & Worker Heartbeat Engine)
* **代码组件**：[go/controlplane/fabric/discovery.go](file:///Users/reese/Code/imReese/NexusKV/go/controlplane/fabric/discovery.go)
* **工作机制**：
  1. **Lease 授予**：Worker 节点在 Pin 锁定 HBM Cache Block 时，获得有时限的 Lease（默认 TTL 5000ms）。
  2. **Pulse 心跳**：Worker 节点每 500ms 向控制面推送信跳脉冲 `ActiveTransfers` 与显存容量报告。
  3. **超时撤销**：若监控器检测到某 Worker 连续 3 次未响应心跳（1500ms），控制面立即触发 `RevokeLeasesForHolder` **强行回收其持有的全部 Cache Block**，防止死锁。

### 2.4 配额准入与动态反压 (Quota Admission Backpressure)
* **代码组件**：[go/controlplane/fabric/metrics.go](file:///Users/reese/Code/imReese/NexusKV/go/controlplane/fabric/metrics.go)
* **反压触发条件**：
  - 控制面待处理队列深度 > 80% 阈值；
  - 目标 Worker 的并发传输任务数 `ActiveTransfers` 过高。
* **反压效果**：控制面暴露 `nexuskv_quota_backpressure_events_total` 监控指标，并主动拒绝收益边缘的复用请求，引导客户端降级为本地 Prefill 重算，**防止集群雪崩**。
