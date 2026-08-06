# NexusKV 架构设计与系统规范 (Architecture Specification)

NexusKV 是专为分布式 LLM 推理集群打造的 **KV Cache 智能感知存储与微秒级传输引擎**。

---

## 1. 极简系统架构 (System Architecture)

NexusKV 采用 **控制面与数据面解耦** 的 4 层结构设计：

![NexusKV Architecture Overview](images/nexus_architecture_overview.jpg)

### 1.1 核心分层职责 (Layer Responsibilities)

```text
┌────────────────────────────────────────────────────────────────────────┐
│ Layer 1: Control Plane (Go Raft)                                      │
│ Consensus & Coordination · HashRing Migration · Binary WAL Engine      │
└────────────────────────────────────────────────────────────────────────┘
                                   │ Metadata Sync (:9090 gRPC)
                                   ▼
┌────────────────────────────────────────────────────────────────────────┐
│ Layer 2: Data Plane (Rust Radix Memory)                                │
│ nxradixtree-core (Agentic CoW) · Host DRAM LRU · POSIX Zero-Copy SHM   │
└────────────────────────────────────────────────────────────────────────┘
                                   │ FFI Interceptor (<100ns)
                                   ▼
┌────────────────────────────────────────────────────────────────────────┐
│ Layer 3: Connectors & SDK (vLLM & SGLang)                              │
│ PagedAttention V2 Hook · SGLang Interceptor · C++ Client SDK           │
└────────────────────────────────────────────────────────────────────────┘
                                   │ Direct Memory Transport
                                   ▼
┌────────────────────────────────────────────────────────────────────────┐
│ Layer 4: Hardware Transport (NVLink & 400G RDMA)                       │
│ NVLink 6 (3.6TB/s) · RoCEv2 (400Gbps) · <1ms Fail-Open Fallback        │
└────────────────────────────────────────────────────────────────────────┘
```

---

## 2. 5 级传输降级阶梯 (5-Tier Transport Failover)

NexusKV 提供极速自动降级状态机，保障推理集群高可用：

1. **CUDA_IPC** (0.01ms) — 同机 NVLink 显存直传
2. **NVLINK** (0.05ms) — Multi-GPU 跨卡织网
3. **RDMA_ROCE** (0.2ms) — 跨节点 400Gbps 网卡直传
4. **HOST_DRAM** (0.5ms) — CPU 锁页内存中转
5. **FAIL_OPEN** (<1ms) — 熔断保护，触发本地 GPU Prefill 重算，**确保上层服务绝不崩溃**

---

## 3. 全量环境变量配置 (Environment Configuration)

| 环境变量 | 默认值 | 作用描述 |
| :--- | :--- | :--- |
| `NEXUSKV_LOG_LEVEL` | `INFO` | 日志输出级别 (`DEBUG`/`INFO`/`WARN`/`ERROR`) |
| `NEXUSKV_PREFERRED_NIC` | *(自动)* | 指定 RDMA 物理网卡 (如 `mlx5_0`) |
| `NEXUSKV_FAIL_OPEN_MODE` | `true` | <1ms Fail-Open 极速降级熔断开关 |
| `NEXUSKV_GPU_DIRECT_RDMA`| `true` | GPUDirect RDMA 显存直传开关 |

---

## 4. 运维诊断 (Operations)

```bash
# 启动 Prometheus + Grafana 服务
docker compose up -d

# 运行集群健康诊断
nexuskv-cli status
```
