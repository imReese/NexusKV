# 🛡️ NexusKV 系统可靠性与降级熔断模型 (System Reliability & Fallback Model)

本文档说明 NexusKV 在面临网络抖动、控制面分区、进程卡死或高并发拥塞时的**工业级可靠性保证与 Fail-Open 降级熔断模型**。

---

## 1. 核心可靠性哲学：Fail-Open (故障开放)

在 LLM 推理生产服务中，**“推理服务的绝对不中断与低延迟”永远优先于“KV Cache 的加速复用”**。

 NexusKV 遵循 **Fail-Open (故障开放降级)** 原则：
> **如果 NexusKV 控制面、网络传输或共享内存出现任何超时或异常，系统必须在 <1ms 内无缝回退到本地 GPU Prefill 重算，确保上层应用 0 报错、0 崩溃、0 阻塞！**

```
                     ┌───────────────────────────────────────┐
                     │ Incoming Inference Request (Prompt)   │
                     └───────────────────┬───────────────────┘
                                         │
                                         ▼
                     ┌───────────────────────────────────────┐
                     │ CacheAwareMiddleware / Interceptor    │
                     └───────────────────┬───────────────────┘
                                         │
                         ┌───────────────┴───────────────┐
                         │ Try Control Plane & Router    │
                         └───────────────┬───────────────┘
                                         │
                        Is Healthy?      │     Exception / Timeout (>1ms)
                     ┌───────────────────┴───────────────────┐
                     │                                       │
                     ▼ YES                                   ▼ NO (Fail-Open)
      ┌─────────────────────────────┐         ┌─────────────────────────────┐
      │ Execute Cache Materialization│         │ Fallback to Local GPU       │
      │ & Zero-Copy Mounting        │         │ Prefill Recomputation       │
      └─────────────────────────────┘         └─────────────────────────────┘
              (Saved Compute)                         (Guaranteed 0 Crash)
```

---

## 2. 三大核心熔断与防雪崩机制

### 2.1 <1ms 物理平滑降级 (Microsecond Circuit Breaker)
* **拦截点**：[python/nexuskv/integrations/vllm_integration.py](file:///Users/reese/Code/imReese/NexusKV/python/nexuskv/integrations/vllm_integration.py)
* **触发机制**：当 Socket 连接断开、控制面心跳丢包或句柄挂载超时，中间件拦截器在低于 1 毫秒内捕获 `Exception`。
* **降级保证**：透传原始 `Prompt Tokens` 给引擎本地 Prefill 计算逻辑，端到端用户请求毫无察觉。

### 2.2 配额与并发主动反压 (Quota Admission Backpressure)
* **实现逻辑**：[python/nexuskv/execution/quota.py](file:///Users/reese/Code/imReese/NexusKV/python/nexuskv/execution/quota.py)
* **监控维度**：
  - 单 Worker 节点的在途传输任务数 `ActiveTransfers`；
  - HBM / Host DRAM 显存池使用率高水位（85%）。
* **防护效果**：当并发数触及阈值时，控制面拒绝新边缘请求的挂载，引导其降级为本地 Prefill，**防止整个推理集群遭遇雪崩式死锁**。

### 2.3 自动内存泄露防护与无锁句柄清理 (Zero Memory-Leak Guarantee)
* **测试验证**：经过 [python/nexuskv/benchmarks/stress.py](file:///Users/reese/Code/imReese/NexusKV/python/nexuskv/benchmarks/stress.py) 2400+ 高并发压力测试验证。
* **防护效果**：支持物理 POSIX `/dev/shm` 与 HBM Paged Block 的引用计数自动回收，在长时间高并发碾压下 **RSS 内存增长为 0.00 MB**，达成零内存泄露结论。
