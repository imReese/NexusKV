# 🛡️ NexusKV 系统可靠性与降级熔断模型

本文档说明 NexusKV 在面临网络抖动、进程异常或高并发拥塞时的可靠性模型。

---

## 核心可靠性机制

1. **<1ms 强保障 Fail-Open 平滑降级**：
   - 当 Sidecar 进程卡死、Socket 超时或跨节点网络未就绪时，`NativeEngineHookInterceptor` 会在 1 毫秒内自动捕获异常，将请求平滑退回到 GPU 本地重算（Prefill），防止推理引擎主进程死锁。

2. **配额与并发主动反压 (Quota Admission Backpressure)**：
   - `QuotaTracker` 实时监控当前节点与网络的数据搬运并发量。当并发达到上限时，主动拒绝新的复用开销请求，回退至本地 Prefill 重算，防止集群出现雪崩效应。
