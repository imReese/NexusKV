# ⚙️ 控制面执行策略与 Quota 准入 (Controlplane Execution Policy)

本文档说明 Go 控制面如何通过配置策略管理配额与一致性。

---

## 策略组件

1. **`LeaseManager`**：租约秒级自动过期与显式撤销机制；
2. **`EpochTracker`**：全局单调递增纪元，防止网络延迟导致旧元数据覆盖；
3. **`WorkerHeartbeatMonitor`**：心跳故障检测与超时节点清理。
