# 🛠️ NexusKV 架构迁移与 PR 演进状态

本文档追踪 NexusKV 从早期原型向现代 Rust/Go/Python 生产级三层解耦架构迁移的全过程。

---

## 历史 PR 演进记录

### PR 18: Python 原生 Hook 拦截器与集群压测套件
- 新增 `NativeEngineHookInterceptor` (`python/nexuskv/connectors/native_hooks.py`)，提供 <1ms 强保障平滑降级机制。
- 新增 `ClusterStressTestRunner` (`python/nexuskv/benchmarks/stress.py`)，提供高并发集群压测与内存 RSS 零泄漏检测。

### PR 19: Prefill-Decode (PD) 分离握手钩子
- 在 `python/nexuskv/connectors/base.py` 中增加 `PDDisaggregateContext` 与 `on_pd_disaggregate_handshake` 跨节点握手生命周期钩子。

### PR 21: GPU HBM 直接分配器与前沿状态契约扩展
- 在 Rust (`rust/crates/nexus-store/src/hbm.rs`) 和 Python (`python/nexuskv/execution/hbm.py`) 中实现 `HbmBlockAllocator`，负责 HBM 分块管理与 Pin/Unpin 换入换出。
- 在 Schema 中扩充 `CSA_STATE` (DeepSeek V4)、`HCA_SUMMARY` 与 `DSPARK_SPARSE` 状态描述符。
- 在 `python/nexuskv/execution/k3_cascade.py` 中实现 `K3CascadeMountEngine`，支持 Kimi K3 终端 Checkpoint 常驻与历史级联挂载。

### PR 22: 控制面心跳发现、Maturin 打包、Prometheus 监控与 POSIX 共享内存
- 在 Go (`go/controlplane/fabric/discovery.go`) 中实现 `NodeDiscoveryService` 与 `WorkerHeartbeatMonitor`，支持心跳超时自动撤销租约。
- 配置 Maturin `pyproject.toml` 支持 Python `.whl` 轮子打包。
- 在 Go (`go/controlplane/fabric/metrics.go`) 中实现 `PrometheusMetricsExporter` 暴露指标。
- 在 Rust (`rust/crates/nexus-transfer/src/shm.rs`) 中实现 `PosixShmAllocator` 共享内存分配器。
