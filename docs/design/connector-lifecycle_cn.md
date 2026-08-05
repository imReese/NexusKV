# 🔄 引擎接插件生命周期 (Connector Lifecycle)

本文档说明 NexusKV 与 vLLM / SGLang 等推理引擎交互时的生命周期钩子。

---

## 生命周期阶段

1. **`on_request_admitted` (请求准入阶段)**：在推理引擎调度 Batch 前，发起微秒级前缀匹配与收益成本评估。
2. **`on_pd_disaggregate_handshake` (PD 分离握手阶段)**：在 Prefill 节点与 Decode 节点之间进行状态准备与通道挂载。
3. **`on_state_materialized` (状态挂载/复用完成阶段)**：更新逻辑句柄与显存引用。
4. **`on_request_completed` (请求结束/清理阶段)**：释放租约与临时引用。
