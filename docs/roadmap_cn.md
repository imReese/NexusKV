# 🗺️ NexusKV 架构演进路线图

本文档记录 NexusKV 模型状态智能层的研发里程碑与演进规划。

---

## 阶段列表与完成状态

### Phase 1 (v1.0): 核心基础设施与底层契约
**状态：** 已完成  
- 完成 Go 控制面框架（`LeaseManager` 租约、`EpochTracker` 单调纪元、`GarbageCollector` 垃圾回收）。
- 完成 Rust 高性能前缀树核心引擎 (`nxradixtree-core`)。
- 制定跨语言契约 Schema (`schema/nexuskv_contract.json`)。

### Phase 2 (v1.1): Multi-Head / Grouped-Query Attention (MHA/GQA) 状态适配
**状态：** 已完成  
- 建立 FP16 / BF16 MHA 与 GQA 状态描述符。
- 实现微秒级配额检查（`QuotaTracker`）与自动容量淘汰策略。

### Phase 3 (v1.2): DeepSeek MLA & DeepSeek DSA 稀疏注意力契约
**状态：** 已完成  
- 引入 DeepSeek Multi-Head Latent Attention (MLA) 解耦状态向量 ($c_t^{KV}$ 与 $k_t^R$)。
- 引入 DeepSeek Sparse Attention (DSA) 动态稀疏 indexing descriptor。

### Phase 4 (v1.3): 多后端硬件传输适配层
**状态：** 已完成  
- 实现 NVIDIA CUDA IPC / UVA / NVLink 零拷贝通道。
- 扩展 AMD ROCm HIP IPC、Apple Metal UMA、华为昇腾 Ascend CANN、谷歌 TPU XLA、英特尔 Gaudi 等 9 大异构硬件适配器。

### Phase 5 (v1.4): SGLang UnifiedRadix & HiCache 适配
**状态：** 已完成  
- 实现 SGLang `UnifiedRadixCache` 与多级缓存句柄共享契约。

### Phase 6 (v1.5): Prefill-Decode (PD) 分离握手与动态 Cost 自动调优
**状态：** 已完成  
- 引入 `pd_disaggregate_handshake` 跨节点 PD 分离握手钩子。
- 引入 `DynamicCostProfiler`，根据实时网络带宽与 GPU 算力动态计算 $G = T_{\text{compute}} - T_{\text{cache}}$ 成本方程。

### Phase 7 (v2.0): GPU HBM 显存直接分配与前沿模型状态扩展
**状态：** 已完成  
- 实现 Rust/Python 双语言 `HbmBlockAllocator`，接管物理 GPU HBM Paged Block 显存池与 Pin/Unpin 卸载。
- 扩充 DeepSeek V4 (`CSA_STATE` 4-token FP4 稀疏块、`HCA_SUMMARY` 128-token 摘要)、DSpark 稀疏分片契约与 Kimi K3 复现记忆挂载引擎。

### Phase 8 (v2.1): 控制面心跳发现、Maturin 轮子打包与 Prometheus 监控
**状态：** 已完成  
- 实现 Go `NodeDiscoveryService` 与 Worker 心跳监控（超时自动撤销 Cache 租约）。
- 配置 Maturin `pyproject.toml`，支持一键构建 Python `.whl` 轮子包。
- 导出 Prometheus 监控大屏端点（`PrometheusMetricsExporter`）与 POSIX `/dev/shm` 共享内存驱动。

---

## 研发准入闸门 (Development Gate)

每个新特性的加入都必须回答以下三个核心问题：
1. 它是否保持了控制决策层的微秒级低延迟（<50us）？
2. 它是否能防止慢网或短 Prompt 下的 TTFT 恶化（TTFT Regression）？
3. 它是否具备 <1ms 强保障的平滑降级（Fail-Open Fallback）能力？
