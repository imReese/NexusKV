# 🏛 NexusKV 核心架构与多后端全景指南

NexusKV 是专为大语言模型 (LLM) 状态感知打造的 **Model State Intelligence Layer（模型状态智能决策层）**。

---

## 架构层级划分

NexusKV 采用 **Go (控制面) + Rust (数据面/前缀树引擎) + Python (推理引擎接插件)** 的三层分离架构：

```text
 ┌──────────────────────────────────────────────────────────────┐
 │ 1. Go Control Plane (nexuskv-controlplane)                   │
 │    - Lease 租约分配 / Epoch 纪元单调递增 / 心跳与发现             │
 └──────────────────────────────┬───────────────────────────────┘
                                │ (RPC / Sub-ms Metadata)
 ┌──────────────────────────────▼───────────────────────────────┐
 │ 2. Rust Core Engine (nxradixtree-core & nexus-store)         │
 │    - Radix 前缀树微秒查找 / HbmBlockAllocator / POSIX SHM      │
 └──────────────────────────────┬───────────────────────────────┘
                                │ (PyO3 C-FFI / Native IPC)
 ┌──────────────────────────────▼───────────────────────────────┐
 │ 3. Python Interceptor Layer (python/nexuskv)                 │
 │    - vLLM / SGLang Connector / DynamicCostProfiler           │
 └──────────────────────────────────────────────────────────────┘
```

---

## 前沿状态契约 Taxonomies

NexusKV 完美适配前沿模型的多样化 KV 状态，包括：
1. **DeepSeek V4**: `CSA_STATE` (4-token FP4 稀疏块) 与 `HCA_SUMMARY` (128-token 全局摘要)；
2. **Kimi K3**: `K3CascadeMountEngine` 终端常驻索引与递推级联挂载；
3. **DSpark**: `DSPARK_SPARSE` 分片稀疏状态。

---

## 多后端硬件适配矩阵

NexusKV 支持异构硬件架构：
- **NVIDIA**: CUDA IPC / UVA / NVLink / NIXL SDK
- **AMD**: ROCm HIP IPC
- **Apple Silicon**: Metal UMA 零拷贝
- **Huawei**: Ascend CANN
- **Google**: TPU XLA
- **Intel**: Gaudi Level-Zero / XPU
- **Cambricon / Musa / Biren**: 寒武纪 MLU / 摩尔线程 MUSA / 壁仞 BR100
