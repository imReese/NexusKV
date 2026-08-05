# 🎯 NexusKV 目标平台与运行环境矩阵

NexusKV 支持跨异构硬件与操作系统的部署落地。

---

## 运行环境与硬件支持矩阵

| 平台 / 芯片 | 硬件传输与 IPC 机制 | 物理内存/显存类型 | 状态与适用场景 |
| :--- | :--- | :--- | :--- |
| **NVIDIA GPU** | CUDA IPC / NVLink / NIXL SDK | HBM2e / HBM3 / HBM3e | 工业级千卡推理集群、PD 分离 |
| **AMD GPU** | ROCm HIP IPC | HBM2 / HBM3 | Instinct 系列卡推理集群 |
| **Apple Silicon** | Metal UMA Direct Pointer | 统一内存 (UMA) | 本地开发测试与 Apple 芯片推理 |
| **Huawei Ascend** | Ascend CANN Transport | HBM | 华为昇腾 910 系列算力集群 |
| **Google TPU** | TPU XLA Handle | High Bandwidth Memory | GCP TPU v4/v5e 集群 |
| **Intel Gaudi** | Level-Zero Transport | HBM | Gaudi 2/3 推理集群 |
