# 📖 硬件传输后端适配目录 (Transport Backend Catalog)

本文档列出 NexusKV 支持的传输后端与其句柄挂载规范。

---

## 适配目录清单

1. **`CudaIpcHandleAdapter`**：支持 NVIDIA CUDA IPC 跨进程指针共享；
2. **`AmdRocmHipIpcAdapter`**：支持 AMD ROCm HIP IPC 指针挂载；
3. **`AppleMetalUmaAdapter`**：支持 Apple Silicon 统一内存零拷贝指针直连；
4. **`HuaweiAscendCannAdapter`**：支持华为昇腾 CANN 张量挂载；
5. **`GoogleTpuXlaAdapter`**：支持 GCP TPU XLA Handle 传输。
