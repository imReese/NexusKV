# 🚧 系统执行边界与隔离设计 (Execution Boundary)

本文档说明 NexusKV 在进程、线程与硬件级别的隔离边界设计。

---

## 边界层级

1. **Python / Rust 语言边界**：使用 PyO3 Native C 扩展通信，通过零拷贝避免内存重复序列化；
2. **Sidecar / Inference Engine 进程边界**：通过 Unix Domain Socket (UDS) 或 POSIX `/dev/shm` 进行跨进程隔离；
3. **Controlplane / Worker 网络边界**：基于 gRPC 与单调 Epoch 隔离集群元数据。
