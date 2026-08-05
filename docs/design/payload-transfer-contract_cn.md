# 📦 Payload 跨节点传输契约 (Payload Transfer Contract)

本文档说明 NexusKV 在跨节点传输 KV Tensor 时的零拷贝传输契约。

---

## 契约字段

- **`base_addr`**：物理显存/内存虚拟起始地址；
- **`size_bytes`**：字节数据块容量大小；
- **`location`**：硬件物理位置（CPU NUMA、CUDA Device、ROCm Device、Metal Device 等）；
- **`transport_handle`**：传输句柄（CUDA IPC Handle / POSIX SHM Name / NIXL Handle）。
