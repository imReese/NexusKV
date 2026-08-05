# 🏛 NexusKV 核心架构与通用基础设施全景指南

NexusKV 是专为大语言模型 (LLM) 推理基础设施打造的 **Universal Zero-Overhead Model State Intelligence Layer（通用零开销模型状态智能基础设施层）**。

其核心目标是提供 **引擎无关 (Engine-Agnostic)、模型无关 (Model-Agnostic)、硬件无关 (Hardware-Agnostic)** 的通用大模型状态计算与分布式内存池协议。

---

## 1. 引擎无关架构与抽象分层 (Engine-Agnostic Architecture)

NexusKV 采用 **Go (分布式控制面) + Rust (数据面与 Radix 引擎) + C-FFI / Python (通用引擎挂载协议)** 的分层解耦架构。无论是开源推理引擎还是企业自研 C++ 推理引擎，均可通过统一 Schema 契约无缝接入：

```text
 ┌──────────────────────────────────────────────────────────────────┐
 │ 1. Ingress & Router Layer (网关与入口接入层)                       │
 │    - SGLang Router / Ray Serve / FastChat Gateway / 自研 C++ 网关 │
 └────────────────────────────────┬─────────────────────────────────┘
                                  │ (Universal Schema Contract)
 ┌────────────────────────────────▼─────────────────────────────────┐
 │ 2. Go Control Plane (nexuskv-controlplane)                       │
 │    - 租约分配 / Monotonic Epoch / 流量反压 / 节点心跳发现         │
 └────────────────────────────────┬─────────────────────────────────┘
                                  │ (Sub-ms Metadata Protocol)
 ┌────────────────────────────────▼─────────────────────────────────┐
 │ 3. Rust Data Plane (nxradixtree-core & nexus-store)             │
 │    - Radix 前缀树微秒查找 / 页块分配器 / POSIX SHM 共享内存       │
 └────────────────────────────────┬─────────────────────────────────┘
                                  │ (C-FFI / Native IPC / PyO3)
 ┌────────────────────────────────▼─────────────────────────────────┐
 │ 4. Universal Engine Connector Protocol (通用引擎挂载层)           │
 │    - vLLM / SGLang / TensorRT-LLM / LMDeploy / 自研 C++ Engine    │
 └──────────────────────────────────────────────────────────────────┘
```

---

## 2. 通用模型注意力语义抽象 (Model-Agnostic State Taxonomy)

NexusKV 抛弃了对具体模型商业名称的硬编码绑定，将其抽象为 **4 大通用注意力状态分类 (State Taxonomies)**：

1. **密集型注意力 (Dense Attention: MHA / GQA / MQA)**：
   - 包含传统 Multi-Head / Grouped-Query 注意力，按页块 (Page) 粒度管理全量 KV Tensor。
2. **低秩隐空间注意力 (Latent Attention: MLA)**：
   - 包含低秩潜向量压缩，按低维 Latent 向量管理页面。
3. **稀疏与滑动窗口摘要 (Sparse & Windowed: CSA / HCA / SWA / NSA)**：
   - 包含滑动窗口 (Sliding Window) 与全局 Summary 摘要向量（如 128-token 压缩块）。
4. **线性递归注意力 (Linear Recurrent State: KDA / GDN / SSM / GLA / RWKV / RetNet)**：
   - 包含固定尺寸递归状态 Checkpoint，支持微秒级 $O(1)$ 恒定开销 HBM 挂载。

---

## 3. 多后端硬件适配矩阵 (Hardware-Agnostic Transport)

NexusKV 统一抽象底层 Memory Transport Fabric，支持跨硬件设备的零拷贝与 RDMA 传输：
- **NVIDIA**: CUDA IPC / UVA / NVLink / NIXL SDK
- **AMD**: ROCm HIP IPC / Heterogeneous Memory
- **Huawei**: Ascend CANN HCCL / Shared Memory
- **Apple Silicon**: Metal UMA 统一内存零拷贝
- **Google**: TPU XLA Memory Transport
- **Intel**: Gaudi Level-Zero / XPU
- **Cambricon / Musa / Biren**: 寒武纪 MLU / 摩尔线程 MUSA / 壁仞 BR100
