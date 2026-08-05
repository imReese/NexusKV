# 📐 Attention 状态描述符设计 (Attention State Descriptor)

本文档说明 NexusKV 如何通过精细化的描述符（Descriptor）来表达多样化的 Attention 架构状态。

---

## 适配的状态类型

- **MHA / GQA / MQA**：标准 Paged KV Tensor 块描述符；
- **DeepSeek MLA**：解耦压缩状态 $c_t^{KV}$ 与 RoPE 键向量 $k_t^R$ 描述符；
- **DeepSeek V4 (CSA & HCA)**：4-token 压缩稀疏块与 128-token 全局上下文摘要；
- **Kimi K3 (KDA)**：终端常驻索引 Checkpoint 描述符。
