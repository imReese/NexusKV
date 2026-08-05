# 📄 超越 KV Cache：打造大语言模型推理的零开销模型状态智能层

这是一篇关于 NexusKV 核心算法架构设计与生产级评测的技术论文中文精译版。

---

## 摘要 (Abstract)

在最新的 LLM 推理场景中，传统的 Hit-Driven 盲目复用会导致显着的 TTFT 恶化（TTFT Regression）。NexusKV 提出了模型状态智能层（Model State Intelligence Layer），通过控制面与数据面解耦、收益成本评估方程 $G = T_{\text{compute}} - T_{\text{cache}} > 0$ 以及微秒级 Radix 匹配引擎，在大规模 long-context 与 PD 分离场景中实现了零开销、零恶化的状态复用。

详细架构设计与实测数据请参阅英文原版 [beyond-kv-cache.md](beyond-kv-cache.md) 与架构文档 [docs/architecture_cn.md](../architecture_cn.md)。
