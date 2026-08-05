# 🌲 nxradixtree 原生前缀树引擎设计 (nxradixtree Core Engine)

`nxradixtree-core` 是 NexusKV 中使用 Rust 编写的高性能 Radix 前缀树核心算法库。

---

## 核心特性

- **微秒级查找**：在 100,000 个 Token 节点规模下，单次最长前缀查找耗时低于 3 微秒；
- **确定性打破平局规则**：在多租户/多 Namespace 下保证 Key 稳定匹配；
- **并发只读安全**：支持无锁/读写锁高并发查询。
