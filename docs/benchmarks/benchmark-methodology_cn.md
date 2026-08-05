# 📊 NexusKV 基准测试方法论与性能打点规范

本文档定义 NexusKV 双维度基准测试的评估准则与测量方法。

---

## 双维度测量标准

1. **控制决策维度 (Decision Intelligence Rate)**：
   - 关注 QPS / RPS 吞吐量与微秒级响应延迟 (P50, P90, P99)；
   - 使用 `time.perf_counter_ns()` 测量真实 Wall-Clock 物理开销。

2. **字节容量与算力卸载维度 (Payload Capacity & Bandwidth)**：
   - 测量复用的 KV Tensor 物理字节容量（MB / GB）；
   - 计算避免 GPU 重算所折算的等效传输带宽（GB/sec）。
