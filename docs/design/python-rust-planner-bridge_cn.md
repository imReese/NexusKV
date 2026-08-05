# 🌉 Python-Rust 桥接设计 (Python-Rust Planner Bridge)

本文档说明 `nexuskv.planner.rust_backend` 如何通过 PyO3 C-Extension 桥接 Rust 高性能前缀树与 Python 推理引擎。

---

## 跨平台模块加载与降级

- **跨平台动态库自动匹配**：自动解析 macOS (`.dylib`)、Linux (`.so`) 与 Windows (`.pyd`/`.dll`)；
- **纯 Python 平滑降级**：如果动态库缺失，自动降级为内置的 Python 前缀树模块，保证 100% 可用性。
