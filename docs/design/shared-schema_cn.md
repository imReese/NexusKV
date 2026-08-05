# 📜 跨语言共享契约 Schema 设计 (Shared Schema)

NexusKV 的核心类型系统建立在单一起源的 JSON Schema（`schema/nexuskv_contract.json`）之上。

---

## 自动代码生成流程

运行 `python3 tools/generate_contracts.py` 会自动重新生成以下绑定：
1. **Python 绑定**：`python/nexuskv/contracts/generated.py`；
2. **Rust 绑定**：`rust/crates/nexus-state/src/generated.rs`；
3. **Go 绑定**：`go/controlplane/fabric/generated.go`。
