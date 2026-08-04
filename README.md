# NexusKV

[![NexusKV Unified CI](https://github.com/imReese/NexusKV/actions/workflows/ci.yml/badge.svg)](https://github.com/imReese/NexusKV/actions/workflows/ci.yml)

NexusKV is a Zero-Overhead Model State Intelligence Layer for LLM inference. It separates Control Plane, Data Plane, Intelligence Layer, and engine-adapter concerns across Go, Rust, and Python rather than treating the KV Cache as a single monolithic service.

## Repository Structure

- `go/` — Control Plane scaffold, execution policy distribution, leases (`LeaseManager`), epoch tracking, and garbage collection.
- `rust/` — Data Plane contracts, bounded Host DRAM payload store (`nexus-store`), zero-copy memory registration (`nexus-transfer`), and high-performance radix tree matcher (`nxradixtree-core`).
- `python/` — Engine-facing adapters (vLLM, SGLang), native FFI hooks, quota admission tracker, cost estimator ($G = T_{compute} - T_{cache}$), multi-attention descriptors (MLA, DSA, KDA), tenant security isolation, and benchmark evidence engine.
- `schema/` — Versioned JSON schema contracts (`nexuskv_contract.json` & `nexuskv_execution_policy.json`).
- `docs/` — Whitepaper v1.1, architecture design, migration PR history, and benchmark methodology.

## Start Here

- **Whitepaper:** [NexusKV Whitepaper v1.1 — Beyond KV Cache: Toward a Zero-Overhead Model State Intelligence Layer for LLM Inference](docs/papers/beyond-kv-cache.md)
- **Architecture:** [NexusKV Architecture](docs/design/nexuskv-architecture.md)
- **Roadmap:** [NexusKV Roadmap](docs/roadmap.md) (All Phases 1–4 Completed)
- **Implementation status:** [Migration Status](docs/architecture/migration-status.md)
- **Benchmark contract:** [Benchmark Methodology](docs/benchmarks/benchmark-methodology.md)

## Toolchain And Tests

### 1. Go Control Plane Tests

```bash
GOTOOLCHAIN=go1.25.9 go test ./...
cd go && GOTOOLCHAIN=go1.25.9 go test ./...
```

### 2. Rust Workspace Checks & Tests

```bash
cd rust
cargo fmt --all -- --check
cargo check --workspace --all-targets --locked
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
```

### 3. Python Suite & PyO3 Native Build

```bash
# Build PyO3 native extension
cd rust
cargo rustc -p bindings-py --crate-type cdylib
cd ..

# Run full Python unittest suite (65+ tests)
PYTHONPATH=python python3 -m unittest discover -s python/tests -p "test_*.py"
```

### 4. Benchmark Evidence & Stress Suite

```bash
# Run 3-way strategy comparison & cluster stress test
python3 tools/run_benchmarks.py
```

## Architecture Status

All Phase 1–4 milestones defined in the Whitepaper have been implemented, tested, and validated:
1. **Phase 1: State Contracts & Radix Indexing** — `nxradixtree-core`, Rust/Go/Python schemas.
2. **Phase 2: Cost-Based Reuse Engine & Benchmark Evidence** — Effective Gain calculator, Quota admission policy, 3-strategy runner (`trace.py`, `runner.py`, `metrics.py`).
3. **Phase 3: Zero-Overhead Asynchronous Runtime** — `PrefetchScheduler`, `TransferSessionTracker`, Native FFI Hook Interceptor with <1ms Fail-Open guarantee.
4. **Phase 4: Model State Fabric & Multi-Attention Taxonomy** — DeepSeek MLA, DeepSeek DSA, Kimi KDA descriptors, Go Control-Plane Fabric (Leases, Epochs, GC), and HMAC-SHA256 Multi-Tenant Isolation.
