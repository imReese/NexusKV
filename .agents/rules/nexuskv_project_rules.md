# NexusKV Project Rules & Conventions

## 1. Virtual Environment & Python Environment
- Python virtual environment is managed via **`pyenv`** with the environment name **`nexuskv-env`** (bound locally via `.python-version`).
- Run python commands directly with `python3 <script>` without sourcing `.venv`.

## 2. Git Strategy & File Naming
- Local development takes place directly on the `main` branch.
- Push changes directly to `origin/main` without PR workflows unless explicitly asked.
- **Never** append version suffixes like `v1`, `v2` to filenames during active feature development.
- Keep Git commit history clean: never commit build artifacts (`.so`, `.dylib`, `.pyd`, `.dll`, `target/`, `__pycache__`).

## 3. GitHub Actions CI Compliance
- Pushing code to GitHub is **not** the end of a task.
- Continuously monitor GitHub Actions workflow runs (via `gh run view` / `gh run list`) after pushing until all matrix jobs pass 100% GREEN (`✓`).
- If CI fails (e.g. `clippy` or `fmt` or test failure), immediately diagnose and fix the root cause.

## 4. Benchmark & Metrics Conventions
- Benchmarks (`tools/run_benchmarks.py`) report dual dimensions:
  1. **Control Decision Rate**: QPS/RPS and real Wall-Clock P50/P90/P99 latency in microseconds.
  2. **Payload Capacity Saved**: Saved KV Tensor GBs and GB/sec equivalent compute offload.
- Benchmark timings use real nanosecond wall-clock metrics (`time.perf_counter_ns()`).

## 5. Modern Technology Stack Standards (Zero Legacy Code Rule)
- **Go Standards (Go 1.22+)**:
  - **NEVER** use legacy `interface{}` in Go code. **ALWAYS** use `any` or strict type generic constraints (`[T any]`).
- **C++ Standards (C++17 Production Standard)**:
  - Standardize on `-std=c++17` for 100% PyTorch and CUDA NVCC compatibility.
  - Use portable hardware cache-line alignment probes to eliminate CPU False Sharing.
  - Use atomic lock-free MPMC ring buffers and POSIX `mlock` page pinning.
- **Rust Standards (Rust 2021 Edition Stable)**:
  - Enforce Rust 2021 Edition with zero unnecessary `unsafe` blocks.
- **Python Standards (Python 3.10 ~ 3.13)**:
  - Use PEP 563 `from __future__ import annotations`, PEP 544 `Protocol`, and built-in type hints (`list`, `dict`, `tuple`, `X | None`).
