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
