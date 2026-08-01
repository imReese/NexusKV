# NexusKV

NexusKV is being evolved from a small Go prototype into a Model State
Infrastructure platform for LLM inference. The target system separates Control
Plane, Data Plane, Intelligence Layer, and engine-adapter concerns across Go,
Rust, and Python rather than treating the KV Cache as a single monolithic
service.

## Repository Status

- `cmd/` and `pkg/` contain the original Go prototype. It is a baseline
  reference, not the long-term architecture.
- `go/` contains the new Control Plane scaffold.
- `rust/` contains the Data Plane contracts, concrete Host DRAM payload store,
  and state/index implementation.
- `python/` contains engine-facing adapters and compatibility layers.
- `docs/` contains the migration, architecture, benchmark, and reliability
  documents that define the next phases.

## Start Here

- **Flagship paper:** [NexusKV Whitepaper v1.0 — Beyond KV Cache: Toward a
  Zero-Overhead Model State Intelligence Layer for LLM
  Inference](docs/papers/beyond-kv-cache.md)
- [docs/architecture/repo-assessment.md](docs/architecture/repo-assessment.md)
- [docs/architecture/target-platform.md](docs/architecture/target-platform.md)
- [docs/design/attention-state-descriptor.md](docs/design/attention-state-descriptor.md)
- [docs/design/nxradixtree.md](docs/design/nxradixtree.md)
- [docs/design/execution-boundary.md](docs/design/execution-boundary.md)
- [docs/benchmarks/benchmark-methodology.md](docs/benchmarks/benchmark-methodology.md)
- [docs/ops/reliability-model.md](docs/ops/reliability-model.md)

## Toolchain And Tests

Go Control Plane and legacy baseline tests currently use Go `1.25.9`:

```bash
GOTOOLCHAIN=go1.25.9 go test ./...
cd go && GOTOOLCHAIN=go1.25.9 go test ./...
```

The Rust workspace includes:

- `nexus-transfer`: validated, device-neutral metadata for runtime-owned KV
  memory that a transfer backend can register.
- `nexus-store`: a bounded Host DRAM KV payload store with exact entry identity
  isolation and LRU eviction.
- `nxradixtree-core`: exact and prefix reuse lookup.

Run its checks from `rust/`:

```bash
cargo fmt --all -- --check
cargo check --workspace --all-targets --locked
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
```

## Near-Term Direction

The first migration milestone does not try to replace the legacy prototype in one step. It establishes:

1. Versioned state descriptors for multiple attention/state types.
2. A Rust-first `nxradixtree` core for exact and prefix reuse planning.
3. Python connector surfaces for SGLang and vLLM.
4. A new Go Control Plane scaffold for health, admin, and policy-facing services.
5. Test and benchmark scaffolding that can grow with the platform.
