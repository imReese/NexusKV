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

- **Whitepaper:** [NexusKV Whitepaper v1.1 — Beyond KV Cache: Toward a
  Zero-Overhead Model State Intelligence Layer for LLM
  Inference](docs/papers/beyond-kv-cache.md)
- **Architecture:** [NexusKV Architecture](docs/design/nexuskv-architecture.md)
- **Roadmap:** [NexusKV Roadmap](docs/roadmap.md)
- **Implementation status:** [Migration Status](docs/architecture/migration-status.md)
- **Benchmark contract:** [Benchmark Methodology](docs/benchmarks/benchmark-methodology.md)

## Supporting Design Documents

- [docs/architecture/repo-assessment.md](docs/architecture/repo-assessment.md)
- [docs/architecture/target-platform.md](docs/architecture/target-platform.md)
- [docs/design/attention-state-descriptor.md](docs/design/attention-state-descriptor.md)
- [docs/design/nxradixtree.md](docs/design/nxradixtree.md)
- [docs/design/execution-boundary.md](docs/design/execution-boundary.md)
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

## Development Direction

Development is governed by the Whitepaper, Architecture, and Roadmap rather
than an open-ended feature list. The current direction is Phase 2: cost-based
reuse planning, admission policy, and benchmark evidence over the existing
State Contract and deterministic execution boundary.

Every feature must identify its owning architecture layer, the problem it
addresses, its benchmark or validation method, and whether it changes Model
State compatibility.
