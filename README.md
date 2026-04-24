# NexusKV

NexusKV is being evolved from a small Go prototype into a production-grade KV cache platform for LLM inference. The target system separates control-plane, data-plane, and engine-adapter concerns across Go, Rust, and Python rather than treating the cache as a single monolithic service.

## Repository Status

- `cmd/` and `pkg/` contain the original Go prototype. It is useful as a baseline reference, but it is not the long-term architecture.
- `go/` contains the new control-plane scaffold.
- `rust/` contains the new data-plane and state/index scaffolds.
- `python/` contains engine-facing adapters and compatibility layers.
- `docs/` contains the migration, architecture, benchmark, and reliability documents that define the next phases.

## Start Here

- [docs/architecture/repo-assessment.md](docs/architecture/repo-assessment.md)
- [docs/architecture/target-platform.md](docs/architecture/target-platform.md)
- [docs/design/attention-state-descriptor.md](docs/design/attention-state-descriptor.md)
- [docs/design/nxradixtree.md](docs/design/nxradixtree.md)
- [docs/design/execution-boundary.md](docs/design/execution-boundary.md)
- [docs/benchmarks/benchmark-methodology.md](docs/benchmarks/benchmark-methodology.md)
- [docs/ops/reliability-model.md](docs/ops/reliability-model.md)

## Near-Term Direction

The first migration milestone does not try to replace the legacy prototype in one step. It establishes:

1. Versioned state descriptors for multiple attention/state types.
2. A Rust-first `nxradixtree` core for exact and prefix reuse planning.
3. Python connector surfaces for SGLang and vLLM.
4. A new Go control-plane scaffold for health, admin, and policy-facing services.
5. Test and benchmark scaffolding that can grow with the platform.
