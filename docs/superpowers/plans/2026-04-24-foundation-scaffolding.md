# Foundation Scaffolding Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Establish the multi-language NexusKV foundation without destabilizing the legacy Go prototype.

**Architecture:** Add the new target layout in parallel with the existing root Go module. Keep the new control plane, data plane, and engine adapters isolated so later PRs can add real behavior behind stable contracts.

**Tech Stack:** Go 1.25.9, Rust 1.95, Python 3.12, stdlib-first scaffolding

---

### Task 1: Document the migration baseline

**Files:**
- Create: `docs/architecture/repo-assessment.md`
- Create: `docs/architecture/target-platform.md`
- Create: `docs/design/attention-state-descriptor.md`
- Create: `docs/design/nxradixtree.md`
- Create: `docs/benchmarks/benchmark-methodology.md`
- Create: `docs/ops/reliability-model.md`
- Modify: `README.md`

- [ ] Capture current repo findings and target architecture.
- [ ] Record the first milestone and PR sequence.
- [ ] Point the root README to the new docs.

### Task 2: Add Python adapter scaffolding

**Files:**
- Create: `python/nexuskv/__init__.py`
- Create: `python/nexuskv/adapters/state.py`
- Create: `python/nexuskv/connectors/base.py`
- Create: `python/nexuskv/connectors/sglang/__init__.py`
- Create: `python/nexuskv/connectors/sglang/connector.py`
- Create: `python/nexuskv/connectors/vllm/__init__.py`
- Create: `python/nexuskv/connectors/vllm/connector.py`
- Modify: `python/tests/test_state.py`
- Modify: `python/tests/test_connectors.py`

- [ ] Implement the descriptor model and validation.
- [ ] Implement connector skeletons for SGLang and vLLM.
- [ ] Run `env PYTHONPATH=python python3 -m unittest discover -s python/tests -v`.

### Task 3: Add Rust state and radix tree scaffolding

**Files:**
- Modify: `rust/Cargo.toml`
- Modify: `rust/crates/nexus-state/src/lib.rs`
- Modify: `rust/crates/nxradixtree-core/src/lib.rs`
- Modify: `rust/crates/nexus-state/tests/descriptor.rs`
- Modify: `rust/crates/nxradixtree-core/tests/tree.rs`

- [ ] Implement descriptor validation in `nexus-state`.
- [ ] Implement exact and prefix lookup in `nxradixtree-core`.
- [ ] Run `cargo test -p nexus-state -p nxradixtree-core`.

### Task 4: Add Go control-plane scaffold

**Files:**
- Modify: `go/go.mod`
- Create: `go/cmd/nexuskv-controlplane/main.go`
- Create: `go/controlplane/app/app.go`
- Create: `go/config/config.go`
- Modify: `go/controlplane/app/app_test.go`
- Modify: `go/config/config_test.go`

- [ ] Implement default control-plane config.
- [ ] Implement a tiny admin/health HTTP surface.
- [ ] Run `GOTOOLCHAIN=go1.25.9 go test ./...` in `go/`.

### Task 5: Add test and benchmark scaffold directories

**Files:**
- Create: `tests/README.md`
- Create: `tests/integration/README.md`
- Create: `tests/e2e/README.md`
- Create: `tests/fault/README.md`
- Create: `tests/compat/README.md`
- Create: `tests/bench-smoke/README.md`

- [ ] Document the intent of each test lane.
- [ ] Keep benchmark expectations explicit and falsifiable.
