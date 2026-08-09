# Locus Bridge

## Status

NexusKV ships a reference HTTP service for the versioned
`locus.nexuskv-bridge.v1` integration contract. It exposes lookup, estimate,
and materialize operations backed by the Rust `nxradixtree` matcher, the Python
cost estimator, and the existing execution boundary.

This milestone establishes process separation, contract compatibility,
capability binding, and orchestration behavior. The baseline execution backend
does not move cache bytes into an inference engine. A successful materialize
response therefore reports zero transferred bytes, uses the
`nexuskv.protocol-transfer-receipt.v1` namespace, and sets
`physical_transfer_verified` to `false`.

## Ownership

| Concern | Owner |
| --- | --- |
| Global compute and state placement | Locus planner |
| Match evidence, source location, and transfer estimate | NexusKV bridge |
| Destination allocation and state attachment | Locus engine adapter and inference engine |
| Prepare, materialize, commit, abort, and cold fallback ordering | Locus `PlanExecutor` |
| Physical transport implementation and verified byte count | A future NexusKV native transport backend |

NexusKV options are evidence offered to Locus, not placement authority. Locus
chooses a target and may discard an option or fall back to recompute.

## Contract

The JSON Schema source of truth is
[`schema/locus_nexuskv_bridge.json`](../../schema/locus_nexuskv_bridge.json).
The service exposes:

- `POST /locus/v1/lookup`
- `POST /locus/v1/estimate`
- `POST /locus/v1/materialize`
- `GET /healthz`

Lookup requires canonical token IDs plus the tenant, namespace, immutable model
identity, and Locus input-semantic identity. NexusKV fails closed when the
matched entry lacks identical compatibility evidence. A successful match also
returns an unguessable `source_handle`; estimate must present that handle, so a
state ID alone does not authorize materialization planning.

Estimate validates the source handle and source location, then returns a
short-lived option bound to one Locus target, engine ID, engine generation, and
residency. Materialize requires the opaque option handle and matching target.
The option is consumed only after the execution boundary reports a completed
materialize action. Malformed requests, expired or replayed options, mismatched
targets, and execution rejection return structured errors.

Bearer authentication is optional for local fixtures and should be enabled for
deployment with `--api-key-env`. The reference server has one process-wide
credential; it does not yet map credentials to tenant scopes, implement
per-tenant admission, or replace network-layer isolation. The source capability
reduces state-ID enumeration risk but is not a substitute for tenant-scoped
authorization.

## Running the reference service

Build the Rust planner extension, then start the bridge with registered fixture
state:

```bash
cd rust
cargo rustc -p bindings-py --crate-type cdylib
cd ..
PYTHONPATH=python python -m nexuskv.integrations.locus_bridge \
  --listen 127.0.0.1:9099 \
  --fixture tests/fixtures/locus_bridge/conformance.json
```

On macOS, the PyO3 build additionally needs
`-- -C link-arg=-undefined -C link-arg=dynamic_lookup`.

The fixture schema is `locus.nexuskv-bridge.fixture.v1`. Locus vendors the same
fixture and its cross-repository E2E script rejects any byte-level divergence
before starting the two processes.

## Validation boundary

Automated tests cover strict request validation, bearer authentication,
semantic mismatch, source/option handle tampering, target-generation mismatch,
one-time option consumption, backend rejection, and structured failure. The
cross-repository test starts this service in a separate process with the real
Rust matcher and verifies Locus planning plus prepare/materialize/commit.

The following remain unverified:

- native SGLang or vLLM GPU state import;
- physical Host DRAM, shared-memory, RDMA, or device transfer through this API;
- production cost calibration, throughput, and tail latency;
- multi-replica option/state coordination and tenant-scoped authentication.
