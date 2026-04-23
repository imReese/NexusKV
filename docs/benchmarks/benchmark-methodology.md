# Benchmark Methodology

## Objectives

All performance claims for NexusKV must be backed by executable benchmark code or by an explicit TODO attached to a missing harness.

## First benchmark categories

- exact-hit lookup latency
- prefix-hit lookup latency
- mixed hit/miss lookup latency
- connector translation overhead
- warm-start effectiveness
- degraded-mode behavior under synthetic remote tier failure

## Metrics to report

- p50, p95, p99 lookup latency
- p50, p95, p99 fetch latency
- TTFT delta on hit vs miss
- CPU overhead per request
- memory overhead per cached unit
- fairness under multi-tenant contention

## First scaffold

- `rust/crates/nxradixtree-core`: unit-testable lookup core
- `tests/bench-smoke/`: smoke benchmark cases and workload notes
- future:
  - realistic serving traces
  - connector-integrated benchmark drivers
  - tier-failure injection
