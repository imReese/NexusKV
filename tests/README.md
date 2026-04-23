# Test Scaffold

This directory holds the cross-language test lanes for the new NexusKV platform.

- `integration/`: connector plus control-plane plus data-plane interaction tests
- `e2e/`: user-visible cache hit/miss and degraded-mode paths
- `fault/`: timeout, corruption, stale metadata, and restart scenarios
- `compat/`: protocol and descriptor compatibility checks
- `bench-smoke/`: executable smoke workloads for latency and reuse regressions
