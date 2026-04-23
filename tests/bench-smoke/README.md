# Benchmark Smoke Tests

These are lightweight benchmark lanes used to catch regressions early. The first expected cases are:

- exact radix-tree hit lookup
- prefix radix-tree hit lookup
- mixed hit/miss lookup
- connector translation overhead
- degraded-mode fallback overhead
