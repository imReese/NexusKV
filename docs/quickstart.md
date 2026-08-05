# 🚀 NexusKV Out-of-the-Box Quickstart Guide

NexusKV is a high-performance **Model State Intelligence Layer and KV Cache Storage Engine** for LLM inference clusters.

---

## Quickstart & Build

```bash
# Clone repository
git clone https://github.com/imReese/NexusKV.git
cd NexusKV

# Build Go controlplane and Rust native extension
make build

# Build Python .whl package
make wheel

# Run tests
make test

# Run benchmark suite
make bench
```

## Deployment Topologies

1. **In-Process Python Library**: `pip install dist/*.whl` and attach `NexusKVPagedAttentionConnector.attach(vllm_engine)`.
2. **Sidecar Process**: Run `./bin/nexuskv-sidecar --listen=unix:///tmp/nexuskv.sock`.
3. **Go Distributed Cluster**: Run `./bin/nexuskv-controlplane --config=config/controlplane.yaml`.
