# 🚀 NexusKV Production Quickstart Guide

NexusKV is a high-performance **Universal Model State Intelligence and Distributed Memory Fabric** for LLM inference clusters.

---

## 1. Quickstart & Build

```bash
# Clone repository
git clone https://github.com/imReese/NexusKV.git
cd NexusKV

# Build Go control plane and Rust native extension
make build

# Package & install Python plugin wheel
make wheel
pip install dist/nexuskv_planner_native-*.whl
```

---

## 2. Production Serving Launch (Standard CLI Serving)

In production deployments, inference servers are launched via standard CLI entry points (`vllm serve` or `sglang serve`).

### Option A: `vllm serve` Integration

```bash
# Auto-enable NexusKV state intelligence plugin
export VLLM_PLUGINS=nexuskv

# Launch vLLM production API server
vllm serve Qwen/Qwen2.5-72B-Instruct \
  --host 0.0.0.0 \
  --port 8000 \
  --kv-transfer-config '{"kv_connector": "NexusKVConnector", "kv_role": "kv_both"}'
```

---

### Option B: `sglang serve` Integration

```bash
# Launch SGLang server with NexusKV connector
python3 -m sglang.launch_server \
  --model-path Qwen/Qwen2.5-72B-Instruct \
  --host 0.0.0.0 \
  --port 30000 \
  --kv-cache-connector nexuskv
```

---

### Option C: Programmatic Python SDK Integration

For custom Python gateways or proprietary serving frameworks:

```python
import vllm
from nexuskv.connectors.vllm import NexusKVPagedAttentionConnector

# Initialize engine
engine = vllm.LLMEngine.from_engine_args(engine_args)

# Attach NexusKV connector
engine = NexusKVPagedAttentionConnector.attach(engine)
```

---

## 3. Distributed Control Plane & Sidecar Topologies

1. **Sidecar Process**: Run `./bin/nexuskv-sidecar --listen=unix:///tmp/nexuskv.sock`.
2. **Go Distributed Cluster**: Run `./bin/nexuskv-controlplane --config=config/controlplane.yaml`.
