# Attention State Descriptor

## Goal

Represent reusable inference state without assuming the system only stores classic MHA key/value tensors.

## Design direction

The descriptor is the minimum stable contract shared across Rust hot-path code and Python engine adapters. It must describe what a cached state means before deciding how it is stored.

### Core fields

- `descriptor_id`: stable identifier for the state shape/layout contract
- `semantic_type`: MHA KV, GQA KV, MQA KV, MLA state, or future generalized containers
- `engine_family`: SGLang, vLLM, or future engines
- `granularity`: token, block, page, segment
- `tensor_specs`: named role-bearing state members
- future:
  - quantization metadata
  - layout/packing metadata
  - compatibility constraints
  - merge/split rules
  - serialization version

### Why this shape

- The control plane needs to understand compatibility without owning the hot path.
- Python adapters need a safe translation target for engine-specific state.
- Rust needs a validation boundary before page/block pooling and transfer logic.

### Alternatives considered

1. Engine-specific descriptors only
   - Rejected because it would lock the system around SGLang and vLLM assumptions.
2. One opaque blob schema
   - Rejected because operability, compatibility checks, and partial materialization would become guesswork.
3. Fully normalized tensor algebra model from day one
   - Deferred because it adds complexity before the repository has a stable reuse path.

### Deferred items

- conversion graphs between semantic types
- quantized layout metadata
- partial merge planning
- version negotiation between Rust and Python
