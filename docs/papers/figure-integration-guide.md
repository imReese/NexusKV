# Figure Integration Guide

The whitepaper figures should be embedded as part of the main narrative rather than treated as standalone assets.

## Figure 1: KV Cache Evolution

Place after the introduction.

Purpose:

Show the evolution:

```
KV Buffer
   |
Paged KV Cache
   |
Hierarchical Cache
   |
Distributed KV Fabric
   |
Model State Intelligence
```

Message:

The industry evolution is moving from memory optimization toward intelligent state management.

---

## Figure 2: System Landscape

Place after the current system comparison section.

Recommended axes:

```
                Intelligence
                    ^
                    |
                    |
Mooncake            NexusKV
                    |
                    |
Storage ------------ Runtime
                    |
                    |
                 vLLM
```

Message:

Existing systems optimize different layers rather than competing directly.

---

## Figure 3: NexusKV Architecture

Place in the architecture proposal section.

Structure:

```
Inference Engines
       |
       v
NexusKV Intelligence Layer
       |
+------+------+
|             |
Planner    Prefetch
       |
       v
Storage Fabric
GPU / Host / Remote
```

Message:

Cache operations should disappear behind computation.

---

## Figure 4: Zero-Overhead Pipeline

Recommended addition for future versions:

```
GPU Compute
====================

       Async KV Transfer
       ====================
```

Message:

The critical path should contain computation, not cache movement.
