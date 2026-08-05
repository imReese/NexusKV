from __future__ import annotations

import random
from dataclasses import dataclass

from nexuskv.contracts.generated import TierKind


@dataclass(slots=True)
class TraceRequest:
    request_id: str
    tenant: str
    namespace: str
    model: str
    tokens: list[int]
    shared_prefix_len: int
    context_length: int
    reuse_ratio: float
    tier_source: TierKind


@dataclass(slots=True)
class WorkloadTrace:
    name: str
    requests: list[TraceRequest]

    def __len__(self) -> int:
        return len(self.requests)


@dataclass(slots=True)
class BenchmarkTraceGenerator:
    seed: int = 42

    def generate_synthetic_trace(
        self,
        name: str = "synthetic_mixed_workload",
        num_requests: int = 20,
        context_lengths: tuple[int, ...] = (8192, 32768, 131072),
        reuse_ratios: tuple[float, ...] = (0.0, 0.5, 0.9, 1.0),
        tier_distribution: tuple[TierKind, ...] = (
            TierKind.HOST_DRAM,
            TierKind.REMOTE_SHARED,
            TierKind.LOCAL_SSD,
        ),
        tenant: str = "default_tenant",
        namespace: str = "default_ns",
        model: str = "llama-3-70b",
    ) -> WorkloadTrace:
        rng = random.Random(self.seed)
        requests: list[TraceRequest] = []
        base_prefix = [rng.randint(100, 50000) for _ in range(1024)]

        for i in range(num_requests):
            ctx_len = rng.choice(context_lengths)
            reuse_ratio = rng.choice(reuse_ratios)
            prefix_len = int(ctx_len * reuse_ratio)
            prefix_len = min(prefix_len, len(base_prefix))

            # Build token array
            prefix_tokens = base_prefix[:prefix_len]
            suffix_len = max(0, ctx_len - prefix_len)
            suffix_tokens = [rng.randint(100, 50000) for _ in range(suffix_len)]
            tokens = prefix_tokens + suffix_tokens

            tier = rng.choice(tier_distribution)

            req = TraceRequest(
                request_id=f"req_{i:04d}",
                tenant=tenant,
                namespace=namespace,
                model=model,
                tokens=tokens,
                shared_prefix_len=prefix_len,
                context_length=len(tokens),
                reuse_ratio=reuse_ratio,
                tier_source=tier,
            )
            requests.append(req)

        return WorkloadTrace(name=name, requests=requests)

    def generate_multi_size_matrix_traces(self) -> dict[str, WorkloadTrace]:
        """Generate specialized workload traces across Small, Medium, and Large KV Tensor Payload sizes."""
        tiers = {
            "Small (512 - 2,048 Tokens / 128KB - 512KB)": (512, 1024, 2048),
            "Medium (4,096 - 16,384 Tokens / 1MB - 4MB)": (4096, 8192, 16384),
            "Large (32,768 - 131,072 Tokens / 8MB - 32MB)": (32768, 65536, 131072),
        }
        result = {}
        for label, lengths in tiers.items():
            result[label] = self.generate_synthetic_trace(
                name=f"trace_{label.split()[0].lower()}",
                num_requests=20,
                context_lengths=lengths,
            )
        return result
