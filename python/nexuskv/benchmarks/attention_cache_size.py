from __future__ import annotations

from dataclasses import dataclass
from nexuskv.contracts.generated import StateSemanticType


@dataclass(slots=True)
class AttentionCacheConfig:
    name: str
    semantic_type: StateSemanticType
    num_layers: int
    num_heads: int
    num_kv_heads: int
    head_dim: int
    latent_dim: int = 0      # For MLA (DeepSeek-V2/V3/R1)
    rope_dim: int = 0        # For MLA RoPE
    sparsity_ratio: float = 1.0  # For DSA

    def bytes_per_token(self, bytes_per_elem: int = 2) -> int:
        if self.semantic_type == StateSemanticType.MHA_KV:
            # 2 * num_layers * num_heads * head_dim * bytes_per_elem
            return 2 * self.num_layers * self.num_heads * self.head_dim * bytes_per_elem
        
        elif self.semantic_type == StateSemanticType.GQA_KV or self.semantic_type == StateSemanticType.MQA_KV:
            # 2 * num_layers * num_kv_heads * head_dim * bytes_per_elem
            return 2 * self.num_layers * self.num_kv_heads * self.head_dim * bytes_per_elem

        elif self.semantic_type == StateSemanticType.MLA_STATE:
            # num_layers * (latent_dim + rope_dim) * bytes_per_elem
            return self.num_layers * (self.latent_dim + self.rope_dim) * bytes_per_elem

        elif self.semantic_type == StateSemanticType.DSA_STATE:
            # Base GQA/MLA * sparsity_ratio
            base_bytes = 2 * self.num_layers * self.num_kv_heads * self.head_dim * bytes_per_elem
            return int(base_bytes * self.sparsity_ratio)

        return 2 * self.num_layers * self.num_heads * self.head_dim * bytes_per_elem

    def calculate_footprint(self, seq_len: int, bytes_per_elem: int = 2) -> float:
        """Returns physical memory footprint in Megabytes (MB)."""
        total_bytes = self.bytes_per_token(bytes_per_elem) * seq_len
        return total_bytes / (1024 * 1024)


# Predefined Industry Standard Model Attention Profiles
ATTENTION_PROFILES: list[AttentionCacheConfig] = [
    AttentionCacheConfig(
        name="LLaMA-2 70B (MHA)",
        semantic_type=StateSemanticType.MHA_KV,
        num_layers=80,
        num_heads=64,
        num_kv_heads=64,
        head_dim=128,
    ),
    AttentionCacheConfig(
        name="LLaMA-3 70B (GQA 8:1)",
        semantic_type=StateSemanticType.GQA_KV,
        num_layers=80,
        num_heads=64,
        num_kv_heads=8,
        head_dim=128,
    ),
    AttentionCacheConfig(
        name="Qwen-2.5 72B (GQA 8:1)",
        semantic_type=StateSemanticType.GQA_KV,
        num_layers=80,
        num_heads=64,
        num_kv_heads=8,
        head_dim=128,
    ),
    AttentionCacheConfig(
        name="DeepSeek-V3 / R1 (MLA)",
        semantic_type=StateSemanticType.MLA_STATE,
        num_layers=61,
        num_heads=128,
        num_kv_heads=1,
        head_dim=128,
        latent_dim=512,
        rope_dim=64,
    ),
    AttentionCacheConfig(
        name="DeepSeek-V3 Sparse (DSA 10%)",
        semantic_type=StateSemanticType.DSA_STATE,
        num_layers=61,
        num_heads=128,
        num_kv_heads=16,
        head_dim=128,
        sparsity_ratio=0.10,
    ),
]
