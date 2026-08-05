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
    latent_dim: int = 0  # For MLA (DeepSeek-V2/V3/R1 & Kimi K3)
    rope_dim: int = 0  # For MLA RoPE
    sparsity_ratio: float = 1.0  # For DSA / CSA / HCA
    kda_fixed_state_dim: int = 0  # For Kimi K3 KDA linear recurrent layers

    def bytes_per_token(self, bytes_per_elem: int = 2) -> int:
        if self.semantic_type == StateSemanticType.MHA_KV:
            return 2 * self.num_layers * self.num_heads * self.head_dim * bytes_per_elem

        elif self.semantic_type in (StateSemanticType.GQA_KV, StateSemanticType.MQA_KV):
            return 2 * self.num_layers * self.num_kv_heads * self.head_dim * bytes_per_elem

        elif self.semantic_type == StateSemanticType.MLA_STATE:
            return self.num_layers * (self.latent_dim + self.rope_dim) * bytes_per_elem

        elif self.semantic_type == StateSemanticType.KDA_CHECKPOINT:
            # Kimi K3: 69 KDA linear layers (fixed recurrent state) + 24 Gated MLA layers
            kda_layers = 69
            mla_layers = 24
            kda_bytes = kda_layers * self.kda_fixed_state_dim * bytes_per_elem
            mla_bytes = mla_layers * (self.latent_dim + self.rope_dim) * bytes_per_elem
            return kda_bytes + mla_bytes

        elif self.semantic_type in (StateSemanticType.CSA_STATE, StateSemanticType.HCA_SUMMARY):
            # DeepSeek V4: Compressed Sparse + Heavily Compressed Attention (~90% memory reduction)
            base_mla_bytes = self.num_layers * (self.latent_dim + self.rope_dim) * bytes_per_elem
            return int(base_mla_bytes * self.sparsity_ratio)

        elif self.semantic_type == StateSemanticType.DSA_STATE:
            base_bytes = 2 * self.num_layers * self.num_kv_heads * self.head_dim * bytes_per_elem
            return int(base_bytes * self.sparsity_ratio)

        return 2 * self.num_layers * self.num_heads * self.head_dim * bytes_per_elem

    def calculate_footprint(self, seq_len: int, bytes_per_elem: int = 2) -> float:
        """Returns physical memory footprint in Megabytes (MB)."""
        total_bytes = self.bytes_per_token(bytes_per_elem) * seq_len
        return total_bytes / (1024 * 1024)


# Predefined Frontier Model Attention Profiles (Up-to-Date 2026 Standards)
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
        name="Kimi K3 (KDA + Gated MLA)",
        semantic_type=StateSemanticType.KDA_CHECKPOINT,
        num_layers=93,
        num_heads=128,
        num_kv_heads=1,
        head_dim=128,
        latent_dim=512,
        rope_dim=64,
        kda_fixed_state_dim=128,
    ),
    AttentionCacheConfig(
        name="DeepSeek V4 (CSA + HCA Hybrid)",
        semantic_type=StateSemanticType.CSA_STATE,
        num_layers=64,
        num_heads=128,
        num_kv_heads=1,
        head_dim=128,
        latent_dim=512,
        rope_dim=64,
        sparsity_ratio=0.10,  # 90% memory reduction
    ),
]
