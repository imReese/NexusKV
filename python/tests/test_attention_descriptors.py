"""Unit tests for all 8 Attention State Descriptors in NexusKV."""

from __future__ import annotations

import unittest

from nexuskv.contracts.generated import (
    SCHEMA_VERSION,
    AttentionStateDescriptor,
    BufferKind,
    CacheEntry,
    CompatibilityFlag,
    DeviceClass,
    EngineFamily,
    EntryIdentity,
    EntryLocation,
    EntryVersion,
    Granularity,
    KeyIdentity,
    LayoutMetadata,
    MaterializationCapability,
    MaterializationProfile,
    PolicyHint,
    QuantizationMetadata,
    StateSemanticType,
    TensorRole,
    TensorSpec,
    TierKind,
    TransferBackend,
    TransferCapability,
    TransferPath,
)
from nexuskv.execution.store import InMemoryEntryStore


class TestAttentionStateDescriptors(unittest.TestCase):
    """Test suite verifying all 8 Attention State Descriptors."""

    def setUp(self) -> None:
        self.store = InMemoryEntryStore()

    def _create_descriptor(
        self, semantic_type: StateSemanticType, desc_id: str, specs: list[TensorSpec]
    ) -> AttentionStateDescriptor:
        return AttentionStateDescriptor(
            schema_version=SCHEMA_VERSION,
            descriptor_id=desc_id,
            engine_family=EngineFamily.UNKNOWN,
            semantic_type=semantic_type,
            granularity=Granularity.PAGE,
            tensor_specs=specs,
            quantization=QuantizationMetadata(scheme="none", bits=16, group_size=1),
            layout=LayoutMetadata(
                layout="interleaved", page_tokens=16, block_tokens=64, packed=True
            ),
            compatibility_flags=[CompatibilityFlag.EXACT_REUSE],
            transfer_paths=[
                TransferPath(
                    backend=TransferBackend.BASELINE_TRANSPORT,
                    capabilities=[TransferCapability.HOST_TO_DEVICE],
                )
            ],
            materialization=MaterializationProfile(
                capabilities=[MaterializationCapability.FULL],
                tier_kinds=[TierKind.HOST_DRAM],
                device_classes=[DeviceClass.CPU],
                buffer_kinds=[BufferKind.HOST_PAGEABLE],
            ),
            layout_metadata={},
        )

    def test_1_mha_descriptor(self) -> None:
        """1. MHA/GQA/MQA Standard Attention."""
        desc = self._create_descriptor(
            StateSemanticType.MHA_KV,
            "mha-standard",
            [
                TensorSpec(
                    name="key", role=TensorRole.KEY, dtype="float16", shape=["16", "32", "128"]
                ),
                TensorSpec(
                    name="value", role=TensorRole.VALUE, dtype="float16", shape=["16", "32", "128"]
                ),
            ],
        )
        self.assertEqual(desc.semantic_type, StateSemanticType.MHA_KV)
        self.assertEqual(len(desc.tensor_specs), 2)

    def test_2_deepseek_mla_descriptor(self) -> None:
        """2. DeepSeek MLA (Multi-Head Latent Attention)."""
        desc = self._create_descriptor(
            StateSemanticType.MLA_STATE,
            "deepseek-v3-mla",
            [
                TensorSpec(
                    name="latent_kv", role=TensorRole.LATENT, dtype="float16", shape=["16", "512"]
                ),
                TensorSpec(
                    name="rope_k", role=TensorRole.POSITION, dtype="float16", shape=["16", "64"]
                ),
            ],
        )
        self.assertEqual(desc.semantic_type, StateSemanticType.MLA_STATE)
        self.assertEqual(desc.tensor_specs[0].role, TensorRole.LATENT)
        self.assertEqual(desc.tensor_specs[1].role, TensorRole.POSITION)

    def test_3_deepseek_dsa_descriptor(self) -> None:
        """3. DeepSeek DSA (DeepSeek Sparse Attention)."""
        desc = self._create_descriptor(
            StateSemanticType.DSA_STATE,
            "deepseek-dsa-sparse",
            [
                TensorSpec(
                    name="sparse_data",
                    role=TensorRole.KEY,
                    dtype="float16",
                    shape=["4", "16", "128"],
                ),
                TensorSpec(
                    name="sparse_indices", role=TensorRole.AUXILIARY, dtype="int32", shape=["4"]
                ),
            ],
        )
        self.assertEqual(desc.semantic_type, StateSemanticType.DSA_STATE)

    def test_4_kimi_kda_descriptor(self) -> None:
        """4. Kimi KDA (Recurrent Terminal Checkpoints)."""
        desc = self._create_descriptor(
            StateSemanticType.KDA_CHECKPOINT,
            "kimi-k3-kda-checkpoint",
            [
                TensorSpec(
                    name="recurrent_state",
                    role=TensorRole.LATENT,
                    dtype="float32",
                    shape=["128", "128"],
                ),
            ],
        )
        self.assertEqual(desc.semantic_type, StateSemanticType.KDA_CHECKPOINT)

    def test_5_mamba2_ssm_descriptor(self) -> None:
        """5. Mamba2 / Selective SSM Checkpoint."""
        desc = self._create_descriptor(
            StateSemanticType.GENERIC_CONTAINER,
            "mamba2-ssm-checkpoint",
            [
                TensorSpec(
                    name="ssm_state",
                    role=TensorRole.LATENT,
                    dtype="float32",
                    shape=["8", "64", "16"],
                ),
                TensorSpec(
                    name="conv_state",
                    role=TensorRole.AUXILIARY,
                    dtype="float16",
                    shape=["8", "128", "4"],
                ),
            ],
        )
        self.assertEqual(desc.descriptor_id, "mamba2-ssm-checkpoint")

    def test_6_deepseek_nsa_descriptor(self) -> None:
        """6. DeepSeek NSA (Native Sparse Attention)."""
        desc = self._create_descriptor(
            StateSemanticType.CSA_STATE,
            "deepseek-nsa-summary",
            [
                TensorSpec(
                    name="summary_blocks", role=TensorRole.KEY, dtype="float16", shape=["8", "128"]
                ),
                TensorSpec(
                    name="selected_blocks",
                    role=TensorRole.VALUE,
                    dtype="float16",
                    shape=["4", "16", "128"],
                ),
            ],
        )
        self.assertEqual(desc.semantic_type, StateSemanticType.CSA_STATE)

    def test_7_multimodal_vision_descriptor(self) -> None:
        """7. Multimodal Audio/Video Vision KV Cache."""
        desc = self._create_descriptor(
            StateSemanticType.MULTIMODAL_VISION,
            "qwen2-vl-vision-kv",
            [
                TensorSpec(
                    name="spatiotemporal_key",
                    role=TensorRole.KEY,
                    dtype="float16",
                    shape=["8", "24", "24", "128"],
                ),
                TensorSpec(
                    name="spatiotemporal_val",
                    role=TensorRole.VALUE,
                    dtype="float16",
                    shape=["8", "24", "24", "128"],
                ),
            ],
        )
        self.assertEqual(desc.semantic_type, StateSemanticType.MULTIMODAL_VISION)

    def test_8_agent_tot_cow_descriptor(self) -> None:
        """8. Agentic ToT / MCTS Multi-Branch CoW."""
        desc = self._create_descriptor(
            StateSemanticType.AGENT_TOT_COW,
            "agent-mcts-cow-branch",
            [
                TensorSpec(
                    name="shared_prefix", role=TensorRole.KEY, dtype="float16", shape=["16", "64"]
                ),
            ],
        )
        self.assertEqual(desc.semantic_type, StateSemanticType.AGENT_TOT_COW)

        # Verify End-to-End Store Put/Get for Agent ToT CoW Entry
        key = KeyIdentity(
            tenant="agent_tenant",
            namespace="mcts_ns",
            model="qwen-72b",
            engine_family=EngineFamily.UNKNOWN,
            semantic_type=StateSemanticType.AGENT_TOT_COW,
            tokens=[1, 2, 3],
            block_id=None,
            page_id=None,
        )
        entry = CacheEntry(
            identity=EntryIdentity(
                key=key,
                entry_id="tot_branch_001",
                version=EntryVersion(generation=1, lineage="cow_branch"),
            ),
            descriptor=desc,
            location=EntryLocation(tier=TierKind.HOST_DRAM, locator="memory://tot_branch_001"),
            policy_hint=PolicyHint(
                reusable=True, admission_hint="cow_shared", eviction_hint="pinned"
            ),
        )
        self.store.put(entry, b"agent_tot_payload")
        retrieved = self.store.get_identity(key)
        self.assertIsNotNone(retrieved)
        self.assertEqual(retrieved.entry.identity.entry_id, "tot_branch_001")


if __name__ == "__main__":
    unittest.main()
