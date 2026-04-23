import unittest

from nexuskv.adapters.state import (
    AttentionStateDescriptor,
    CompatibilityFlag,
    EngineFamily,
    Granularity,
    LayoutMetadata,
    MaterializationCapability,
    MaterializationProfile,
    QuantizationMetadata,
    SCHEMA_VERSION,
    StateSemanticType,
    TensorRole,
    TensorSpec,
    TransferBackend,
    TransferCapability,
    TransferPath,
    validate_descriptor,
    supports_partial_materialization,
)


class AttentionStateDescriptorTest(unittest.TestCase):
    def test_descriptor_rejects_duplicate_roles(self) -> None:
        with self.assertRaises(ValueError):
            validate_descriptor(AttentionStateDescriptor(
                schema_version=SCHEMA_VERSION,
                descriptor_id="mha-invalid",
                semantic_type=StateSemanticType.MHA_KV,
                engine_family=EngineFamily.SGLANG,
                granularity=Granularity.TOKEN,
                tensor_specs=[
                    TensorSpec(name="k_cache", role=TensorRole.KEY, dtype="float16", shape=["layers", "tokens", "heads", "head_dim"]),
                    TensorSpec(name="v_cache_duplicate", role=TensorRole.KEY, dtype="float16", shape=["layers", "tokens", "heads", "head_dim"]),
                ],
                quantization=QuantizationMetadata(scheme="none", bits=16, group_size=0),
                layout=LayoutMetadata(layout="contiguous", page_tokens=1, block_tokens=1, packed=False),
                compatibility_flags=[CompatibilityFlag.EXACT_REUSE],
                transfer_paths=[TransferPath(backend=TransferBackend.BASELINE_TRANSPORT, capabilities=[TransferCapability.HOST_TO_STORE])],
                materialization=MaterializationProfile(capabilities=[MaterializationCapability.FULL], tier_kinds=[], device_classes=[], buffer_kinds=[]),
                layout_metadata={},
            ))

    def test_descriptor_supports_partial_materialization_for_paged_layouts(self) -> None:
        descriptor = AttentionStateDescriptor(
            schema_version=SCHEMA_VERSION,
            descriptor_id="paged-gqa",
            semantic_type=StateSemanticType.GQA_KV,
            engine_family=EngineFamily.VLLM,
            granularity=Granularity.PAGE,
            tensor_specs=[
                TensorSpec(name="k_cache", role=TensorRole.KEY, dtype="float16", shape=["layers", "pages", "kv_heads", "page_tokens", "head_dim"]),
                TensorSpec(name="v_cache", role=TensorRole.VALUE, dtype="float16", shape=["layers", "pages", "kv_heads", "page_tokens", "head_dim"]),
            ],
            quantization=QuantizationMetadata(scheme="none", bits=16, group_size=0),
            layout=LayoutMetadata(layout="paged", page_tokens=16, block_tokens=16, packed=True),
            compatibility_flags=[CompatibilityFlag.PAGE_REUSE],
            transfer_paths=[TransferPath(backend=TransferBackend.STAGED_COPY, capabilities=[TransferCapability.ASYNC])],
            materialization=MaterializationProfile(capabilities=[MaterializationCapability.PARTIAL], tier_kinds=[], device_classes=[], buffer_kinds=[]),
            layout_metadata={},
        )

        validate_descriptor(descriptor)
        self.assertTrue(supports_partial_materialization(descriptor))


if __name__ == "__main__":
    unittest.main()
