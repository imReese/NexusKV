from __future__ import annotations

import unittest

from nexuskv.adapters.state import (
    EngineFamily,
    StateSemanticType,
    TensorRole,
    create_sparse_indexed_descriptor,
    validate_descriptor,
)


class TestUniversalPrimitives(unittest.TestCase):
    def test_universal_sparse_indexed_primitive_validation(self) -> None:
        descriptor = create_sparse_indexed_descriptor(
            descriptor_id="universal_sparse_001",
            quant_bits=4,
            group_size=32,
            engine_family=EngineFamily.VLLM,
        )

        # Ensure validation passes
        validate_descriptor(descriptor)

        # Verify universal primitives topology
        self.assertEqual(descriptor.semantic_type, StateSemanticType.SPARSE_INDEXED_STATE)
        self.assertEqual(descriptor.quantization.bits, 4)
        self.assertEqual(descriptor.quantization.group_size, 32)

        # Verify scale tensor role presence
        roles = {spec.role for spec in descriptor.tensor_specs}
        self.assertIn(TensorRole.SCALE_TENSOR, roles)
        self.assertIn(TensorRole.LATENT, roles)
        self.assertIn(TensorRole.AUXILIARY, roles)

    def test_fp8_block_quantized_primitive(self) -> None:
        descriptor = create_sparse_indexed_descriptor(
            descriptor_id="universal_fp8_002",
            quant_bits=8,
            group_size=64,
            engine_family=EngineFamily.SGLANG,
        )

        validate_descriptor(descriptor)
        self.assertEqual(descriptor.quantization.bits, 8)
        self.assertEqual(descriptor.quantization.group_size, 64)


if __name__ == "__main__":
    unittest.main()
