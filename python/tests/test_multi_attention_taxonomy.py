import unittest

from nexuskv.adapters.state import (
    create_dsa_descriptor,
    create_kda_descriptor,
    create_mla_descriptor,
    validate_descriptor,
)
from nexuskv.contracts.generated import StateSemanticType, TensorRole


class TestMultiAttentionTaxonomy(unittest.TestCase):
    def test_mla_descriptor_validation_and_structure(self):
        mla = create_mla_descriptor("desc-mla-001")
        validate_descriptor(mla)
        self.assertEqual(mla.semantic_type, StateSemanticType.MLA_STATE)
        self.assertEqual(len(mla.tensor_specs), 2)
        roles = {spec.role for spec in mla.tensor_specs}
        self.assertIn(TensorRole.LATENT, roles)
        self.assertIn(TensorRole.POSITION, roles)
        self.assertEqual(mla.layout_metadata["attention_family"], "MLA")

    def test_dsa_descriptor_validation_and_structure(self):
        dsa = create_dsa_descriptor("desc-dsa-001")
        validate_descriptor(dsa)
        self.assertEqual(dsa.semantic_type, StateSemanticType.DSA_STATE)
        roles = {spec.role for spec in dsa.tensor_specs}
        self.assertIn(TensorRole.KEY, roles)
        self.assertIn(TensorRole.AUXILIARY, roles)

    def test_kda_descriptor_validation_and_structure(self):
        kda = create_kda_descriptor("desc-kda-001")
        validate_descriptor(kda)
        self.assertEqual(kda.semantic_type, StateSemanticType.KDA_CHECKPOINT)
        self.assertEqual(kda.layout_metadata["checkpoint_boundary"], "terminal_recurrent_state")


if __name__ == "__main__":
    unittest.main()
