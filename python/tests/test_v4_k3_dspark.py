import unittest

from nexuskv.adapters.state import (
    StateSemanticType,
    create_csa_descriptor,
    create_dspark_descriptor,
    create_hca_descriptor,
    validate_descriptor,
)
from nexuskv.execution.recurrent_cascade import RecurrentStateCascadeEngine


class TestV4K3DSpark(unittest.TestCase):
    def test_descriptors_validation(self):
        csa = create_csa_descriptor("desc_csa_1")
        self.assertEqual(csa.semantic_type, StateSemanticType.CSA_STATE)
        validate_descriptor(csa)

        hca = create_hca_descriptor("desc_hca_1")
        self.assertEqual(hca.semantic_type, StateSemanticType.HCA_SUMMARY)
        validate_descriptor(hca)

        dspark = create_dspark_descriptor("desc_dspark_1")
        self.assertEqual(dspark.semantic_type, StateSemanticType.DSPARK_SPARSE)
        validate_descriptor(dspark)

    def test_recurrent_state_cascade_engine(self):
        engine = RecurrentStateCascadeEngine()
        handle = engine.mount_k3_recurrent_checkpoint("session_001", b"recurrent_checkpoint_data")
        self.assertTrue(handle.is_pinned)

        engine.stage_history_context_to_host("session_001", b"history_chunk_1")
        res = engine.cascade_incremental_restore("session_001")
        self.assertTrue(res["checkpoint_mounted"])
        self.assertEqual(res["history_chunks"], 1)


if __name__ == "__main__":
    unittest.main()
