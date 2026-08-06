from __future__ import annotations

import unittest

from nexuskv.execution.rubin_handshake import (
    RubinCPXPrefillHandshake,
    RubinHandshakeStatus,
)


class TestRubinCPXPrefillHandshake(unittest.TestCase):
    def test_rubin_handshake_lifecycle(self) -> None:
        handshake = RubinCPXPrefillHandshake()
        tokens = list(range(512))

        session = handshake.initiate_handshake(
            session_id="rubin_sess_001",
            prefill_node_id="rubin_cpx_node_01",
            decode_node_id="rubin_hbm4_node_02",
            prompt_tokens=tokens,
        )

        self.assertEqual(session.session_id, "rubin_sess_001")
        self.assertEqual(session.status, RubinHandshakeStatus.ACKNOWLEDGED)
        self.assertEqual(session.token_count, 512)

        # Complete handshake
        success = handshake.complete_handshake("rubin_sess_001")
        self.assertTrue(success)

        retrieved = handshake.get_session("rubin_sess_001")
        self.assertIsNotNone(retrieved)
        self.assertEqual(retrieved.status, RubinHandshakeStatus.COMPLETED)


if __name__ == "__main__":
    unittest.main()
