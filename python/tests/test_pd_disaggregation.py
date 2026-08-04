import unittest

from nexuskv.connectors.base import PDDisaggregateContext
from nexuskv.connectors.vllm.connector import VLLMConnector
from nexuskv.planner.rust_backend import RustPlanner


class TestPDDisaggregation(unittest.TestCase):
    def test_pd_disaggregation_handshake_lifecycle(self):
        connector = VLLMConnector()
        planner = RustPlanner()

        ctx = PDDisaggregateContext(
            tenant="tenant-pd",
            namespace="ns-pd",
            model="llama-70b",
            tokens=[1, 2, 3, 4, 5],
            descriptor=connector.default_descriptor(),
            prefill_worker_id="prefill-node-01",
            decode_worker_id="decode-node-01",
            handshake_ack=True,
        )

        decision = connector.on_pd_disaggregate_handshake(ctx, planner)
        self.assertIsNotNone(decision)
        self.assertEqual(decision.hook, "pd_disaggregate_handshake")


if __name__ == "__main__":
    unittest.main()
