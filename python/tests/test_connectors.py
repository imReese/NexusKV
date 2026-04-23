import unittest

from nexuskv.connectors.sglang.connector import SGLangConnector
from nexuskv.connectors.vllm.connector import VLLMConnector


class ConnectorSurfaceTest(unittest.TestCase):
    def test_sglang_connector_reports_prefill_and_decode_hooks(self) -> None:
        connector = SGLangConnector()

        self.assertIn("prefill", connector.supported_hooks())
        self.assertIn("decode", connector.supported_hooks())

    def test_vllm_connector_declares_engine_identity(self) -> None:
        connector = VLLMConnector()

        self.assertEqual(connector.engine_name, "vllm")
        self.assertIn("request_start", connector.supported_hooks())


if __name__ == "__main__":
    unittest.main()
