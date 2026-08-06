"""Unit tests for Python metrics exporter."""

import unittest

from nexuskv.metrics.exporter import PythonMetricsExporter


class TestPythonMetricsExporter(unittest.TestCase):
    def test_record_lookup_and_fail_open(self) -> None:
        exporter = PythonMetricsExporter()
        exporter.record_lookup(hit=True, tokens_saved=512)
        exporter.record_fail_open(reason="timeout_1ms")
        exporter.set_active_pinned_memory(1048576)

        text = exporter.export_prometheus_text()
        self.assertIn("nexuskv_cache_lookups_total 1", text)
        self.assertIn("nexuskv_prefill_saved_tokens_total 512", text)
        self.assertIn("nexuskv_active_pinned_memory_bytes 1048576", text)
        self.assertIn("nexuskv_fail_open_fallbacks_total 1", text)


if __name__ == "__main__":
    unittest.main()
