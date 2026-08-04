import unittest

from nexuskv.connectors.base import VLLMLifecycleContext
from nexuskv.connectors.vllm.connector import VLLMConnector
from nexuskv.connectors.native_hooks import NativeEngineHookInterceptor
from nexuskv.execution.types import BackendActionStatus


class TestNativeHooks(unittest.TestCase):
    def test_interceptor_normal_execution_and_fail_open_guarantee(self):
        connector = VLLMConnector()
        interceptor = NativeEngineHookInterceptor(connector=connector, max_hook_timeout_ms=5.0)

        ctx = VLLMLifecycleContext(
            tenant="tenant-1",
            namespace="ns-1",
            model="m-1",
            tokens=[1, 2, 3, 4],
            descriptor=connector.default_descriptor(),
        )

        # Intercept request_start
        decision = interceptor.intercept_hook("request_start", ctx)
        self.assertIsNotNone(decision)
        self.assertGreaterEqual(interceptor.stats.total_calls, 1)

    def test_interceptor_fail_open_on_exception(self):
        connector = VLLMConnector()
        interceptor = NativeEngineHookInterceptor(connector=connector)

        ctx = VLLMLifecycleContext(
            tenant="tenant-1",
            namespace="ns-1",
            model="m-1",
            tokens=[1, 2, 3, 4],
            descriptor=connector.default_descriptor(),
        )

        # Intercept with invalid hook triggers fail-open without raising exception
        decision = interceptor.intercept_hook("invalid_hook_name", ctx)
        self.assertIsNotNone(decision)
        self.assertEqual(decision.execution.primary.result.status, BackendActionStatus.FALLBACK)
        self.assertEqual(interceptor.stats.fail_open_fallbacks, 1)


if __name__ == "__main__":
    unittest.main()
