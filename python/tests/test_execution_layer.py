import unittest

from nexuskv.connectors.base import LookupOutcome, LookupStatus, SGLangLifecycleContext, VLLMLifecycleContext
from nexuskv.connectors.sglang.connector import SGLangConnector
from nexuskv.connectors.vllm.connector import VLLMConnector
from nexuskv.contracts.generated import TransferBackend
from nexuskv.execution.runner import BaselineExecutionRunner
from nexuskv.execution.types import ExecutionDisposition, FallbackReason, MaterializationRequest
from nexuskv.testsupport.matches import make_lookup_outcome


class ExecutionLayerTest(unittest.TestCase):
    def test_exact_hit_materializes_to_device_tier_for_vllm(self) -> None:
        connector = VLLMConnector()
        runner = BaselineExecutionRunner()
        context = VLLMLifecycleContext(
            tenant="tenant-a",
            namespace="chat",
            model="llama-70b",
            tokens=[1, 2, 3],
            descriptor=connector.default_descriptor(),
            page_id=1,
        )
        lookup = make_lookup_outcome(connector, [1, 2, 3], [], page_id=1)

        outcome = runner.execute(
            MaterializationRequest(
                hook="request_start",
                context=context,
                lookup=lookup,
                preferred_backend=None,
                allow_store_after_stage=False,
                enable_prefetch=False,
            )
        )

        self.assertEqual(outcome.primary.disposition, ExecutionDisposition.MATERIALIZE)
        self.assertEqual(outcome.primary.source.tier, lookup.match.entry.location.tier)
        self.assertEqual(outcome.primary.target.tier.value, "device")

    def test_partial_hit_recomputes_when_partial_materialization_is_unsupported(self) -> None:
        connector = SGLangConnector()
        runner = BaselineExecutionRunner()
        context = SGLangLifecycleContext(
            tenant="tenant-a",
            namespace="chat",
            model="llama-70b",
            tokens=[1, 2, 3],
            descriptor=connector.default_descriptor(),
        )
        lookup = make_lookup_outcome(connector, [1, 2], [3])

        outcome = runner.execute(
            MaterializationRequest(
                hook="extend",
                context=context,
                lookup=lookup,
                preferred_backend=None,
                allow_store_after_stage=True,
                enable_prefetch=False,
            )
        )

        self.assertEqual(outcome.primary.disposition, ExecutionDisposition.RECOMPUTE)
        self.assertEqual(outcome.primary.fallback_reason, FallbackReason.UNSUPPORTED_CAPABILITY)
        self.assertEqual(outcome.store.disposition, ExecutionDisposition.STORE)

    def test_degrades_to_simpler_backend_when_preferred_backend_is_unavailable(self) -> None:
        connector = VLLMConnector()
        runner = BaselineExecutionRunner()
        context = VLLMLifecycleContext(
            tenant="tenant-a",
            namespace="chat",
            model="llama-70b",
            tokens=[7, 8, 9],
            descriptor=connector.default_descriptor(),
            page_id=2,
        )
        lookup = make_lookup_outcome(connector, [7, 8, 9], [], page_id=2)

        outcome = runner.execute(
            MaterializationRequest(
                hook="decode_step",
                context=context,
                lookup=lookup,
                preferred_backend=TransferBackend.ZERO_COPY,
                allow_store_after_stage=False,
                enable_prefetch=False,
            )
        )

        self.assertEqual(outcome.primary.disposition, ExecutionDisposition.MATERIALIZE)
        self.assertTrue(outcome.primary.capability_check.degraded)
        self.assertEqual(outcome.primary.fallback_reason, FallbackReason.PREFERRED_BACKEND_UNAVAILABLE)

    def test_unsupported_prefetch_returns_safe_skip(self) -> None:
        connector = SGLangConnector()
        runner = BaselineExecutionRunner()
        context = SGLangLifecycleContext(
            tenant="tenant-a",
            namespace="chat",
            model="llama-70b",
            tokens=[4, 5, 6],
            descriptor=connector.default_descriptor(),
        )
        lookup = make_lookup_outcome(connector, [4, 5], [6])

        outcome = runner.execute(
            MaterializationRequest(
                hook="extend",
                context=context,
                lookup=lookup,
                preferred_backend=None,
                allow_store_after_stage=False,
                enable_prefetch=True,
            )
        )

        self.assertIsNotNone(outcome.prefetch)
        self.assertEqual(outcome.prefetch.disposition, ExecutionDisposition.SKIP)
        self.assertEqual(outcome.prefetch.fallback_reason, FallbackReason.UNSUPPORTED_CAPABILITY)

    def test_miss_recomputes_and_skips_store_when_disabled(self) -> None:
        connector = VLLMConnector()
        runner = BaselineExecutionRunner()
        context = VLLMLifecycleContext(
            tenant="tenant-a",
            namespace="chat",
            model="llama-70b",
            tokens=[10, 11, 12],
            descriptor=connector.default_descriptor(),
            page_id=3,
        )
        lookup = LookupOutcome(
            query=connector.build_query_key(context),
            status=LookupStatus.MISS,
            match=None,
            partial_plan=None,
        )

        outcome = runner.execute(
            MaterializationRequest(
                hook="decode_step",
                context=context,
                lookup=lookup,
                preferred_backend=None,
                allow_store_after_stage=False,
                enable_prefetch=False,
            )
        )

        self.assertEqual(outcome.primary.disposition, ExecutionDisposition.RECOMPUTE)
        self.assertEqual(outcome.store.disposition, ExecutionDisposition.SKIP)


if __name__ == "__main__":
    unittest.main()
