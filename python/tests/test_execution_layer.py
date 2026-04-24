import unittest

from nexuskv.connectors.base import LookupOutcome, LookupStatus, SGLangLifecycleContext, VLLMLifecycleContext
from nexuskv.connectors.sglang.connector import SGLangConnector
from nexuskv.connectors.vllm.connector import VLLMConnector
from nexuskv.contracts.generated import TransferBackend
from nexuskv.execution.backend import BaselineExecutionBackend
from nexuskv.execution.runner import BaselineExecutionRunner
from nexuskv.execution.types import (
    BackendActionKind,
    BackendActionStatus,
    ExecutionDisposition,
    FallbackReason,
    MaterializationRequest,
)
from nexuskv.testsupport.matches import make_lookup_outcome


class ExecutionLayerTest(unittest.TestCase):
    def test_exact_hit_materializes_to_device_tier_for_vllm(self) -> None:
        connector = VLLMConnector()
        backend = BaselineExecutionBackend()
        runner = BaselineExecutionRunner(backend=backend)
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

        self.assertEqual(outcome.primary.decision.disposition, ExecutionDisposition.MATERIALIZE)
        self.assertEqual(outcome.primary.decision.source.tier, lookup.match.entry.location.tier)
        self.assertEqual(outcome.primary.decision.target.tier.value, "device")
        self.assertEqual(outcome.primary.result.status, BackendActionStatus.SUCCEEDED)
        self.assertEqual(outcome.primary.result.executed_kind, BackendActionKind.MATERIALIZE)
        self.assertEqual(backend.calls[0].request.kind, BackendActionKind.MATERIALIZE)

    def test_partial_hit_recomputes_when_partial_materialization_is_unsupported(self) -> None:
        connector = SGLangConnector()
        backend = BaselineExecutionBackend()
        runner = BaselineExecutionRunner(backend=backend)
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

        self.assertEqual(outcome.primary.decision.disposition, ExecutionDisposition.RECOMPUTE)
        self.assertEqual(outcome.primary.result.status, BackendActionStatus.RECOMPUTED)
        self.assertEqual(outcome.primary.result.executed_kind, BackendActionKind.RECOMPUTE)
        self.assertEqual(outcome.primary.decision.fallback_reason, FallbackReason.UNSUPPORTED_CAPABILITY)
        self.assertEqual(outcome.store.decision.disposition, ExecutionDisposition.STORE)
        self.assertEqual(backend.calls[0].request.kind, BackendActionKind.RECOMPUTE)
        self.assertEqual(backend.calls[1].request.kind, BackendActionKind.STORE)

    def test_degrades_to_simpler_backend_when_preferred_backend_is_unavailable(self) -> None:
        connector = VLLMConnector()
        backend = BaselineExecutionBackend()
        runner = BaselineExecutionRunner(backend=backend)
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

        self.assertEqual(outcome.primary.decision.disposition, ExecutionDisposition.MATERIALIZE)
        self.assertTrue(outcome.primary.decision.capability_check.degraded)
        self.assertEqual(outcome.primary.decision.fallback_reason, FallbackReason.PREFERRED_BACKEND_UNAVAILABLE)
        self.assertEqual(outcome.primary.result.status, BackendActionStatus.SUCCEEDED)
        self.assertEqual(outcome.primary.result.selected_backend, TransferBackend.STAGED_COPY)

    def test_unsupported_prefetch_returns_safe_skip(self) -> None:
        connector = SGLangConnector()
        backend = BaselineExecutionBackend()
        runner = BaselineExecutionRunner(backend=backend)
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
        self.assertEqual(outcome.prefetch.decision.disposition, ExecutionDisposition.SKIP)
        self.assertEqual(outcome.prefetch.decision.fallback_reason, FallbackReason.UNSUPPORTED_CAPABILITY)
        self.assertEqual(outcome.prefetch.result.status, BackendActionStatus.SKIPPED)
        self.assertEqual(outcome.prefetch.result.executed_kind, BackendActionKind.SKIP)

    def test_miss_recomputes_and_skips_store_when_disabled(self) -> None:
        connector = VLLMConnector()
        backend = BaselineExecutionBackend()
        runner = BaselineExecutionRunner(backend=backend)
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

        self.assertEqual(outcome.primary.decision.disposition, ExecutionDisposition.RECOMPUTE)
        self.assertEqual(outcome.primary.result.status, BackendActionStatus.RECOMPUTED)
        self.assertEqual(outcome.store.decision.disposition, ExecutionDisposition.SKIP)
        self.assertEqual(outcome.store.result.status, BackendActionStatus.SKIPPED)

    def test_backend_rejection_falls_back_to_recompute(self) -> None:
        connector = VLLMConnector()
        backend = BaselineExecutionBackend(supported_backends=(TransferBackend.BASELINE_TRANSPORT,))
        runner = BaselineExecutionRunner(backend=backend)
        context = VLLMLifecycleContext(
            tenant="tenant-a",
            namespace="chat",
            model="llama-70b",
            tokens=[12, 13, 14],
            descriptor=connector.default_descriptor(),
            page_id=5,
        )
        lookup = make_lookup_outcome(connector, [12, 13, 14], [], page_id=5)

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

        self.assertEqual(outcome.primary.decision.disposition, ExecutionDisposition.MATERIALIZE)
        self.assertEqual(outcome.primary.result.status, BackendActionStatus.FALLBACK)
        self.assertEqual(outcome.primary.result.executed_kind, BackendActionKind.RECOMPUTE)
        self.assertEqual(backend.calls[0].result.status, BackendActionStatus.REJECTED)
        self.assertEqual(backend.calls[1].request.kind, BackendActionKind.RECOMPUTE)


if __name__ == "__main__":
    unittest.main()
