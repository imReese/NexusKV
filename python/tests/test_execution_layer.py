import unittest

from nexuskv.connectors.base import LookupOutcome, LookupStatus, SGLangLifecycleContext, VLLMLifecycleContext
from nexuskv.connectors.sglang.connector import SGLangConnector
from nexuskv.connectors.vllm.connector import VLLMConnector
from nexuskv.contracts.generated import (
    CompatibilitySignal,
    EntryIdentity,
    EntryLocation,
    EntryVersion,
    KeyIdentity,
    MatchClassification,
    MatchExtent,
    QueryKey,
    RemainingWork,
    ReuseKey,
    TierKind,
    TransferBackend,
)
from nexuskv.execution.backend import BaselineExecutionBackend
from nexuskv.execution.catalog import BackendCatalog, BackendRegistration
from nexuskv.execution.runner import BaselineExecutionRunner
from nexuskv.execution.store import InMemoryEntryStore
from nexuskv.execution.types import (
    BackendActionKind,
    BackendActionStatus,
    CapabilityCheckResult,
    ExecutionDisposition,
    FallbackReason,
    MaterializationDecision,
    MaterializationRequest,
    SourceTier,
    TargetTier,
    TransferMode,
)
from nexuskv.testsupport.matches import make_lookup_outcome


class ExecutionLayerTest(unittest.TestCase):
    def test_exact_hit_materializes_to_device_tier_for_vllm(self) -> None:
        connector = VLLMConnector()
        backend = BaselineExecutionBackend(supported_backends=(TransferBackend.STAGED_COPY,))
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
        backend = BaselineExecutionBackend(supported_backends=(TransferBackend.STAGED_COPY,))
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
        backend = BaselineExecutionBackend(supported_backends=(TransferBackend.STAGED_COPY,))
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

    def test_no_backend_catalog_entry_falls_back_to_recompute(self) -> None:
        connector = VLLMConnector()
        runner = BaselineExecutionRunner(catalog=BackendCatalog())
        context = VLLMLifecycleContext(
            tenant="tenant-a",
            namespace="chat",
            model="llama-70b",
            tokens=[15, 16, 17],
            descriptor=connector.default_descriptor(),
            page_id=6,
        )
        lookup = make_lookup_outcome(connector, [15, 16, 17], [], page_id=6)

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

        self.assertEqual(outcome.primary.result.status, BackendActionStatus.FALLBACK)
        self.assertEqual(outcome.primary.result.executed_kind, BackendActionKind.RECOMPUTE)
        self.assertEqual(outcome.primary.result.backend_name, "no-backend-selected")

    def test_store_then_materialize_reads_from_baseline_store(self) -> None:
        connector = SGLangConnector()
        store = InMemoryEntryStore()
        backend = BaselineExecutionBackend(store=store)
        catalog = BackendCatalog()
        catalog.register(
            BackendRegistration(
                name=backend.backend_name,
                backend=backend,
                transfer_backend=TransferBackend.BASELINE_TRANSPORT,
                action_kinds=(BackendActionKind.MATERIALIZE, BackendActionKind.PREFETCH, BackendActionKind.STORE),
                target_tiers=(TierKind.HOST_DRAM, TierKind.REMOTE_SHARED),
                priority=10,
            )
        )
        catalog.register(
            BackendRegistration(
                name=backend.backend_name,
                backend=backend,
                transfer_backend=None,
                action_kinds=(BackendActionKind.SKIP, BackendActionKind.RECOMPUTE),
                priority=5,
            )
        )
        runner = BaselineExecutionRunner(catalog=catalog)
        context = SGLangLifecycleContext(
            tenant="tenant-a",
            namespace="chat",
            model="llama-70b",
            tokens=[20, 21, 22],
            descriptor=connector.default_descriptor(),
        )
        miss_lookup = LookupOutcome(
            query=connector.build_query_key(context),
            status=LookupStatus.MISS,
            match=None,
            partial_plan=None,
        )

        store_outcome = runner.execute(
            MaterializationRequest(
                hook="prefill",
                context=context,
                lookup=miss_lookup,
                preferred_backend=None,
                allow_store_after_stage=True,
                enable_prefetch=False,
            )
        )

        self.assertEqual(store_outcome.store.result.status, BackendActionStatus.SUCCEEDED)
        self.assertEqual(store_outcome.store.result.executed_kind, BackendActionKind.STORE)

        materialize_lookup = make_lookup_outcome(connector, [20, 21, 22], [])
        materialize_outcome = runner.execute(
            MaterializationRequest(
                hook="prefill",
                context=context,
                lookup=materialize_lookup,
                preferred_backend=None,
                allow_store_after_stage=False,
                enable_prefetch=False,
            )
        )

        self.assertEqual(materialize_outcome.primary.result.status, BackendActionStatus.SUCCEEDED)
        self.assertIn("materialized stored entry", materialize_outcome.primary.result.detail)

    def test_missing_materialize_returns_structured_miss(self) -> None:
        connector = SGLangConnector()
        backend = BaselineExecutionBackend()
        catalog = BackendCatalog()
        catalog.register(
            BackendRegistration(
                name=backend.backend_name,
                backend=backend,
                transfer_backend=TransferBackend.BASELINE_TRANSPORT,
                action_kinds=(BackendActionKind.MATERIALIZE,),
                source_tiers=(TierKind.REMOTE_SHARED,),
                target_tiers=(TierKind.HOST_DRAM,),
                priority=10,
            )
        )
        catalog.register(
            BackendRegistration(
                name=backend.backend_name,
                backend=backend,
                transfer_backend=None,
                action_kinds=(BackendActionKind.SKIP, BackendActionKind.RECOMPUTE),
                priority=5,
            )
        )
        runner = BaselineExecutionRunner(catalog=catalog)
        context = SGLangLifecycleContext(
            tenant="tenant-a",
            namespace="chat",
            model="llama-70b",
            tokens=[30, 31, 32],
            descriptor=connector.default_descriptor(),
        )
        identity = KeyIdentity(
            tenant="tenant-a",
            namespace="chat",
            model="llama-70b",
            engine_family=context.descriptor.engine_family,
            semantic_type=context.descriptor.semantic_type,
            tokens=[30, 31, 32],
            block_id=None,
            page_id=None,
        )
        lookup = LookupOutcome(
            query=QueryKey(identity=identity),
            status=LookupStatus.HIT,
            match=make_lookup_outcome(connector, [30, 31, 32], []).match,
            partial_plan=None,
        )
        lookup.match.entry = lookup.match.entry.__class__(
            identity=EntryIdentity(
                key=identity,
                entry_id="missing-entry",
                version=EntryVersion(generation=1, lineage="missing"),
            ),
            descriptor=context.descriptor,
            location=EntryLocation(tier=TierKind.REMOTE_SHARED, locator="memory://missing-entry"),
            policy_hint=lookup.match.entry.policy_hint,
        )

        outcome = runner.execute(
            MaterializationRequest(
                hook="prefill",
                context=context,
                lookup=lookup,
                preferred_backend=None,
                allow_store_after_stage=False,
                enable_prefetch=False,
            )
        )

        self.assertEqual(outcome.primary.result.status, BackendActionStatus.FALLBACK)
        self.assertEqual(outcome.primary.result.executed_kind, BackendActionKind.RECOMPUTE)
        self.assertEqual(outcome.primary.result.fallback_reason, FallbackReason.CACHE_MISS)

    def test_baseline_store_preserves_namespace_model_isolation(self) -> None:
        connector = SGLangConnector()
        store = InMemoryEntryStore()
        backend = BaselineExecutionBackend(store=store)
        catalog = BackendCatalog()
        catalog.register(
            BackendRegistration(
                name=backend.backend_name,
                backend=backend,
                transfer_backend=TransferBackend.BASELINE_TRANSPORT,
                action_kinds=(BackendActionKind.MATERIALIZE, BackendActionKind.STORE),
                target_tiers=(TierKind.HOST_DRAM, TierKind.REMOTE_SHARED),
                priority=10,
            )
        )
        catalog.register(
            BackendRegistration(
                name=backend.backend_name,
                backend=backend,
                transfer_backend=None,
                action_kinds=(BackendActionKind.SKIP, BackendActionKind.RECOMPUTE),
                priority=5,
            )
        )
        runner = BaselineExecutionRunner(catalog=catalog)
        stored_context = SGLangLifecycleContext(
            tenant="tenant-a",
            namespace="chat",
            model="llama-70b",
            tokens=[40, 41, 42],
            descriptor=connector.default_descriptor(),
        )
        runner.execute(
            MaterializationRequest(
                hook="prefill",
                context=stored_context,
                lookup=LookupOutcome(
                    query=connector.build_query_key(stored_context),
                    status=LookupStatus.MISS,
                    match=None,
                    partial_plan=None,
                ),
                preferred_backend=None,
                allow_store_after_stage=True,
                enable_prefetch=False,
            )
        )

        isolated_context = SGLangLifecycleContext(
            tenant="tenant-a",
            namespace="system",
            model="llama-70b",
            tokens=[40, 41, 42],
            descriptor=connector.default_descriptor(),
        )
        isolated_identity = connector.build_query_key(isolated_context).identity
        isolated_lookup = LookupOutcome(
            query=QueryKey(identity=isolated_identity),
            status=LookupStatus.HIT,
            match=make_lookup_outcome(connector, [40, 41, 42], []).match,
            partial_plan=None,
        )
        isolated_lookup.match.entry = isolated_lookup.match.entry.__class__(
            identity=EntryIdentity(
                key=isolated_identity,
                entry_id="isolated-miss",
                version=EntryVersion(generation=1, lineage="isolated"),
            ),
            descriptor=isolated_context.descriptor,
            location=EntryLocation(tier=TierKind.REMOTE_SHARED, locator="memory://isolated-miss"),
            policy_hint=isolated_lookup.match.entry.policy_hint,
        )

        outcome = runner.execute(
            MaterializationRequest(
                hook="prefill",
                context=isolated_context,
                lookup=isolated_lookup,
                preferred_backend=None,
                allow_store_after_stage=False,
                enable_prefetch=False,
            )
        )

        self.assertEqual(outcome.primary.result.status, BackendActionStatus.FALLBACK)
        self.assertEqual(outcome.primary.result.fallback_reason, FallbackReason.CACHE_MISS)

    def test_staged_copy_backend_is_selected_when_capabilities_match(self) -> None:
        connector = VLLMConnector()
        runner = BaselineExecutionRunner()
        context = VLLMLifecycleContext(
            tenant="tenant-a",
            namespace="chat",
            model="llama-70b",
            tokens=[50, 51, 52],
            descriptor=connector.default_descriptor(),
            page_id=8,
        )
        lookup = make_lookup_outcome(connector, [50, 51, 52], [], page_id=8)

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

        self.assertEqual(outcome.primary.result.backend_name, "staged-copy-backend")

    def test_remote_shared_store_backend_is_selected_when_capabilities_match(self) -> None:
        connector = VLLMConnector()
        runner = BaselineExecutionRunner()
        context = VLLMLifecycleContext(
            tenant="tenant-a",
            namespace="chat",
            model="llama-70b",
            tokens=[60, 61, 62],
            descriptor=connector.default_descriptor(),
            page_id=9,
        )
        lookup = LookupOutcome(
            query=connector.build_query_key(context),
            status=LookupStatus.MISS,
            match=None,
            partial_plan=None,
        )

        outcome = runner.execute(
            MaterializationRequest(
                hook="request_start",
                context=context,
                lookup=lookup,
                preferred_backend=None,
                allow_store_after_stage=True,
                enable_prefetch=False,
            )
        )

        self.assertEqual(outcome.store.result.backend_name, "remote-shared-store-backend")


if __name__ == "__main__":
    unittest.main()
