import unittest

from nexuskv.connectors.base import (
    LookupStatus,
    SGLangLifecycleContext,
    TransferBackend,
    VLLMLifecycleContext,
)
from nexuskv.connectors.sglang.connector import SGLangConnector
from nexuskv.connectors.vllm.connector import VLLMConnector
from nexuskv.contracts.generated import (
    CacheEntry,
    CompatibilitySignal,
    EntryIdentity,
    EntryLocation,
    EntryVersion,
    KeyIdentity,
    MatchClassification,
    MatchExtent,
    MatchResult,
    PartialHitPlan,
    PlanDisposition,
    QueryKey,
    RemainingWork,
    ReusableSlice,
    ReuseKey,
    TierKind,
)
from nexuskv.execution.types import ExecutionDisposition, FallbackReason


class FakePlanner:
    def __init__(self, match: MatchResult | None = None, plan: PartialHitPlan | None = None) -> None:
        self.match = match
        self.plan = plan
        self.queries: list[QueryKey] = []

    def lookup(self, query: QueryKey) -> MatchResult | None:
        self.queries.append(query)
        return self.match

    def plan_partial_hit(self, query: QueryKey) -> PartialHitPlan | None:
        self.queries.append(query)
        return self.plan


def make_match(connector, tokens: list[int], remaining: list[int], *, page_id: int | None = None) -> MatchResult:
    descriptor = connector.default_descriptor()
    key = KeyIdentity(
        tenant="tenant-a",
        namespace="chat",
        model="llama-70b",
        engine_family=descriptor.engine_family,
        semantic_type=descriptor.semantic_type,
        tokens=tokens,
        block_id=None,
        page_id=page_id,
    )
    entry = CacheEntry(
        identity=EntryIdentity(
            key=key,
            entry_id="entry-1",
            version=EntryVersion(generation=1, lineage="lineage-a"),
        ),
        descriptor=descriptor,
        location=EntryLocation(tier=TierKind.REMOTE_SHARED, locator="remote://entry-1"),
        policy_hint=connector.prepare_store(
            SGLangLifecycleContext(
                tenant="tenant-a",
                namespace="chat",
                model="llama-70b",
                tokens=tokens,
                descriptor=descriptor,
            )
            if connector.engine_name == "sglang"
            else VLLMLifecycleContext(
                tenant="tenant-a",
                namespace="chat",
                model="llama-70b",
                tokens=tokens,
                descriptor=descriptor,
                page_id=page_id,
            ),
            entry_id="entry-1",
            locator="remote://entry-1",
            tier=TierKind.REMOTE_SHARED,
        ).entry.policy_hint,
    )
    classification = MatchClassification.EXACT if not remaining else MatchClassification.PARTIAL
    return MatchResult(
        classification=classification,
        matched_key=ReuseKey(identity=key),
        requested_key=QueryKey(
            identity=KeyIdentity(
                tenant="tenant-a",
                namespace="chat",
                model="llama-70b",
                engine_family=descriptor.engine_family,
                semantic_type=descriptor.semantic_type,
                tokens=tokens + remaining,
                block_id=None,
                page_id=page_id,
            )
        ),
        matched_extent=MatchExtent(units=len(tokens), granularity=descriptor.granularity),
        entry=entry,
        remaining=RemainingWork(
            tokens=remaining,
            fetch_required=bool(remaining),
            recompute_required=bool(remaining),
        ),
        compatibility=CompatibilitySignal(
            reusable=True,
            fallback_to_recompute=False,
            reason="",
        ),
    )


class ConnectorLifecycleTest(unittest.TestCase):
    def test_sglang_prefill_uses_exact_hit_with_baseline_transport(self) -> None:
        connector = SGLangConnector()
        match = make_match(connector, [1, 2, 3], [])
        planner = FakePlanner(match=match)
        context = SGLangLifecycleContext(
            tenant="tenant-a",
            namespace="chat",
            model="llama-70b",
            tokens=[1, 2, 3],
            descriptor=connector.default_descriptor(),
        )

        decision = connector.on_prefill(context, planner)

        self.assertEqual(decision.lookup.status, LookupStatus.HIT)
        self.assertEqual(decision.materialization.disposition, ExecutionDisposition.MATERIALIZE)
        self.assertEqual(decision.materialization.transfer.selected_backend, TransferBackend.BASELINE_TRANSPORT)
        self.assertFalse(decision.should_store_after_stage)

    def test_sglang_extend_degrades_partial_hit_to_recompute(self) -> None:
        connector = SGLangConnector()
        match = make_match(connector, [1, 2], [3, 4])
        plan = connector.partial_plan_from_match(match)
        planner = FakePlanner(match=match, plan=plan)
        context = SGLangLifecycleContext(
            tenant="tenant-a",
            namespace="chat",
            model="llama-70b",
            tokens=[1, 2, 3, 4],
            descriptor=connector.default_descriptor(),
        )

        decision = connector.on_extend(context, planner)

        self.assertEqual(decision.lookup.status, LookupStatus.PARTIAL)
        self.assertEqual(decision.materialization.disposition, ExecutionDisposition.RECOMPUTE)
        self.assertEqual(decision.materialization.fallback_reason, FallbackReason.UNSUPPORTED_CAPABILITY)
        self.assertTrue(decision.should_store_after_stage)

    def test_vllm_request_start_uses_partial_plan_and_prefetch(self) -> None:
        connector = VLLMConnector()
        match = make_match(connector, [10, 11, 12], [13, 14], page_id=7)
        plan = connector.partial_plan_from_match(match)
        planner = FakePlanner(match=match, plan=plan)
        context = VLLMLifecycleContext(
            tenant="tenant-a",
            namespace="chat",
            model="llama-70b",
            tokens=[10, 11, 12, 13, 14],
            descriptor=connector.default_descriptor(),
            page_id=7,
        )

        decision = connector.on_request_start(context, planner)

        self.assertEqual(decision.lookup.status, LookupStatus.PARTIAL)
        self.assertEqual(decision.materialization.transfer.selected_backend, TransferBackend.STAGED_COPY)
        self.assertEqual(decision.prefetch.disposition, ExecutionDisposition.PREFETCH)
        self.assertTrue(decision.should_store_after_stage)

    def test_vllm_decode_step_degrades_to_available_transfer_backend(self) -> None:
        connector = VLLMConnector()
        match = make_match(connector, [20, 21, 22], [], page_id=9)
        planner = FakePlanner(match=match)
        context = VLLMLifecycleContext(
            tenant="tenant-a",
            namespace="chat",
            model="llama-70b",
            tokens=[20, 21, 22],
            descriptor=connector.default_descriptor(),
            page_id=9,
            preferred_backend=TransferBackend.ZERO_COPY,
        )

        decision = connector.on_decode_step(context, planner)

        self.assertEqual(decision.lookup.status, LookupStatus.HIT)
        self.assertTrue(decision.materialization.capability_check.degraded)
        self.assertEqual(decision.materialization.transfer.selected_backend, TransferBackend.STAGED_COPY)
        self.assertEqual(decision.materialization.fallback_reason, FallbackReason.PREFERRED_BACKEND_UNAVAILABLE)

    def test_prepare_store_uses_generated_reuse_key_boundary(self) -> None:
        connector = VLLMConnector()
        context = VLLMLifecycleContext(
            tenant="tenant-a",
            namespace="chat",
            model="llama-70b",
            tokens=[30, 31, 32],
            descriptor=connector.default_descriptor(),
            page_id=4,
        )

        store = connector.prepare_store(
            context,
            entry_id="entry-store",
            locator="remote://entry-store",
            tier=TierKind.REMOTE_SHARED,
        )

        self.assertEqual(store.reuse_key.identity.page_id, 4)
        self.assertEqual(store.entry.descriptor.engine_family, connector.default_descriptor().engine_family)


if __name__ == "__main__":
    unittest.main()
