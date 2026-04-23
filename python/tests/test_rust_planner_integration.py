import subprocess
import sys
import unittest
from pathlib import Path

from nexuskv.connectors.base import LookupStatus, SGLangLifecycleContext, VLLMLifecycleContext
from nexuskv.connectors.sglang.connector import SGLangConnector
from nexuskv.connectors.vllm.connector import VLLMConnector
from nexuskv.contracts.generated import TierKind, TransferBackend


ROOT = Path(__file__).resolve().parents[2]


class RustPlannerIntegrationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        subprocess.run(
            ["cargo", "build", "-p", "bindings-py"],
            cwd=ROOT / "rust",
            check=True,
            capture_output=True,
            text=True,
        )

    def test_python_can_insert_and_lookup_exact_hit_via_rust(self) -> None:
        from nexuskv.planner.rust_backend import RustPlanner

        connector = VLLMConnector()
        planner = RustPlanner()
        context = VLLMLifecycleContext(
            tenant="tenant-a",
            namespace="chat",
            model="llama-70b",
            tokens=[10, 11, 12],
            descriptor=connector.default_descriptor(),
            page_id=1,
        )
        store = connector.prepare_store(
            context,
            entry_id="entry-exact",
            locator="remote://entry-exact",
            tier=TierKind.REMOTE_SHARED,
        )
        planner.insert(store.reuse_key, store.entry)

        decision = connector.on_request_start(context, planner)

        self.assertEqual(decision.lookup.status, LookupStatus.HIT)
        self.assertEqual(decision.lookup.match.entry.identity.entry_id, "entry-exact")

    def test_python_can_plan_partial_hit_via_rust(self) -> None:
        from nexuskv.planner.rust_backend import RustPlanner

        connector = VLLMConnector()
        planner = RustPlanner()
        base = VLLMLifecycleContext(
            tenant="tenant-a",
            namespace="chat",
            model="llama-70b",
            tokens=[21, 22, 23],
            descriptor=connector.default_descriptor(),
            page_id=7,
        )
        planner.insert(
            connector.prepare_store(
                base,
                entry_id="entry-prefix",
                locator="remote://entry-prefix",
                tier=TierKind.REMOTE_SHARED,
            ).reuse_key,
            connector.prepare_store(
                base,
                entry_id="entry-prefix",
                locator="remote://entry-prefix",
                tier=TierKind.REMOTE_SHARED,
            ).entry,
        )

        query = VLLMLifecycleContext(
            tenant="tenant-a",
            namespace="chat",
            model="llama-70b",
            tokens=[21, 22, 23, 24, 25],
            descriptor=connector.default_descriptor(),
            page_id=7,
            preferred_backend=TransferBackend.STAGED_COPY,
        )

        decision = connector.on_request_start(query, planner)

        self.assertEqual(decision.lookup.status, LookupStatus.PARTIAL)
        self.assertEqual(decision.lookup.partial_plan.remaining.tokens, [24, 25])
        self.assertEqual(decision.prefetch.status.value, "scheduled")

    def test_identity_isolation_is_preserved_through_rust_planner(self) -> None:
        from nexuskv.planner.rust_backend import RustPlanner

        connector = SGLangConnector()
        planner = RustPlanner()
        stored = SGLangLifecycleContext(
            tenant="tenant-a",
            namespace="chat",
            model="llama-70b",
            tokens=[1, 2, 3],
            descriptor=connector.default_descriptor(),
        )
        planner.insert(
            connector.prepare_store(
                stored,
                entry_id="entry-sglang",
                locator="remote://entry-sglang",
                tier=TierKind.REMOTE_SHARED,
            ).reuse_key,
            connector.prepare_store(
                stored,
                entry_id="entry-sglang",
                locator="remote://entry-sglang",
                tier=TierKind.REMOTE_SHARED,
            ).entry,
        )

        isolated = SGLangLifecycleContext(
            tenant="tenant-a",
            namespace="system",
            model="llama-70b",
            tokens=[1, 2, 3],
            descriptor=connector.default_descriptor(),
        )

        decision = connector.on_prefill(isolated, planner)
        self.assertEqual(decision.lookup.status, LookupStatus.MISS)

    def test_unsupported_partial_materialization_degrades_safely_through_real_planner(self) -> None:
        from nexuskv.planner.rust_backend import RustPlanner

        connector = SGLangConnector()
        planner = RustPlanner()
        stored = SGLangLifecycleContext(
            tenant="tenant-a",
            namespace="chat",
            model="llama-70b",
            tokens=[4, 5],
            descriptor=connector.default_descriptor(),
        )
        planner.insert(
            connector.prepare_store(
                stored,
                entry_id="entry-partial",
                locator="remote://entry-partial",
                tier=TierKind.REMOTE_SHARED,
            ).reuse_key,
            connector.prepare_store(
                stored,
                entry_id="entry-partial",
                locator="remote://entry-partial",
                tier=TierKind.REMOTE_SHARED,
            ).entry,
        )

        query = SGLangLifecycleContext(
            tenant="tenant-a",
            namespace="chat",
            model="llama-70b",
            tokens=[4, 5, 6],
            descriptor=connector.default_descriptor(),
        )

        decision = connector.on_extend(query, planner)
        self.assertEqual(decision.lookup.status, LookupStatus.PARTIAL)
        self.assertEqual(decision.materialization.fallback.mode.value, "recompute")


if __name__ == "__main__":
    unittest.main()
