import subprocess
import sys
import unittest
from pathlib import Path

from nexuskv.connectors.base import LookupStatus, SGLangLifecycleContext, VLLMLifecycleContext
from nexuskv.connectors.sglang.connector import SGLangConnector
from nexuskv.connectors.vllm.connector import VLLMConnector
from nexuskv.contracts.generated import TierKind, TransferBackend
from nexuskv.execution.backend import BaselineExecutionBackend
from nexuskv.execution.policy import ExecutionPolicy
from nexuskv.execution.runner import BaselineExecutionRunner
from nexuskv.execution.types import ExecutionDisposition, FallbackReason, TransferStatus


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

        backend = BaselineExecutionBackend(supported_backends=(TransferBackend.STAGED_COPY,))
        connector = VLLMConnector(execution_runner=BaselineExecutionRunner(backend=backend))
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
        self.assertEqual(decision.materialization_result.executed_kind.value, "materialize")
        self.assertEqual(backend.calls[0].request.kind.value, "materialize")
        self.assertIsNotNone(decision.materialization_result.payload_handle)
        self.assertEqual(decision.materialization_result.transfer_session.result.status, TransferStatus.COMPLETED)

    def test_python_can_plan_partial_hit_via_rust(self) -> None:
        from nexuskv.planner.rust_backend import RustPlanner

        backend = BaselineExecutionBackend(supported_backends=(TransferBackend.STAGED_COPY,))
        connector = VLLMConnector(execution_runner=BaselineExecutionRunner(backend=backend))
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
        self.assertEqual(decision.prefetch.disposition, ExecutionDisposition.PREFETCH)
        self.assertEqual(decision.prefetch_result.executed_kind.value, "prefetch")
        self.assertEqual([call.request.kind.value for call in backend.calls[:3]], ["materialize", "prefetch", "store"])
        self.assertEqual(decision.prefetch_result.transfer_session.result.status, TransferStatus.REGISTERED)

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

        backend = BaselineExecutionBackend()
        connector = SGLangConnector(execution_runner=BaselineExecutionRunner(backend=backend))
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
        self.assertEqual(decision.materialization.disposition, ExecutionDisposition.RECOMPUTE)
        self.assertEqual(decision.materialization.fallback_reason, FallbackReason.UNSUPPORTED_CAPABILITY)
        self.assertEqual(decision.materialization_result.executed_kind.value, "recompute")
        self.assertEqual(decision.materialization_result.transfer_session.result.status, TransferStatus.FALLBACK)

    def test_default_runner_uses_catalog_backends_with_real_planner(self) -> None:
        from nexuskv.planner.rust_backend import RustPlanner

        connector = VLLMConnector()
        planner = RustPlanner()
        base = VLLMLifecycleContext(
            tenant="tenant-a",
            namespace="chat",
            model="llama-70b",
            tokens=[70, 71, 72],
            descriptor=connector.default_descriptor(),
            page_id=10,
        )
        planner.insert(
            connector.prepare_store(
                base,
                entry_id="entry-catalog",
                locator="remote://entry-catalog",
                tier=TierKind.REMOTE_SHARED,
            ).reuse_key,
            connector.prepare_store(
                base,
                entry_id="entry-catalog",
                locator="remote://entry-catalog",
                tier=TierKind.REMOTE_SHARED,
            ).entry,
        )

        decision = connector.on_request_start(
            VLLMLifecycleContext(
                tenant="tenant-a",
                namespace="chat",
                model="llama-70b",
                tokens=[70, 71, 72, 73],
                descriptor=connector.default_descriptor(),
                page_id=10,
            ),
            planner,
        )

        self.assertEqual(decision.materialization_result.backend_name, "staged-copy-backend")
        self.assertEqual(decision.prefetch_result.backend_name, "staged-copy-backend")
        self.assertEqual(decision.store_result.backend_name, "remote-shared-store-backend")
        self.assertIsNotNone(decision.materialization_result.payload_handle)
        self.assertIsNotNone(decision.store_result.payload_handle)
        self.assertIsNotNone(decision.materialization_result.transfer_session.result.intermediate_handle)

    def test_connector_execution_remains_stable_under_policy_change(self) -> None:
        from nexuskv.planner.rust_backend import RustPlanner

        policy = ExecutionPolicy.from_dict(
            {
                **ExecutionPolicy.default().to_dict(),
                "allowed_target_tiers": ["host_dram"],
                "allowed_buffer_kinds": ["host_pinned"],
            }
        )
        backend = BaselineExecutionBackend(supported_backends=(TransferBackend.STAGED_COPY,))
        connector = VLLMConnector(execution_runner=BaselineExecutionRunner(backend=backend, policy=policy))
        planner = RustPlanner()
        base = VLLMLifecycleContext(
            tenant="tenant-a",
            namespace="chat",
            model="llama-70b",
            tokens=[90, 91, 92],
            descriptor=connector.default_descriptor(),
            page_id=12,
        )
        planner.insert(
            connector.prepare_store(
                base,
                entry_id="entry-policy",
                locator="remote://entry-policy",
                tier=TierKind.REMOTE_SHARED,
            ).reuse_key,
            connector.prepare_store(
                base,
                entry_id="entry-policy",
                locator="remote://entry-policy",
                tier=TierKind.REMOTE_SHARED,
            ).entry,
        )

        decision = connector.on_request_start(base, planner)

        self.assertEqual(decision.lookup.status, LookupStatus.HIT)
        self.assertEqual(decision.materialization_result.status.value, "succeeded")
        self.assertEqual(decision.materialization_result.target.tier, TierKind.HOST_DRAM)
        self.assertEqual(decision.materialization_result.payload_handle.location.tier, TierKind.HOST_DRAM)


if __name__ == "__main__":
    unittest.main()
