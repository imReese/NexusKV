import tempfile
import unittest
from pathlib import Path

from nexuskv.connectors.base import LookupOutcome, LookupStatus, VLLMLifecycleContext
from nexuskv.connectors.vllm.connector import VLLMConnector
from nexuskv.contracts.generated import MaterializationCapability, TierKind, TransferBackend
from nexuskv.execution import (
    BackendCapabilityOverlay,
    BackendRegistration,
    BaselineExecutionBackend,
    BaselineExecutionRunner,
    ExecutionDisposition,
    ExecutionPolicy,
    FileExecutionPolicyProvider,
)
from nexuskv.execution.backend import StagedCopyExecutionBackend
from nexuskv.execution.catalog import BackendCatalog
from nexuskv.execution.policy import SCHEMA_VERSION
from nexuskv.execution.types import BackendActionKind, BackendActionStatus, FallbackReason, MaterializationRequest
from nexuskv.testsupport.matches import make_lookup_outcome


def _policy_dict(**overrides):
    data = ExecutionPolicy.default().to_dict()
    for key, value in overrides.items():
        data[key] = value
    return data


class ExecutionPolicyTest(unittest.TestCase):
    def test_python_policy_parsing_succeeds_for_valid_config(self) -> None:
        policy = ExecutionPolicy.from_dict(_policy_dict())
        self.assertEqual(policy.schema_version, SCHEMA_VERSION)
        self.assertEqual(policy.backend_priority_order[0], TransferBackend.STAGED_COPY)

    def test_python_policy_rejects_invalid_priority(self) -> None:
        with self.assertRaises(ValueError):
            ExecutionPolicy.from_dict(
                _policy_dict(
                    enabled_transfer_backends=["baseline_transport"],
                    backend_priority_order=["staged_copy"],
                )
            )

    def test_python_policy_parses_backend_overlay(self) -> None:
        policy = ExecutionPolicy.from_dict(
            _policy_dict(
                backend_overlays={
                    "staged_copy": {
                        "enabled": True,
                        "priority_override": 0,
                        "allowed_target_tiers": ["device"],
                        "allowed_materialization_capabilities": ["partial", "prefetch"],
                        "allow_degraded_selection": False,
                    }
                }
            )
        )

        overlay = policy.backend_overlays[TransferBackend.STAGED_COPY]
        self.assertEqual(overlay.priority_override, 0)
        self.assertEqual(overlay.allowed_target_tiers, (TierKind.DEVICE,))
        self.assertEqual(
            overlay.allowed_materialization_capabilities,
            (MaterializationCapability.PARTIAL, MaterializationCapability.PREFETCH),
        )
        self.assertFalse(policy.allows_degraded_backend_selection(TransferBackend.STAGED_COPY))

    def test_file_policy_provider_loads_initial_policy(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "execution-policy.json"
            path.write_text(ExecutionPolicy.default().to_json(), encoding="utf-8")

            provider = FileExecutionPolicyProvider(policy_path=path)

        self.assertFalse(provider.status().using_default_policy)
        self.assertEqual(provider.active_policy().schema_version, SCHEMA_VERSION)

    def test_file_policy_provider_missing_explicit_path_is_clear(self) -> None:
        with self.assertRaises(FileNotFoundError):
            FileExecutionPolicyProvider(policy_path="/tmp/nexuskv-missing-policy.json")

    def test_invalid_reload_preserves_last_known_good_policy(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "execution-policy.json"
            path.write_text(ExecutionPolicy.default().to_json(), encoding="utf-8")
            provider = FileExecutionPolicyProvider(policy_path=path)

            path.write_text("{\"schema_version\":\"bad\"}", encoding="utf-8")
            status = provider.reload()

        self.assertFalse(status.last_reload_succeeded)
        self.assertIsNotNone(status.last_error)
        self.assertEqual(provider.active_policy().schema_version, SCHEMA_VERSION)
        self.assertEqual(provider.status().generation, 1)

    def test_backend_priority_from_policy_controls_catalog_registration(self) -> None:
        policy = ExecutionPolicy.from_dict(
            _policy_dict(
                enabled_transfer_backends=["baseline_transport", "staged_copy"],
                backend_priority_order=["baseline_transport", "staged_copy"],
            )
        )
        catalog = BackendCatalog(policy=policy)
        baseline = BaselineExecutionBackend()
        staged = StagedCopyExecutionBackend()
        catalog.register(
            BackendRegistration(
                name=staged.backend_name,
                backend=staged,
                transfer_backend=TransferBackend.STAGED_COPY,
                action_kinds=(BackendActionKind.MATERIALIZE,),
                source_tiers=(TierKind.REMOTE_SHARED,),
                target_tiers=(TierKind.HOST_DRAM,),
                priority=50,
            )
        )
        catalog.register(
            BackendRegistration(
                name=baseline.backend_name,
                backend=baseline,
                transfer_backend=TransferBackend.BASELINE_TRANSPORT,
                action_kinds=(BackendActionKind.MATERIALIZE,),
                source_tiers=(TierKind.REMOTE_SHARED,),
                target_tiers=(TierKind.HOST_DRAM,),
                priority=50,
            )
        )
        connector = VLLMConnector()
        context = VLLMLifecycleContext(
            tenant="tenant-a",
            namespace="chat",
            model="llama-70b",
            tokens=[1, 2, 3],
            descriptor=connector.default_descriptor(),
            page_id=1,
        )
        lookup = make_lookup_outcome(connector, [1, 2, 3], [], page_id=1)

        from nexuskv.execution.types import (
            BackendActionRequest,
            CapabilityCheckResult,
            MaterializationDecision,
            SourceTier,
            TargetTier,
            TransferMode,
        )

        selection = catalog.select(
            BackendActionRequest(
                kind=BackendActionKind.MATERIALIZE,
                hook="request_start",
                context=context,
                lookup=lookup,
                decision=MaterializationDecision(
                    disposition=ExecutionDisposition.MATERIALIZE,
                    source=SourceTier(tier=TierKind.REMOTE_SHARED, locator="remote://entry"),
                    target=TargetTier(tier=TierKind.HOST_DRAM),
                    transfer=TransferMode(selected_backend=None),
                    capability_check=CapabilityCheckResult(
                        supported=True,
                        degraded=False,
                        required_capability="full",
                        fallback_reason=None,
                        selected_backend=None,
                    ),
                    fallback_reason=None,
                ),
            )
        )

        self.assertIsNotNone(selection)
        _, details = selection
        self.assertEqual(details.backend_name, "baseline-execution-backend")

    def test_disabled_backend_is_not_selected(self) -> None:
        policy = ExecutionPolicy.from_dict(
            _policy_dict(
                enabled_transfer_backends=["baseline_transport"],
                backend_priority_order=["baseline_transport"],
            )
        )
        catalog = BackendCatalog(policy=policy)
        baseline = BaselineExecutionBackend()
        staged = StagedCopyExecutionBackend()
        catalog.register(
            BackendRegistration(
                name=baseline.backend_name,
                backend=baseline,
                transfer_backend=TransferBackend.BASELINE_TRANSPORT,
                action_kinds=(BackendActionKind.MATERIALIZE,),
                source_tiers=(TierKind.REMOTE_SHARED,),
                target_tiers=(TierKind.HOST_DRAM,),
                priority=10,
            )
        )
        catalog.register(
            BackendRegistration(
                name=staged.backend_name,
                backend=staged,
                transfer_backend=TransferBackend.STAGED_COPY,
                action_kinds=(BackendActionKind.MATERIALIZE,),
                source_tiers=(TierKind.REMOTE_SHARED,),
                target_tiers=(TierKind.HOST_DRAM,),
                priority=5,
            )
        )
        connector = VLLMConnector()
        context = VLLMLifecycleContext(
            tenant="tenant-a",
            namespace="chat",
            model="llama-70b",
            tokens=[4, 5, 6],
            descriptor=connector.default_descriptor(),
            page_id=2,
        )
        lookup = make_lookup_outcome(connector, [4, 5, 6], [], page_id=2)

        from nexuskv.execution.types import (
            BackendActionRequest,
            CapabilityCheckResult,
            MaterializationDecision,
            SourceTier,
            TargetTier,
            TransferMode,
        )

        selection = catalog.select(
            BackendActionRequest(
                kind=BackendActionKind.MATERIALIZE,
                hook="request_start",
                context=context,
                lookup=lookup,
                decision=MaterializationDecision(
                    disposition=ExecutionDisposition.MATERIALIZE,
                    source=SourceTier(tier=TierKind.REMOTE_SHARED, locator="remote://entry"),
                    target=TargetTier(tier=TierKind.HOST_DRAM),
                    transfer=TransferMode(selected_backend=TransferBackend.STAGED_COPY),
                    capability_check=CapabilityCheckResult(
                        supported=True,
                        degraded=False,
                        required_capability="full",
                        fallback_reason=None,
                        selected_backend=TransferBackend.STAGED_COPY,
                    ),
                    fallback_reason=None,
                ),
            )
        )

        self.assertIsNotNone(selection)
        _, details = selection
        self.assertNotEqual(details.transfer_backend, TransferBackend.STAGED_COPY)

    def test_disallowed_target_tier_falls_back_safely(self) -> None:
        connector = VLLMConnector()
        policy = ExecutionPolicy.from_dict(
            _policy_dict(
                allowed_target_tiers=["local_ssd"],
                default_fallback_behavior="skip",
                recompute_fallback_policy={
                    "on_cache_miss": False,
                    "on_backend_rejection": False,
                    "on_capability_miss": False,
                    "on_policy_denial": False,
                },
            )
        )
        runner = BaselineExecutionRunner(policy=policy)
        context = VLLMLifecycleContext(
            tenant="tenant-a",
            namespace="chat",
            model="llama-70b",
            tokens=[7, 8, 9],
            descriptor=connector.default_descriptor(),
            page_id=3,
        )
        lookup = make_lookup_outcome(connector, [7, 8, 9], [], page_id=3)

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

        self.assertEqual(outcome.primary.result.status, BackendActionStatus.SKIPPED)
        self.assertEqual(outcome.primary.result.fallback_reason, FallbackReason.ENGINE_POLICY)

    def test_recompute_fallback_behavior_is_controlled_by_policy(self) -> None:
        connector = VLLMConnector()
        policy = ExecutionPolicy.from_dict(
            _policy_dict(
                default_fallback_behavior="skip",
                recompute_fallback_policy={
                    "on_cache_miss": False,
                    "on_backend_rejection": True,
                    "on_capability_miss": True,
                    "on_policy_denial": False,
                },
            )
        )
        runner = BaselineExecutionRunner(policy=policy, catalog=BackendCatalog())
        context = VLLMLifecycleContext(
            tenant="tenant-a",
            namespace="chat",
            model="llama-70b",
            tokens=[10, 11, 12],
            descriptor=connector.default_descriptor(),
            page_id=4,
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

        self.assertEqual(outcome.primary.decision.disposition, ExecutionDisposition.SKIP)
        self.assertEqual(outcome.primary.result.status, BackendActionStatus.FALLBACK)
        self.assertEqual(outcome.primary.result.executed_kind.value, "skip")

    def test_overlay_disables_backend(self) -> None:
        policy = ExecutionPolicy.default()
        policy.backend_overlays[TransferBackend.STAGED_COPY] = BackendCapabilityOverlay(enabled=False)
        catalog = BackendCatalog(policy=policy)
        baseline = BaselineExecutionBackend()
        staged = StagedCopyExecutionBackend()
        catalog.register(
            BackendRegistration(
                name=staged.backend_name,
                backend=staged,
                transfer_backend=TransferBackend.STAGED_COPY,
                action_kinds=(BackendActionKind.MATERIALIZE,),
                source_tiers=(TierKind.REMOTE_SHARED,),
                target_tiers=(TierKind.HOST_DRAM,),
                priority=1,
            )
        )
        catalog.register(
            BackendRegistration(
                name=baseline.backend_name,
                backend=baseline,
                transfer_backend=TransferBackend.BASELINE_TRANSPORT,
                action_kinds=(BackendActionKind.MATERIALIZE,),
                source_tiers=(TierKind.REMOTE_SHARED,),
                target_tiers=(TierKind.HOST_DRAM,),
                priority=10,
            )
        )
        connector = VLLMConnector()
        context = VLLMLifecycleContext(
            tenant="tenant-a",
            namespace="chat",
            model="llama-70b",
            tokens=[13, 14, 15],
            descriptor=connector.default_descriptor(),
            page_id=5,
        )
        lookup = make_lookup_outcome(connector, [13, 14, 15], [], page_id=5)

        from nexuskv.execution.types import (
            BackendActionRequest,
            CapabilityCheckResult,
            MaterializationDecision,
            SourceTier,
            TargetTier,
            TransferMode,
        )

        selection = catalog.select(
            BackendActionRequest(
                kind=BackendActionKind.MATERIALIZE,
                hook="request_start",
                context=context,
                lookup=lookup,
                decision=MaterializationDecision(
                    disposition=ExecutionDisposition.MATERIALIZE,
                    source=SourceTier(tier=TierKind.REMOTE_SHARED, locator="remote://entry"),
                    target=TargetTier(tier=TierKind.HOST_DRAM),
                    transfer=TransferMode(selected_backend=None),
                    capability_check=CapabilityCheckResult(
                        supported=True,
                        degraded=False,
                        required_capability="full",
                        fallback_reason=None,
                        selected_backend=None,
                    ),
                    fallback_reason=None,
                ),
            )
        )

        self.assertIsNotNone(selection)
        _, details = selection
        self.assertEqual(details.transfer_backend, TransferBackend.BASELINE_TRANSPORT)

    def test_overlay_changes_priority_deterministically(self) -> None:
        policy = ExecutionPolicy.default()
        policy.backend_overlays[TransferBackend.BASELINE_TRANSPORT] = BackendCapabilityOverlay(priority_override=0)
        policy.backend_overlays[TransferBackend.STAGED_COPY] = BackendCapabilityOverlay(priority_override=10)
        catalog = BackendCatalog(policy=policy)
        baseline = BaselineExecutionBackend()
        staged = StagedCopyExecutionBackend()
        for backend_name, backend, transfer_backend in (
            (staged.backend_name, staged, TransferBackend.STAGED_COPY),
            (baseline.backend_name, baseline, TransferBackend.BASELINE_TRANSPORT),
        ):
            catalog.register(
                BackendRegistration(
                    name=backend_name,
                    backend=backend,
                    transfer_backend=transfer_backend,
                    action_kinds=(BackendActionKind.MATERIALIZE,),
                    source_tiers=(TierKind.REMOTE_SHARED,),
                    target_tiers=(TierKind.HOST_DRAM,),
                    priority=100,
                )
            )
        connector = VLLMConnector()
        context = VLLMLifecycleContext(
            tenant="tenant-a",
            namespace="chat",
            model="llama-70b",
            tokens=[16, 17, 18],
            descriptor=connector.default_descriptor(),
            page_id=6,
        )
        lookup = make_lookup_outcome(connector, [16, 17, 18], [], page_id=6)

        from nexuskv.execution.types import (
            BackendActionRequest,
            CapabilityCheckResult,
            MaterializationDecision,
            SourceTier,
            TargetTier,
            TransferMode,
        )

        selection = catalog.select(
            BackendActionRequest(
                kind=BackendActionKind.MATERIALIZE,
                hook="request_start",
                context=context,
                lookup=lookup,
                decision=MaterializationDecision(
                    disposition=ExecutionDisposition.MATERIALIZE,
                    source=SourceTier(tier=TierKind.REMOTE_SHARED, locator="remote://entry"),
                    target=TargetTier(tier=TierKind.HOST_DRAM),
                    transfer=TransferMode(selected_backend=None),
                    capability_check=CapabilityCheckResult(
                        supported=True,
                        degraded=False,
                        required_capability="full",
                        fallback_reason=None,
                        selected_backend=None,
                    ),
                    fallback_reason=None,
                ),
            )
        )

        self.assertIsNotNone(selection)
        _, details = selection
        self.assertEqual(details.transfer_backend, TransferBackend.BASELINE_TRANSPORT)

    def test_overlay_restricts_capability(self) -> None:
        connector = VLLMConnector()
        policy = ExecutionPolicy.default()
        policy.backend_overlays[TransferBackend.STAGED_COPY] = BackendCapabilityOverlay(
            allowed_materialization_capabilities=(MaterializationCapability.PREFETCH,)
        )
        runner = BaselineExecutionRunner(policy=policy)
        context = VLLMLifecycleContext(
            tenant="tenant-a",
            namespace="chat",
            model="llama-70b",
            tokens=[19, 20, 21],
            descriptor=connector.default_descriptor(),
            page_id=7,
        )
        lookup = make_lookup_outcome(connector, [19, 20, 21], [], page_id=7)

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

    def test_valid_reload_changes_future_backend_selection(self) -> None:
        connector = VLLMConnector()
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "execution-policy.json"
            initial = _policy_dict(
                allowed_target_tiers=["host_dram"],
                allowed_buffer_kinds=["host_pinned"],
            )
            path.write_text(ExecutionPolicy.from_dict(initial).to_json(), encoding="utf-8")
            provider = FileExecutionPolicyProvider(policy_path=path)
            runner = BaselineExecutionRunner(policy_provider=provider)
            context = VLLMLifecycleContext(
                tenant="tenant-a",
                namespace="chat",
                model="llama-70b",
                tokens=[22, 23, 24],
                descriptor=connector.default_descriptor(),
                page_id=8,
            )
            lookup = make_lookup_outcome(connector, [22, 23, 24], [], page_id=8)

            first = runner.execute(
                MaterializationRequest(
                    hook="request_start",
                    context=context,
                    lookup=lookup,
                    preferred_backend=None,
                    allow_store_after_stage=False,
                    enable_prefetch=False,
                )
            )
            path.write_text(ExecutionPolicy.default().to_json(), encoding="utf-8")
            status = provider.reload()
            second = runner.execute(
                MaterializationRequest(
                    hook="request_start",
                    context=context,
                    lookup=lookup,
                    preferred_backend=None,
                    allow_store_after_stage=False,
                    enable_prefetch=False,
                )
            )

        self.assertTrue(status.last_reload_succeeded)
        self.assertTrue(status.changed)
        self.assertEqual(first.primary.result.target.tier, TierKind.HOST_DRAM)
        self.assertEqual(second.primary.result.target.tier, TierKind.DEVICE)


if __name__ == "__main__":
    unittest.main()
