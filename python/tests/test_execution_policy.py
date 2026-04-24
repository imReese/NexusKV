import unittest

from nexuskv.connectors.base import LookupOutcome, LookupStatus, VLLMLifecycleContext
from nexuskv.connectors.vllm.connector import VLLMConnector
from nexuskv.contracts.generated import TierKind, TransferBackend
from nexuskv.execution import BackendRegistration, BaselineExecutionBackend, BaselineExecutionRunner, ExecutionDisposition, ExecutionPolicy
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

        self.assertIsNone(selection)

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


if __name__ == "__main__":
    unittest.main()
