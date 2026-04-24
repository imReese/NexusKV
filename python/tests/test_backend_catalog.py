import unittest

from nexuskv.connectors.base import SGLangLifecycleContext, VLLMLifecycleContext
from nexuskv.connectors.sglang.connector import SGLangConnector
from nexuskv.connectors.vllm.connector import VLLMConnector
from nexuskv.contracts.generated import MaterializationCapability, TierKind, TransferBackend
from nexuskv.execution.backend import BaselineExecutionBackend
from nexuskv.execution.catalog import BackendCatalog, BackendRegistration
from nexuskv.execution.runner import BaselineExecutionRunner
from nexuskv.execution.types import (
    BackendActionKind,
    BackendActionRequest,
    CapabilityCheckResult,
    ExecutionDisposition,
    MaterializationDecision,
    SourceTier,
    TargetTier,
    TransferMode,
)
from nexuskv.testsupport.matches import make_lookup_outcome


class BackendCatalogTest(unittest.TestCase):
    def test_catalog_selects_exact_backend_match_deterministically(self) -> None:
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
        catalog = runner._build_default_catalog()

        selection = catalog.select(
            BackendActionRequest(
                kind=BackendActionKind.MATERIALIZE,
                hook="request_start",
                context=context,
                lookup=lookup,
                decision=MaterializationDecision(
                    disposition=ExecutionDisposition.MATERIALIZE,
                    source=SourceTier(tier=TierKind.REMOTE_SHARED, locator="remote://entry-1"),
                    target=TargetTier(tier=TierKind.DEVICE),
                    transfer=TransferMode(selected_backend=TransferBackend.STAGED_COPY),
                    capability_check=CapabilityCheckResult(
                        supported=True,
                        degraded=False,
                        required_capability=MaterializationCapability.FULL.value,
                        fallback_reason=None,
                        selected_backend=TransferBackend.STAGED_COPY,
                    ),
                    fallback_reason=None,
                ),
            )
        )

        self.assertIsNotNone(selection)
        _, details = selection
        self.assertEqual(details.backend_name, "staged-copy-backend")
        self.assertFalse(details.degraded)
        self.assertEqual(details.transfer_backend, TransferBackend.STAGED_COPY)

    def test_catalog_degrades_to_safe_backend_when_exact_match_is_missing(self) -> None:
        connector = SGLangConnector()
        baseline = BaselineExecutionBackend()
        catalog = BackendCatalog()
        catalog.register(
            BackendRegistration(
                name=baseline.backend_name,
                backend=baseline,
                transfer_backend=TransferBackend.BASELINE_TRANSPORT,
                action_kinds=(BackendActionKind.MATERIALIZE,),
                target_tiers=(TierKind.HOST_DRAM,),
                priority=10,
            )
        )
        context = SGLangLifecycleContext(
            tenant="tenant-a",
            namespace="chat",
            model="llama-70b",
            tokens=[4, 5, 6],
            descriptor=connector.default_descriptor(),
        )
        lookup = make_lookup_outcome(connector, [4, 5, 6], [])

        selection = catalog.select(
            BackendActionRequest(
                kind=BackendActionKind.MATERIALIZE,
                hook="prefill",
                context=context,
                lookup=lookup,
                decision=MaterializationDecision(
                    disposition=ExecutionDisposition.MATERIALIZE,
                    source=SourceTier(tier=TierKind.REMOTE_SHARED, locator="remote://entry-1"),
                    target=TargetTier(tier=TierKind.HOST_DRAM),
                    transfer=TransferMode(selected_backend=TransferBackend.STAGED_COPY),
                    capability_check=CapabilityCheckResult(
                        supported=True,
                        degraded=False,
                        required_capability=MaterializationCapability.FULL.value,
                        fallback_reason=None,
                        selected_backend=TransferBackend.STAGED_COPY,
                    ),
                    fallback_reason=None,
                ),
            )
        )

        self.assertIsNotNone(selection)
        _, details = selection
        self.assertEqual(details.backend_name, "baseline-execution-backend")
        self.assertTrue(details.degraded)
        self.assertEqual(details.transfer_backend, TransferBackend.BASELINE_TRANSPORT)

    def test_catalog_returns_none_when_no_backend_matches(self) -> None:
        connector = VLLMConnector()
        catalog = BackendCatalog()
        context = VLLMLifecycleContext(
            tenant="tenant-a",
            namespace="chat",
            model="llama-70b",
            tokens=[8, 9, 10],
            descriptor=connector.default_descriptor(),
            page_id=2,
        )
        lookup = make_lookup_outcome(connector, [8, 9, 10], [], page_id=2)

        selection = catalog.select(
            BackendActionRequest(
                kind=BackendActionKind.MATERIALIZE,
                hook="request_start",
                context=context,
                lookup=lookup,
                decision=MaterializationDecision(
                    disposition=ExecutionDisposition.MATERIALIZE,
                    source=SourceTier(tier=TierKind.REMOTE_SHARED, locator="remote://entry-2"),
                    target=TargetTier(tier=TierKind.LOCAL_SSD),
                    transfer=TransferMode(selected_backend=TransferBackend.STAGED_COPY),
                    capability_check=CapabilityCheckResult(
                        supported=True,
                        degraded=False,
                        required_capability=MaterializationCapability.FULL.value,
                        fallback_reason=None,
                        selected_backend=TransferBackend.STAGED_COPY,
                    ),
                    fallback_reason=None,
                ),
            )
        )

        self.assertIsNone(selection)


if __name__ == "__main__":
    unittest.main()
