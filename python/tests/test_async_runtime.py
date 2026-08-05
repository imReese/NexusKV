import time
import unittest

from nexuskv.connectors.base import LookupOutcome, LookupStatus, VLLMLifecycleContext
from nexuskv.connectors.vllm.connector import VLLMConnector
from nexuskv.contracts.generated import (
    TierKind,
    TransferBackend,
)
from nexuskv.execution.prefetch import PrefetchJobStatus, PrefetchScheduler
from nexuskv.execution.transfer import TransferSessionTracker, TransferStatus
from nexuskv.execution.types import (
    CapabilityCheckResult,
    ExecutionDisposition,
    MaterializationDecision,
    MaterializationRequest,
    SourceTier,
    TargetTier,
    TransferMode,
)


class TestAsyncRuntime(unittest.TestCase):
    def test_prefetch_scheduler_deadline_expiration_and_cancellation(self):
        scheduler = PrefetchScheduler(max_concurrent_prefetches=2, default_ttl_sec=0.05)

        descriptor = VLLMConnector().default_descriptor()
        ctx = VLLMLifecycleContext(
            tenant="tenant1",
            namespace="ns1",
            model="m1",
            tokens=[1, 2, 3],
            descriptor=descriptor,
        )
        req = MaterializationRequest(
            hook="prefill",
            context=ctx,
            lookup=LookupOutcome(
                query=None, status=LookupStatus.MISS, match=None, partial_plan=None
            ),
            preferred_backend=None,
            allow_store_after_stage=False,
            enable_prefetch=True,
        )
        dec = MaterializationDecision(
            disposition=ExecutionDisposition.PREFETCH,
            source=SourceTier(tier=TierKind.HOST_DRAM),
            target=TargetTier(tier=TierKind.DEVICE),
            transfer=TransferMode(selected_backend=TransferBackend.STAGED_COPY),
            capability_check=CapabilityCheckResult(
                supported=True,
                degraded=False,
                required_capability=None,
                fallback_reason=None,
                selected_backend=TransferBackend.STAGED_COPY,
            ),
            fallback_reason=None,
        )

        job, err = scheduler.submit_prefetch("job1", req, dec, ttl_sec=0.05)
        self.assertIsNotNone(job)
        self.assertIsNone(err)

        # Job is in progress
        self.assertEqual(scheduler.get_job_status("job1"), PrefetchJobStatus.IN_PROGRESS)

        # Sleep past deadline
        time.sleep(0.08)
        self.assertEqual(scheduler.get_job_status("job1"), PrefetchJobStatus.EXPIRED)

        # Attempting to complete expired job returns False
        self.assertFalse(scheduler.complete_job("job1"))

    def test_transfer_session_tracker_lifecycle(self):
        tracker = TransferSessionTracker()
        session = tracker.register_session(
            session_id="sess1",
            backend=TransferBackend.RDMA,
            source_locator="remote://node1",
            target_locator="device://cuda0",
            total_bytes=1000,
        )
        self.assertEqual(session.status, TransferStatus.IN_FLIGHT)

        tracker.update_progress("sess1", 500)
        self.assertEqual(tracker.get_session("sess1").completed_bytes, 500)

        tracker.mark_completed("sess1")
        self.assertEqual(tracker.get_session("sess1").status, TransferStatus.COMPLETED)
        self.assertTrue(tracker.get_session("sess1").is_complete)


if __name__ == "__main__":
    unittest.main()
