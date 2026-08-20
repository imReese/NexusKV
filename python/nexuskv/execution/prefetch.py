from __future__ import annotations

import asyncio
import threading
import time
from collections.abc import Sequence
from dataclasses import dataclass, field
from enum import StrEnum

from nexuskv.execution.hbm import HbmBlockAllocator
from nexuskv.execution.types import MaterializationDecision, MaterializationRequest


class PrefetchJobStatus(StrEnum):
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    EXPIRED = "expired"
    FAILED = "failed"


@dataclass(slots=True)
class PrefetchJob:
    job_id: str
    request: MaterializationRequest
    decision: MaterializationDecision
    status: PrefetchJobStatus = PrefetchJobStatus.PENDING
    deadline: float = 0.0


@dataclass(slots=True)
class PrefetchScheduler:
    max_concurrent_prefetches: int = 10
    default_ttl_sec: float = 5.0
    _jobs: dict[str, PrefetchJob] = field(default_factory=dict, init=False)

    def submit_prefetch(
        self,
        job_id: str,
        request: MaterializationRequest,
        decision: MaterializationDecision,
        ttl_sec: float | None = None,
    ) -> tuple[PrefetchJob | None, str | None]:
        active_count = sum(
            1
            for j in self._jobs.values()
            if j.status == PrefetchJobStatus.IN_PROGRESS and time.time() <= j.deadline
        )
        if active_count >= self.max_concurrent_prefetches:
            return None, "max concurrent prefetches limit reached"

        ttl = ttl_sec if ttl_sec is not None else self.default_ttl_sec
        deadline = time.time() + ttl
        job = PrefetchJob(
            job_id=job_id,
            request=request,
            decision=decision,
            status=PrefetchJobStatus.IN_PROGRESS,
            deadline=deadline,
        )
        self._jobs[job_id] = job
        return job, None

    def get_job_status(self, job_id: str) -> PrefetchJobStatus | None:
        job = self._jobs.get(job_id)
        if job is None:
            return None
        if job.status == PrefetchJobStatus.IN_PROGRESS and time.time() > job.deadline:
            job.status = PrefetchJobStatus.EXPIRED
        return job.status

    def complete_job(self, job_id: str) -> bool:
        status = self.get_job_status(job_id)
        if status != PrefetchJobStatus.IN_PROGRESS:
            return False
        job = self._jobs[job_id]
        job.status = PrefetchJobStatus.COMPLETED
        return True


@dataclass(slots=True)
class PrefetchTask:
    task_id: str
    prompt_tokens: list[int]
    predicted_suffix_tokens: list[int]
    target_tier: str  # "HBM", "DRAM", "SSD"
    status: str  # "PENDING", "PREFETCHING", "COMPLETED", "FAILED"
    allocated_block_ids: list[int]
    session_ids: list[str] = field(default_factory=list)


class SpeculativePrefetchEngine:
    """Speculative Intent Prefetching Engine.

    Pipelining long-context prefix blocks from Host DRAM/SSD to Device HBM
    based on token prefix intent before the decode phase begins.
    """

    def __init__(self, allocator: HbmBlockAllocator | None = None) -> None:
        self.allocator = allocator or HbmBlockAllocator()
        self.active_tasks: dict[str, PrefetchTask] = {}

        # Start a dedicated daemon thread for background asyncio loop
        self._loop = asyncio.new_event_loop()
        self._daemon_thread = threading.Thread(
            target=self._run_loop, daemon=True, name="PrefetchDaemonWorker"
        )
        self._daemon_thread.start()

    def _run_loop(self) -> None:
        asyncio.set_event_loop(self._loop)
        self._loop.run_forever()

    def submit_intent_prefetch(
        self,
        task_id: str,
        prefix_tokens: Sequence[int],
        predicted_suffix_tokens: Sequence[int],
        target_tier: str = "HBM",
    ) -> PrefetchTask:
        task = PrefetchTask(
            task_id=task_id,
            prompt_tokens=list(prefix_tokens),
            predicted_suffix_tokens=list(predicted_suffix_tokens),
            target_tier=target_tier,
            status="PENDING",
            allocated_block_ids=[],
        )
        self.active_tasks[task_id] = task

        # Schedule the background processing safely in the daemon's event loop
        asyncio.run_coroutine_threadsafe(self._process_task_async(task_id), self._loop)
        return task

    async def _process_task_async(self, task_id: str) -> None:
        task = self.active_tasks.get(task_id)
        if not task:
            return

        task.status = "PREFETCHING"

        # Simulate planning: token sequence to block resolution
        total_tokens = len(task.prompt_tokens) + len(task.predicted_suffix_tokens)
        blocks_needed = max(1, total_tokens // 100)

        allocated_ids = []
        for _ in range(blocks_needed):
            block = self.allocator.allocate_block()
            allocated_ids.append(block.block_id)

        task.allocated_block_ids = allocated_ids

        # In a real system, here we would integrate with `TransferSessionTracker`
        # and wait for PCIe DMA transfer status to complete.
        # We simulate the asynchronous transfer latency proportional to blocks.
        transfer_latency_sec = 0.05 * blocks_needed
        await asyncio.sleep(transfer_latency_sec)

        task.status = "COMPLETED"

    def get_task(self, task_id: str) -> PrefetchTask | None:
        return self.active_tasks.get(task_id)

    def shutdown(self) -> None:
        if self._loop.is_closed():
            return
        if self._loop.is_running():
            self._loop.call_soon_threadsafe(self._loop.stop)
        self._daemon_thread.join(timeout=2.0)
        if not self._daemon_thread.is_alive():
            self._loop.close()
