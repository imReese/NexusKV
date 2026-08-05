from __future__ import annotations

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


class SpeculativePrefetchEngine:
    """Speculative Intent Prefetching Engine.

    Pipelining long-context prefix blocks from Host DRAM/SSD to Device HBM
    based on token prefix intent before the decode phase begins.
    """

    def __init__(self, allocator: HbmBlockAllocator | None = None) -> None:
        self.allocator = allocator or HbmBlockAllocator()
        self.active_tasks: dict[str, PrefetchTask] = {}

    def submit_intent_prefetch(
        self,
        task_id: str,
        prefix_tokens: Sequence[int],
        predicted_suffix_tokens: Sequence[int],
        target_tier: str = "HBM",
    ) -> PrefetchTask:
        total_tokens = len(prefix_tokens) + len(predicted_suffix_tokens)
        blocks_needed = max(1, total_tokens // 100)

        allocated_ids = []
        for _ in range(blocks_needed):
            block = self.allocator.allocate_block()
            allocated_ids.append(block.block_id)

        task = PrefetchTask(
            task_id=task_id,
            prompt_tokens=list(prefix_tokens),
            predicted_suffix_tokens=list(predicted_suffix_tokens),
            target_tier=target_tier,
            status="COMPLETED",
            allocated_block_ids=allocated_ids,
        )
        self.active_tasks[task_id] = task
        return task

    def get_task(self, task_id: str) -> PrefetchTask | None:
        return self.active_tasks.get(task_id)
