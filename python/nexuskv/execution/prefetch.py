from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from enum import StrEnum

from nexuskv.execution.types import FallbackReason, MaterializationDecision, MaterializationRequest


class PrefetchJobStatus(StrEnum):
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    CANCELLED = "cancelled"
    EXPIRED = "expired"


@dataclass(slots=True)
class PrefetchJob:
    job_id: str
    request: MaterializationRequest
    decision: MaterializationDecision
    created_at_sec: float
    deadline_sec: float
    status: PrefetchJobStatus = PrefetchJobStatus.PENDING
    completed_at_sec: float | None = None
    bytes_transferred: int = 0

    @property
    def is_expired(self) -> bool:
        if self.status in {
            PrefetchJobStatus.COMPLETED,
            PrefetchJobStatus.CANCELLED,
            PrefetchJobStatus.EXPIRED,
        }:
            return False
        return time.time() > self.deadline_sec


@dataclass(slots=True)
class PrefetchScheduler:
    max_concurrent_prefetches: int = 8
    default_ttl_sec: float = 0.5  # 500ms default prefetch deadline
    _lock: threading.Lock = field(default_factory=threading.Lock, init=False)
    _jobs: dict[str, PrefetchJob] = field(default_factory=dict, init=False)

    def submit_prefetch(
        self,
        job_id: str,
        request: MaterializationRequest,
        decision: MaterializationDecision,
        ttl_sec: float | None = None,
        bytes_hint: int = 0,
    ) -> tuple[PrefetchJob | None, FallbackReason | None]:
        with self._lock:
            # Purge expired jobs
            self._purge_expired_locked()

            active_count = sum(
                1
                for j in self._jobs.values()
                if j.status in {PrefetchJobStatus.PENDING, PrefetchJobStatus.IN_PROGRESS}
            )
            if active_count >= self.max_concurrent_prefetches:
                return None, FallbackReason.ENGINE_POLICY

            now = time.time()
            deadline = now + (ttl_sec if ttl_sec is not None else self.default_ttl_sec)
            job = PrefetchJob(
                job_id=job_id,
                request=request,
                decision=decision,
                created_at_sec=now,
                deadline_sec=deadline,
                status=PrefetchJobStatus.IN_PROGRESS,
                bytes_transferred=bytes_hint,
            )
            self._jobs[job_id] = job
            return job, None

    def complete_job(self, job_id: str) -> bool:
        with self._lock:
            job = self._jobs.get(job_id)
            if job is None or job.status != PrefetchJobStatus.IN_PROGRESS:
                return False
            if time.time() > job.deadline_sec:
                job.status = PrefetchJobStatus.EXPIRED
                return False
            job.status = PrefetchJobStatus.COMPLETED
            job.completed_at_sec = time.time()
            return True

    def cancel_job(self, job_id: str) -> bool:
        with self._lock:
            job = self._jobs.get(job_id)
            if job is None or job.status in {
                PrefetchJobStatus.COMPLETED,
                PrefetchJobStatus.CANCELLED,
                PrefetchJobStatus.EXPIRED,
            }:
                return False
            job.status = PrefetchJobStatus.CANCELLED
            return True

    def get_job_status(self, job_id: str) -> PrefetchJobStatus | None:
        with self._lock:
            self._purge_expired_locked()
            job = self._jobs.get(job_id)
            return job.status if job else None

    def _purge_expired_locked(self) -> None:
        now = time.time()
        for job in list(self._jobs.values()):
            if (
                job.status in {PrefetchJobStatus.PENDING, PrefetchJobStatus.IN_PROGRESS}
                and now > job.deadline_sec
            ):
                job.status = PrefetchJobStatus.EXPIRED

    def reset(self) -> None:
        with self._lock:
            self._jobs.clear()
