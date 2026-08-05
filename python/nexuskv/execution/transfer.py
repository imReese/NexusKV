from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field

from nexuskv.contracts.generated import TransferBackend
from nexuskv.execution.types import TransferStatus


@dataclass(slots=True)
class ActiveTransferSession:
    session_id: str
    backend: TransferBackend
    source_locator: str
    target_locator: str
    total_bytes: int
    started_at_sec: float
    expected_duration_sec: float
    status: TransferStatus = TransferStatus.INITIATED
    completed_bytes: int = 0

    @property
    def is_complete(self) -> bool:
        return self.status == TransferStatus.COMPLETED

    @property
    def is_failed_or_cancelled(self) -> bool:
        return self.status in {TransferStatus.FAILED, TransferStatus.CANCELLED}


@dataclass(slots=True)
class TransferSessionTracker:
    _lock: threading.Lock = field(default_factory=threading.Lock, init=False)
    _sessions: dict[str, ActiveTransferSession] = field(default_factory=dict, init=False)

    def register_session(
        self,
        session_id: str,
        backend: TransferBackend,
        source_locator: str,
        target_locator: str,
        total_bytes: int,
        estimated_bw_bps: float = 1.0e10,  # 10GB/s default
    ) -> ActiveTransferSession:
        now = time.time()
        duration = total_bytes / estimated_bw_bps if estimated_bw_bps > 0 else 0.001
        session = ActiveTransferSession(
            session_id=session_id,
            backend=backend,
            source_locator=source_locator,
            target_locator=target_locator,
            total_bytes=total_bytes,
            started_at_sec=now,
            expected_duration_sec=duration,
            status=TransferStatus.IN_FLIGHT,
        )
        with self._lock:
            self._sessions[session_id] = session
        return session

    def update_progress(self, session_id: str, bytes_done: int) -> bool:
        with self._lock:
            session = self._sessions.get(session_id)
            if not session or session.is_complete:
                return False
            session.completed_bytes = min(session.total_bytes, bytes_done)
            if session.completed_bytes >= session.total_bytes:
                session.status = TransferStatus.COMPLETED
            return True

    def mark_completed(self, session_id: str) -> bool:
        with self._lock:
            session = self._sessions.get(session_id)
            if not session:
                return False
            session.completed_bytes = session.total_bytes
            session.status = TransferStatus.COMPLETED
            return True

    def mark_failed_or_cancelled(self, session_id: str, cancelled: bool = False) -> bool:
        with self._lock:
            session = self._sessions.get(session_id)
            if not session:
                return False
            session.status = TransferStatus.CANCELLED if cancelled else TransferStatus.FAILED
            return True

    def get_session(self, session_id: str) -> ActiveTransferSession | None:
        with self._lock:
            return self._sessions.get(session_id)

    def reset(self) -> None:
        with self._lock:
            self._sessions.clear()
