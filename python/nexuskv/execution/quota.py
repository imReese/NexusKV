from __future__ import annotations

import threading
from dataclasses import dataclass, field

from nexuskv.execution.policy import ExecutionPolicy


@dataclass(slots=True)
class QuotaTracker:
    _lock: threading.Lock = field(default_factory=threading.Lock, init=False)
    _active_entries: int = field(default=0, init=False)
    _active_payload_bytes: int = field(default=0, init=False)
    _active_transfers: int = field(default=0, init=False)
    _active_pinned_bytes: int = field(default=0, init=False)

    @property
    def active_entries(self) -> int:
        with self._lock:
            return self._active_entries

    @property
    def active_payload_bytes(self) -> int:
        with self._lock:
            return self._active_payload_bytes

    @property
    def active_transfers(self) -> int:
        with self._lock:
            return self._active_transfers

    @property
    def active_pinned_bytes(self) -> int:
        with self._lock:
            return self._active_pinned_bytes

    def add_entry(self, payload_bytes: int = 0) -> None:
        with self._lock:
            self._active_entries += 1
            self._active_payload_bytes += payload_bytes

    def remove_entry(self, payload_bytes: int = 0) -> None:
        with self._lock:
            self._active_entries = max(0, self._active_entries - 1)
            self._active_payload_bytes = max(0, self._active_payload_bytes - payload_bytes)

    def start_transfer(self, pinned_bytes: int = 0) -> None:
        with self._lock:
            self._active_transfers += 1
            self._active_pinned_bytes += pinned_bytes

    def finish_transfer(self, pinned_bytes: int = 0) -> None:
        with self._lock:
            self._active_transfers = max(0, self._active_transfers - 1)
            self._active_pinned_bytes = max(0, self._active_pinned_bytes - pinned_bytes)

    def check_admission(
        self,
        policy: ExecutionPolicy,
        requested_payload_bytes: int = 0,
        requested_pinned_bytes: int = 0,
    ) -> tuple[bool, str | None]:
        with self._lock:
            return policy.quota_admission_policy.check_admission(
                payload_bytes=self._active_payload_bytes + requested_payload_bytes,
                current_entries=self._active_entries,
                current_transfers=self._active_transfers,
                current_pinned_bytes=self._active_pinned_bytes + requested_pinned_bytes,
            )

    def reset(self) -> None:
        with self._lock:
            self._active_entries = 0
            self._active_payload_bytes = 0
            self._active_transfers = 0
            self._active_pinned_bytes = 0
