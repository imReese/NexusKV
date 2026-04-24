from __future__ import annotations

from dataclasses import dataclass, field
from typing import Protocol

from nexuskv.contracts.generated import TransferBackend
from nexuskv.execution.store import InMemoryEntryStore
from nexuskv.execution.types import (
    BackendActionKind,
    BackendActionRequest,
    BackendActionResult,
    BackendActionStatus,
    ExecutionDisposition,
    FallbackReason,
)


@dataclass(slots=True)
class BackendInvocation:
    request: BackendActionRequest
    result: BackendActionResult


class ExecutionBackend(Protocol):
    def materialize(self, request: BackendActionRequest) -> BackendActionResult:
        raise NotImplementedError

    def prefetch(self, request: BackendActionRequest) -> BackendActionResult:
        raise NotImplementedError

    def store(self, request: BackendActionRequest) -> BackendActionResult:
        raise NotImplementedError

    def skip(self, request: BackendActionRequest) -> BackendActionResult:
        raise NotImplementedError

    def recompute(self, request: BackendActionRequest) -> BackendActionResult:
        raise NotImplementedError


@dataclass(slots=True)
class RecordingExecutionBackend:
    backend_name: str
    supported_backends: tuple[TransferBackend, ...] | None = None
    calls: list[BackendInvocation] = field(default_factory=list)

    def materialize(self, request: BackendActionRequest) -> BackendActionResult:
        return self._transfer_action(request)

    def prefetch(self, request: BackendActionRequest) -> BackendActionResult:
        return self._transfer_action(request)

    def store(self, request: BackendActionRequest) -> BackendActionResult:
        return self._transfer_action(request)

    def skip(self, request: BackendActionRequest) -> BackendActionResult:
        return self._record(
            request,
            BackendActionResult(
                requested_kind=request.kind,
                executed_kind=BackendActionKind.SKIP,
                status=BackendActionStatus.SKIPPED,
                final_disposition=ExecutionDisposition.SKIP,
                backend_name=self.backend_name,
                selected_backend=None,
                source=request.decision.source,
                target=request.decision.target,
                degraded=request.decision.capability_check.degraded,
                fallback_reason=request.decision.fallback_reason,
                detail="execution skipped by decision",
            ),
        )

    def recompute(self, request: BackendActionRequest) -> BackendActionResult:
        return self._record(
            request,
            BackendActionResult(
                requested_kind=request.kind,
                executed_kind=BackendActionKind.RECOMPUTE,
                status=BackendActionStatus.RECOMPUTED,
                final_disposition=ExecutionDisposition.RECOMPUTE,
                backend_name=self.backend_name,
                selected_backend=None,
                source=request.decision.source,
                target=request.decision.target,
                degraded=request.decision.capability_check.degraded,
                fallback_reason=request.decision.fallback_reason,
                detail="execution fell back to recompute",
            ),
        )

    def _transfer_action(self, request: BackendActionRequest) -> BackendActionResult:
        selected_backend = request.decision.transfer.selected_backend
        if not request.decision.capability_check.supported or selected_backend is None:
            return self._reject(request, "decision did not provide a supported backend path")

        if self.supported_backends is not None and selected_backend not in self.supported_backends:
            return self._reject(request, f"backend {selected_backend} is unsupported by {self.backend_name}")

        return self._record(
            request,
            BackendActionResult(
                requested_kind=request.kind,
                executed_kind=request.kind,
                status=BackendActionStatus.SUCCEEDED,
                final_disposition=request.decision.disposition,
                backend_name=self.backend_name,
                selected_backend=selected_backend,
                source=request.decision.source,
                target=request.decision.target,
                degraded=request.decision.capability_check.degraded,
                fallback_reason=request.decision.fallback_reason,
                detail="backend action executed",
            ),
        )

    def _reject(
        self,
        request: BackendActionRequest,
        detail: str,
        *,
        fallback_reason: FallbackReason | None = None,
    ) -> BackendActionResult:
        return self._record(
            request,
            BackendActionResult(
                requested_kind=request.kind,
                executed_kind=request.kind,
                status=BackendActionStatus.REJECTED,
                final_disposition=request.decision.disposition,
                backend_name=self.backend_name,
                selected_backend=request.decision.transfer.selected_backend,
                source=request.decision.source,
                target=request.decision.target,
                degraded=request.decision.capability_check.degraded,
                fallback_reason=fallback_reason or request.decision.fallback_reason,
                detail=detail,
            ),
        )

    def _record(self, request: BackendActionRequest, result: BackendActionResult) -> BackendActionResult:
        self.calls.append(BackendInvocation(request=request, result=result))
        return result


@dataclass(slots=True)
class BaselineExecutionBackend(RecordingExecutionBackend):
    state: InMemoryEntryStore = field(default_factory=InMemoryEntryStore)

    def __init__(
        self,
        *,
        supported_backends: tuple[TransferBackend, ...] | None = (TransferBackend.BASELINE_TRANSPORT,),
        backend_name: str = "baseline-execution-backend",
        store: InMemoryEntryStore | None = None,
    ) -> None:
        RecordingExecutionBackend.__init__(self, backend_name=backend_name, supported_backends=supported_backends)
        self.state = InMemoryEntryStore() if store is None else store

    def materialize(self, request: BackendActionRequest) -> BackendActionResult:
        validation = self._validate_transfer(request)
        if validation is not None:
            return validation

        stored = self.state.get(request.lookup.query)
        if stored is not None:
            return self._record(
                request,
                BackendActionResult(
                    requested_kind=request.kind,
                    executed_kind=request.kind,
                    status=BackendActionStatus.SUCCEEDED,
                    final_disposition=request.decision.disposition,
                    backend_name=self.backend_name,
                    selected_backend=request.decision.transfer.selected_backend,
                    source=request.decision.source,
                    target=request.decision.target,
                    degraded=request.decision.capability_check.degraded,
                    fallback_reason=request.decision.fallback_reason,
                    detail=f"materialized stored entry {stored.entry.identity.entry_id}",
                ),
            )

        locator = request.decision.source.locator or ""
        if locator.startswith("memory://"):
            return self._reject(
                request,
                "entry was not found in the in-memory store",
                fallback_reason=FallbackReason.CACHE_MISS,
            )

        if request.lookup.match is not None or request.lookup.partial_plan is not None:
            return self._record(
                request,
                BackendActionResult(
                    requested_kind=request.kind,
                    executed_kind=request.kind,
                    status=BackendActionStatus.SUCCEEDED,
                    final_disposition=request.decision.disposition,
                    backend_name=self.backend_name,
                    selected_backend=request.decision.transfer.selected_backend,
                    source=request.decision.source,
                    target=request.decision.target,
                    degraded=request.decision.capability_check.degraded,
                    fallback_reason=request.decision.fallback_reason,
                    detail="materialized using external source metadata",
                ),
            )

        return self._reject(
            request,
            "materialize request had no resolvable source entry",
            fallback_reason=FallbackReason.CACHE_MISS,
        )

    def prefetch(self, request: BackendActionRequest) -> BackendActionResult:
        validation = self._validate_transfer(request)
        if validation is not None:
            return validation

        locator = request.decision.source.locator
        if locator is not None and locator.startswith("memory://") and self.state.get(request.lookup.query) is None:
            return self._reject(
                request,
                "prefetch source is missing from the in-memory store",
                fallback_reason=FallbackReason.CACHE_MISS,
            )

        self.state.record_prefetch(request.lookup.query, locator)
        return self._record(
            request,
            BackendActionResult(
                requested_kind=request.kind,
                executed_kind=request.kind,
                status=BackendActionStatus.SUCCEEDED,
                final_disposition=request.decision.disposition,
                backend_name=self.backend_name,
                selected_backend=request.decision.transfer.selected_backend,
                source=request.decision.source,
                target=request.decision.target,
                degraded=request.decision.capability_check.degraded,
                fallback_reason=request.decision.fallback_reason,
                detail="prefetch intent recorded",
            ),
        )

    def store(self, request: BackendActionRequest) -> BackendActionResult:
        validation = self._validate_transfer(request)
        if validation is not None:
            return validation

        if request.store_entry is None:
            return self._reject(
                request,
                "store request did not include a cache entry",
                fallback_reason=FallbackReason.ENGINE_POLICY,
            )

        record = self.state.put(request.store_entry)
        return self._record(
            request,
            BackendActionResult(
                requested_kind=request.kind,
                executed_kind=request.kind,
                status=BackendActionStatus.SUCCEEDED,
                final_disposition=request.decision.disposition,
                backend_name=self.backend_name,
                selected_backend=request.decision.transfer.selected_backend,
                source=request.decision.source,
                target=request.decision.target,
                degraded=request.decision.capability_check.degraded,
                fallback_reason=request.decision.fallback_reason,
                detail=f"stored entry {record.entry.identity.entry_id}",
            ),
        )

    def _validate_transfer(self, request: BackendActionRequest) -> BackendActionResult | None:
        selected_backend = request.decision.transfer.selected_backend
        if not request.decision.capability_check.supported or selected_backend is None:
            return self._reject(request, "decision did not provide a supported backend path")
        if self.supported_backends is not None and selected_backend not in self.supported_backends:
            return self._reject(request, f"backend {selected_backend} is unsupported by {self.backend_name}")
        return None


@dataclass(slots=True)
class StagedCopyExecutionBackend(RecordingExecutionBackend):
    def __init__(self, *, backend_name: str = "staged-copy-backend") -> None:
        RecordingExecutionBackend.__init__(
            self,
            backend_name=backend_name,
            supported_backends=(TransferBackend.STAGED_COPY,),
        )


@dataclass(slots=True)
class RemoteSharedStoreExecutionBackend(RecordingExecutionBackend):
    def __init__(self, *, backend_name: str = "remote-shared-store-backend") -> None:
        RecordingExecutionBackend.__init__(
            self,
            backend_name=backend_name,
            supported_backends=(TransferBackend.STAGED_COPY,),
        )
