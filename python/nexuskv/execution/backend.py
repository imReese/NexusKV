from __future__ import annotations

from dataclasses import dataclass, field
from typing import Protocol

from nexuskv.contracts.generated import BufferKind, DeviceClass, TierKind, TransferBackend
from nexuskv.execution.store import InMemoryEntryStore
from nexuskv.execution.types import (
    BackendActionKind,
    BackendActionRequest,
    BackendActionResult,
    BackendActionStatus,
    ExecutionDisposition,
    FallbackReason,
    PayloadHandle,
    PayloadLocation,
    PayloadOwnership,
    TransferRequest,
    TransferResult,
    TransferSession,
    TransferStatus,
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
                payload_handle=request.payload_handle,
                transfer_session=self._transfer_session(
                    request,
                    status=TransferStatus.FALLBACK,
                    result_handle=request.payload_handle,
                    detail="skip does not move payload data",
                ),
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
                payload_handle=request.payload_handle,
                transfer_session=self._transfer_session(
                    request,
                    status=TransferStatus.FALLBACK,
                    result_handle=request.payload_handle,
                    detail="recompute does not reuse cached payload data",
                ),
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
                payload_handle=request.payload_handle,
                transfer_session=self._transfer_session(
                    request,
                    status=TransferStatus.COMPLETED,
                    result_handle=request.payload_handle,
                    detail="transfer-like action executed",
                ),
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
                payload_handle=request.payload_handle,
                transfer_session=self._transfer_session(
                    request,
                    status=TransferStatus.REJECTED,
                    result_handle=request.payload_handle,
                    detail=detail,
                ),
                detail=detail,
            ),
        )

    def _record(self, request: BackendActionRequest, result: BackendActionResult) -> BackendActionResult:
        self.calls.append(BackendInvocation(request=request, result=result))
        return result

    def _transfer_session(
        self,
        request: BackendActionRequest,
        *,
        status: TransferStatus,
        result_handle: PayloadHandle | None,
        detail: str,
        intermediate_handle: PayloadHandle | None = None,
    ) -> TransferSession:
        transfer_request = request.transfer_request or TransferRequest(
            action_kind=request.kind,
            source_handle=request.payload_handle,
            desired_target=PayloadLocation(
                tier=request.decision.target.tier,
                buffer_kind=request.decision.target.buffer_kind,
                device_class=request.decision.target.device_class,
                locator=request.decision.target.tier.value if request.decision.target.tier is not None else None,
                handle_kind="implicit_target",
            ),
            transfer_backend=request.decision.transfer.selected_backend,
            degraded_from=request.decision.transfer.degraded_from,
            fallback_reason=request.decision.fallback_reason,
        )
        return TransferSession(
            session_id=f"{self.backend_name}:{request.hook}:{request.kind}:{len(self.calls) + 1}",
            request=transfer_request,
            result=TransferResult(
                status=status,
                result_handle=result_handle,
                intermediate_handle=intermediate_handle,
                detail=detail,
            ),
        )


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
                    payload_handle=stored.payload_handle,
                    transfer_session=self._transfer_session(
                        request,
                        status=TransferStatus.COMPLETED,
                        result_handle=stored.payload_handle,
                        detail=f"materialized stored entry {stored.entry.identity.entry_id}",
                    ),
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
            result_handle = self._result_handle_for_materialize(request)
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
                    payload_handle=result_handle,
                    transfer_session=self._transfer_session(
                        request,
                        status=TransferStatus.COMPLETED,
                        result_handle=result_handle,
                        detail="materialized using external source metadata",
                    ),
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

        self.state.record_prefetch(request.lookup.query, locator, request.payload_handle)
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
                payload_handle=request.payload_handle,
                transfer_session=self._transfer_session(
                    request,
                    status=TransferStatus.REGISTERED,
                    result_handle=request.payload_handle,
                    detail="prefetch intent recorded",
                ),
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
        if request.payload_handle is None:
            return self._reject(
                request,
                "store request did not include a payload handle",
                fallback_reason=FallbackReason.ENGINE_POLICY,
            )

        record = self.state.put(request.store_entry, request.payload_handle)
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
                payload_handle=record.payload_handle,
                transfer_session=self._transfer_session(
                    request,
                    status=TransferStatus.COMPLETED,
                    result_handle=record.payload_handle,
                    detail=f"stored entry {record.entry.identity.entry_id}",
                ),
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

    def _result_handle_for_materialize(self, request: BackendActionRequest) -> PayloadHandle:
        source_handle = (
            request.transfer_request.source_handle
            if request.transfer_request is not None and request.transfer_request.source_handle is not None
            else request.payload_handle
        )
        assert source_handle is not None
        target = request.transfer_request.desired_target if request.transfer_request is not None else PayloadLocation(
            tier=request.decision.target.tier,
            buffer_kind=request.decision.target.buffer_kind,
            device_class=request.decision.target.device_class,
            locator=request.decision.target.tier.value if request.decision.target.tier is not None else None,
            handle_kind="materialized_target",
        )
        return PayloadHandle(
            handle_id=f"materialized:{request.hook}:{request.kind}:{request.lookup.query.identity.tenant}:{request.lookup.query.identity.namespace}:{request.lookup.query.identity.model}",
            location=target,
            descriptor=source_handle.descriptor,
            ownership=PayloadOwnership.OWNED,
            opaque_ref=target.locator,
        )


@dataclass(slots=True)
class StagedCopyExecutionBackend(RecordingExecutionBackend):
    def __init__(self, *, backend_name: str = "staged-copy-backend") -> None:
        RecordingExecutionBackend.__init__(
            self,
            backend_name=backend_name,
            supported_backends=(TransferBackend.STAGED_COPY,),
        )

    def materialize(self, request: BackendActionRequest) -> BackendActionResult:
        result = self._transfer_action(request)
        if result.status != BackendActionStatus.SUCCEEDED:
            return result
        source = request.transfer_request.source_handle if request.transfer_request is not None else request.payload_handle
        result_handle = self._materialized_target_handle(request, ownership=PayloadOwnership.OWNED)
        intermediate = self._intermediate_host_handle(request, source)
        result.payload_handle = result_handle
        result.transfer_session = self._transfer_session(
            request,
            status=TransferStatus.COMPLETED,
            result_handle=result_handle,
            intermediate_handle=intermediate,
            detail="staged copy completed through an intermediate host buffer",
        )
        result.detail = "staged copy materialized payload"
        return result

    def prefetch(self, request: BackendActionRequest) -> BackendActionResult:
        result = self._transfer_action(request)
        if result.status != BackendActionStatus.SUCCEEDED:
            return result
        source = request.transfer_request.source_handle if request.transfer_request is not None else request.payload_handle
        result_handle = self._materialized_target_handle(request, ownership=PayloadOwnership.EPHEMERAL)
        intermediate = self._intermediate_host_handle(request, source)
        result.payload_handle = result_handle
        result.transfer_session = self._transfer_session(
            request,
            status=TransferStatus.REGISTERED,
            result_handle=result_handle,
            intermediate_handle=intermediate,
            detail="staged copy prefetch registered through an intermediate host buffer",
        )
        result.detail = "staged copy prefetch registered"
        return result

    def _materialized_target_handle(
        self,
        request: BackendActionRequest,
        *,
        ownership: PayloadOwnership,
    ) -> PayloadHandle:
        source = request.transfer_request.source_handle if request.transfer_request is not None else request.payload_handle
        assert source is not None
        target = request.transfer_request.desired_target if request.transfer_request is not None else PayloadLocation(
            tier=request.decision.target.tier,
            buffer_kind=request.decision.target.buffer_kind,
            device_class=request.decision.target.device_class,
            locator=request.decision.target.tier.value if request.decision.target.tier is not None else None,
            handle_kind="staged_target",
        )
        return PayloadHandle(
            handle_id=f"staged:{request.hook}:{request.kind}:{len(self.calls) + 1}",
            location=target,
            descriptor=source.descriptor,
            ownership=ownership,
            opaque_ref="staged-copy",
        )

    def _intermediate_host_handle(
        self,
        request: BackendActionRequest,
        source: PayloadHandle | None,
    ) -> PayloadHandle | None:
        if source is None:
            return None
        return PayloadHandle(
            handle_id=f"host-stage:{request.hook}:{request.kind}:{len(self.calls) + 1}",
            location=PayloadLocation(
                tier=TierKind.HOST_DRAM,
                buffer_kind=BufferKind.HOST_PINNED,
                device_class=DeviceClass.CPU,
                locator="host://staging-buffer",
                handle_kind="host_staging",
            ),
            descriptor=source.descriptor,
            ownership=PayloadOwnership.EPHEMERAL,
            opaque_ref="host-staging",
        )


@dataclass(slots=True)
class RemoteSharedStoreExecutionBackend(RecordingExecutionBackend):
    def __init__(self, *, backend_name: str = "remote-shared-store-backend") -> None:
        RecordingExecutionBackend.__init__(
            self,
            backend_name=backend_name,
            supported_backends=(TransferBackend.STAGED_COPY,),
        )

    def store(self, request: BackendActionRequest) -> BackendActionResult:
        result = self._transfer_action(request)
        if result.status != BackendActionStatus.SUCCEEDED:
            return result
        if request.store_entry is None:
            return self._reject(
                request,
                "remote store request did not include a cache entry",
                fallback_reason=FallbackReason.ENGINE_POLICY,
            )
        source = request.payload_handle or (request.transfer_request.source_handle if request.transfer_request is not None else None)
        assert source is not None
        result_handle = PayloadHandle(
            handle_id=f"remote:{request.store_entry.identity.entry_id}",
            location=PayloadLocation(
                tier=request.store_entry.location.tier,
                buffer_kind=BufferKind.REMOTE,
                locator=request.store_entry.location.locator,
                handle_kind="remote_store",
            ),
            descriptor=source.descriptor,
            ownership=PayloadOwnership.CACHED,
            opaque_ref=request.store_entry.location.locator,
        )
        result.payload_handle = result_handle
        result.transfer_session = self._transfer_session(
            request,
            status=TransferStatus.COMPLETED,
            result_handle=result_handle,
            detail="remote shared-store stub recorded a remote payload handle",
        )
        result.detail = "remote shared-store stub recorded a remote payload handle"
        return result
