from __future__ import annotations

from dataclasses import dataclass, field
from typing import Protocol

from nexuskv.execution.types import (
    BackendActionKind,
    BackendActionRequest,
    BackendActionResult,
    BackendActionStatus,
    ExecutionDisposition,
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
class BaselineExecutionBackend:
    supported_backends: tuple | None = None
    calls: list[BackendInvocation] = field(default_factory=list)
    backend_name: str = "baseline-execution-backend"

    def materialize(self, request: BackendActionRequest) -> BackendActionResult:
        return self._transfer_action(request)

    def prefetch(self, request: BackendActionRequest) -> BackendActionResult:
        return self._transfer_action(request)

    def store(self, request: BackendActionRequest) -> BackendActionResult:
        return self._transfer_action(request)

    def skip(self, request: BackendActionRequest) -> BackendActionResult:
        result = BackendActionResult(
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
        )
        self.calls.append(BackendInvocation(request=request, result=result))
        return result

    def recompute(self, request: BackendActionRequest) -> BackendActionResult:
        result = BackendActionResult(
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
        )
        self.calls.append(BackendInvocation(request=request, result=result))
        return result

    def _transfer_action(self, request: BackendActionRequest) -> BackendActionResult:
        selected_backend = request.decision.transfer.selected_backend
        if not request.decision.capability_check.supported or selected_backend is None:
            result = BackendActionResult(
                requested_kind=request.kind,
                executed_kind=request.kind,
                status=BackendActionStatus.REJECTED,
                final_disposition=request.decision.disposition,
                backend_name=self.backend_name,
                selected_backend=selected_backend,
                source=request.decision.source,
                target=request.decision.target,
                degraded=request.decision.capability_check.degraded,
                fallback_reason=request.decision.fallback_reason,
                detail="decision did not provide a supported backend path",
            )
            self.calls.append(BackendInvocation(request=request, result=result))
            return result

        if self.supported_backends is not None and selected_backend not in self.supported_backends:
            result = BackendActionResult(
                requested_kind=request.kind,
                executed_kind=request.kind,
                status=BackendActionStatus.REJECTED,
                final_disposition=request.decision.disposition,
                backend_name=self.backend_name,
                selected_backend=selected_backend,
                source=request.decision.source,
                target=request.decision.target,
                degraded=request.decision.capability_check.degraded,
                fallback_reason=request.decision.fallback_reason,
                detail=f"backend {selected_backend} is unsupported by {self.backend_name}",
            )
            self.calls.append(BackendInvocation(request=request, result=result))
            return result

        result = BackendActionResult(
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
        )
        self.calls.append(BackendInvocation(request=request, result=result))
        return result
