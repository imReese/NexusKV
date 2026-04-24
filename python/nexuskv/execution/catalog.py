from __future__ import annotations

from dataclasses import dataclass, field

from nexuskv.contracts.generated import BufferKind, DeviceClass, MaterializationCapability, TierKind, TransferBackend
from nexuskv.execution.backend import ExecutionBackend
from nexuskv.execution.types import BackendActionKind, BackendActionRequest, BackendSelection, FallbackReason


@dataclass(slots=True)
class BackendRegistration:
    name: str
    backend: ExecutionBackend
    transfer_backend: TransferBackend | None
    action_kinds: tuple[BackendActionKind, ...]
    source_tiers: tuple[TierKind, ...] = ()
    target_tiers: tuple[TierKind, ...] = ()
    device_classes: tuple[DeviceClass, ...] = ()
    buffer_kinds: tuple[BufferKind, ...] = ()
    materialization_capabilities: tuple[MaterializationCapability, ...] = ()
    priority: int = 100
    _order: int = field(default=0, init=False, repr=False)

    def matches_common(
        self,
        request: BackendActionRequest,
        required_capability: MaterializationCapability | None,
    ) -> bool:
        return (
            request.kind in self.action_kinds
            and self._matches_tier(request.decision.source.tier, self.source_tiers)
            and self._matches_tier(request.decision.target.tier, self.target_tiers)
            and self._matches_device(request.decision.target.device_class, self.device_classes)
            and self._matches_buffer(request.decision.target.buffer_kind, self.buffer_kinds)
            and self._matches_capability(required_capability)
        )

    def _matches_tier(self, value: TierKind | None, supported: tuple[TierKind, ...]) -> bool:
        return not supported or value is None or value in supported

    def _matches_device(self, value: DeviceClass | None, supported: tuple[DeviceClass, ...]) -> bool:
        return not supported or value is None or value in supported

    def _matches_buffer(self, value: BufferKind | None, supported: tuple[BufferKind, ...]) -> bool:
        return not supported or value is None or value in supported

    def _matches_capability(self, value: MaterializationCapability | None) -> bool:
        return not self.materialization_capabilities or value is None or value in self.materialization_capabilities


@dataclass(slots=True)
class BackendCatalog:
    registrations: list[BackendRegistration] = field(default_factory=list)
    _next_order: int = field(default=0, init=False, repr=False)

    def register(self, registration: BackendRegistration) -> None:
        registration._order = self._next_order
        self._next_order += 1
        self.registrations.append(registration)

    def select(self, request: BackendActionRequest) -> tuple[ExecutionBackend, BackendSelection] | None:
        desired_backend = request.decision.transfer.selected_backend
        required_capability = self._required_capability(request)
        common = [reg for reg in self.registrations if reg.matches_common(request, required_capability)]
        if not common:
            return None

        common.sort(key=lambda reg: (reg.priority, reg._order))
        if desired_backend is not None:
            exact = [reg for reg in common if reg.transfer_backend == desired_backend]
            if exact:
                chosen = exact[0]
                return chosen.backend, BackendSelection(
                    backend_name=chosen.name,
                    transfer_backend=chosen.transfer_backend,
                    degraded=False,
                    fallback_reason=None,
                    required_capability=required_capability,
                )

            degraded = [reg for reg in common if reg.transfer_backend is not None]
            if degraded:
                chosen = degraded[0]
                return chosen.backend, BackendSelection(
                    backend_name=chosen.name,
                    transfer_backend=chosen.transfer_backend,
                    degraded=True,
                    fallback_reason=FallbackReason.PREFERRED_BACKEND_UNAVAILABLE,
                    required_capability=required_capability,
                )
            return None

        chosen = common[0]
        return chosen.backend, BackendSelection(
            backend_name=chosen.name,
            transfer_backend=chosen.transfer_backend,
            degraded=False,
            fallback_reason=None,
            required_capability=required_capability,
        )

    def _required_capability(self, request: BackendActionRequest) -> MaterializationCapability | None:
        raw = request.decision.capability_check.required_capability
        if raw is None:
            return None
        try:
            return MaterializationCapability(raw)
        except ValueError:
            return None
