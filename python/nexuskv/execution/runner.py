from __future__ import annotations

from dataclasses import dataclass

from nexuskv.connectors.base import LookupStatus
from nexuskv.contracts.generated import BufferKind, MaterializationCapability, TierKind, TransferBackend
from nexuskv.execution.backend import BaselineExecutionBackend, ExecutionBackend
from nexuskv.execution.types import (
    BackendActionKind,
    BackendActionRequest,
    BackendActionResult,
    BackendActionStatus,
    CapabilityCheckResult,
    ExecutionStepOutcome,
    ExecutionDisposition,
    FallbackReason,
    MaterializationDecision,
    MaterializationOutcome,
    MaterializationRequest,
    SourceTier,
    TargetTier,
    TransferMode,
)


@dataclass(slots=True)
class BaselineExecutionRunner:
    backend: ExecutionBackend | None = None

    def execute(self, request: MaterializationRequest) -> MaterializationOutcome:
        if self.backend is None:
            self.backend = BaselineExecutionBackend()

        primary = self._execute_step(request, self._decide_primary(request))
        prefetch_decision = self._decide_prefetch(request)
        prefetch = None if prefetch_decision is None else self._execute_step(request, prefetch_decision)
        store = self._execute_step(request, self._decide_store(request))
        return MaterializationOutcome(primary=primary, prefetch=prefetch, store=store)

    def _execute_step(
        self,
        request: MaterializationRequest,
        decision: MaterializationDecision,
    ) -> ExecutionStepOutcome:
        action_request = BackendActionRequest(
            kind=self._action_kind_for(decision.disposition),
            hook=request.hook,
            context=request.context,
            lookup=request.lookup,
            decision=decision,
        )
        result = self._dispatch(action_request)
        if result.status == BackendActionStatus.REJECTED:
            result = self._fallback_after_rejection(action_request, result)
        return ExecutionStepOutcome(decision=decision, result=result)

    def _decide_primary(self, request: MaterializationRequest) -> MaterializationDecision:
        descriptor = request.context.descriptor
        target = self._select_target_tier(descriptor)

        if request.lookup.status == LookupStatus.UNSUPPORTED:
            return self._decision(
                ExecutionDisposition.SKIP,
                target=target,
                fallback_reason=FallbackReason.LOOKUP_UNSUPPORTED,
            )

        if request.lookup.status == LookupStatus.MISS:
            return self._decision(
                ExecutionDisposition.RECOMPUTE,
                target=target,
                fallback_reason=FallbackReason.CACHE_MISS,
            )

        if request.lookup.status == LookupStatus.HIT and request.lookup.match is not None:
            return self._materialize_decision(
                request,
                required_capability=MaterializationCapability.FULL,
                source=SourceTier(
                    tier=request.lookup.match.entry.location.tier,
                    locator=request.lookup.match.entry.location.locator,
                ),
                target=target,
                disposition=ExecutionDisposition.MATERIALIZE,
            )

        if request.lookup.status == LookupStatus.PARTIAL and request.lookup.partial_plan is not None:
            return self._materialize_decision(
                request,
                required_capability=MaterializationCapability.PARTIAL,
                source=SourceTier(
                    tier=request.lookup.partial_plan.entry.location.tier,
                    locator=request.lookup.partial_plan.entry.location.locator,
                ),
                target=target,
                disposition=ExecutionDisposition.MATERIALIZE,
            )

        return self._decision(
            ExecutionDisposition.RECOMPUTE,
            target=target,
            fallback_reason=FallbackReason.CACHE_MISS,
        )

    def _decide_prefetch(self, request: MaterializationRequest) -> MaterializationDecision | None:
        if not request.enable_prefetch:
            return None

        descriptor = request.context.descriptor
        target = self._select_target_tier(descriptor)
        if request.lookup.partial_plan is None:
            return self._decision(ExecutionDisposition.SKIP, target=target, fallback_reason=None)

        if MaterializationCapability.PREFETCH not in descriptor.materialization.capabilities:
            return MaterializationDecision(
                disposition=ExecutionDisposition.SKIP,
                source=SourceTier(
                    tier=request.lookup.partial_plan.entry.location.tier,
                    locator=request.lookup.partial_plan.entry.location.locator,
                ),
                target=target,
                transfer=TransferMode(selected_backend=None),
                capability_check=CapabilityCheckResult(
                    supported=False,
                    degraded=False,
                    required_capability=MaterializationCapability.PREFETCH.value,
                    fallback_reason=FallbackReason.UNSUPPORTED_CAPABILITY,
                    selected_backend=None,
                ),
                fallback_reason=FallbackReason.UNSUPPORTED_CAPABILITY,
            )

        return self._materialize_decision(
            request,
            required_capability=MaterializationCapability.PREFETCH,
            source=SourceTier(
                tier=request.lookup.partial_plan.entry.location.tier,
                locator=request.lookup.partial_plan.entry.location.locator,
            ),
            target=target,
            disposition=ExecutionDisposition.PREFETCH,
        )

    def _decide_store(self, request: MaterializationRequest) -> MaterializationDecision:
        descriptor = request.context.descriptor
        target = self._select_store_tier(descriptor)
        if request.allow_store_after_stage:
            return self._decision(
                ExecutionDisposition.STORE,
                target=target,
                fallback_reason=None,
            )
        return self._decision(
            ExecutionDisposition.SKIP,
            target=target,
            fallback_reason=None,
        )

    def _materialize_decision(
        self,
        request: MaterializationRequest,
        *,
        required_capability: MaterializationCapability,
        source: SourceTier,
        target: TargetTier,
        disposition: ExecutionDisposition,
    ) -> MaterializationDecision:
        descriptor = request.context.descriptor
        if required_capability not in descriptor.materialization.capabilities:
            fallback_reason = FallbackReason.UNSUPPORTED_CAPABILITY
            fallback_disposition = (
                ExecutionDisposition.RECOMPUTE
                if MaterializationCapability.FALLBACK_RECOMPUTE in descriptor.materialization.capabilities
                else ExecutionDisposition.SKIP
            )
            return MaterializationDecision(
                disposition=fallback_disposition,
                source=source,
                target=target,
                transfer=TransferMode(selected_backend=None),
                capability_check=CapabilityCheckResult(
                    supported=False,
                    degraded=False,
                    required_capability=required_capability.value,
                    fallback_reason=fallback_reason,
                    selected_backend=None,
                ),
                fallback_reason=fallback_reason,
            )

        backend, degraded, fallback_reason = self._select_backend(
            descriptor,
            preferred_backend=request.preferred_backend,
        )
        return MaterializationDecision(
            disposition=disposition,
            source=source,
            target=target,
            transfer=TransferMode(
                selected_backend=backend,
                degraded_from=request.preferred_backend if degraded else None,
            ),
            capability_check=CapabilityCheckResult(
                supported=backend is not None,
                degraded=degraded,
                required_capability=required_capability.value,
                fallback_reason=fallback_reason,
                selected_backend=backend,
            ),
            fallback_reason=fallback_reason,
        )

    def _select_backend(
        self,
        descriptor,
        *,
        preferred_backend: TransferBackend | None,
    ) -> tuple[TransferBackend | None, bool, FallbackReason | None]:
        available = tuple(path.backend for path in descriptor.transfer_paths)
        if not available:
            return None, False, FallbackReason.NO_TRANSFER_PATH
        if preferred_backend is None:
            return available[0], False, None
        if preferred_backend in available:
            return preferred_backend, False, None
        return available[0], True, FallbackReason.PREFERRED_BACKEND_UNAVAILABLE

    def _select_target_tier(self, descriptor) -> TargetTier:
        tiers = descriptor.materialization.tier_kinds
        buffer_kinds = descriptor.materialization.buffer_kinds
        device_classes = descriptor.materialization.device_classes

        if TierKind.DEVICE in tiers:
            return TargetTier(
                tier=TierKind.DEVICE,
                device_class=device_classes[0] if device_classes else None,
                buffer_kind=BufferKind.DEVICE if BufferKind.DEVICE in buffer_kinds else None,
            )
        if TierKind.HOST_DRAM in tiers:
            buffer_kind = (
                BufferKind.HOST_PINNED
                if BufferKind.HOST_PINNED in buffer_kinds
                else BufferKind.HOST_PAGEABLE
                if BufferKind.HOST_PAGEABLE in buffer_kinds
                else None
            )
            return TargetTier(tier=TierKind.HOST_DRAM, buffer_kind=buffer_kind)
        if TierKind.LOCAL_SSD in tiers:
            return TargetTier(tier=TierKind.LOCAL_SSD, buffer_kind=BufferKind.FILE_BACKED)
        if TierKind.REMOTE_SHARED in tiers:
            return TargetTier(tier=TierKind.REMOTE_SHARED, buffer_kind=BufferKind.REMOTE)
        return TargetTier(tier=None)

    def _select_store_tier(self, descriptor) -> TargetTier:
        tiers = descriptor.materialization.tier_kinds
        if TierKind.REMOTE_SHARED in tiers:
            return TargetTier(tier=TierKind.REMOTE_SHARED, buffer_kind=BufferKind.REMOTE)
        return self._select_target_tier(descriptor)

    def _decision(
        self,
        disposition: ExecutionDisposition,
        *,
        target: TargetTier,
        fallback_reason: FallbackReason | None,
    ) -> MaterializationDecision:
        return MaterializationDecision(
            disposition=disposition,
            source=SourceTier(tier=None),
            target=target,
            transfer=TransferMode(selected_backend=None),
            capability_check=CapabilityCheckResult(
                supported=disposition not in {ExecutionDisposition.SKIP, ExecutionDisposition.RECOMPUTE},
                degraded=False,
                required_capability=None,
                fallback_reason=fallback_reason,
                selected_backend=None,
            ),
            fallback_reason=fallback_reason,
        )

    def _action_kind_for(self, disposition: ExecutionDisposition) -> BackendActionKind:
        return BackendActionKind(disposition.value)

    def _dispatch(self, request: BackendActionRequest) -> BackendActionResult:
        assert self.backend is not None
        if request.kind == BackendActionKind.MATERIALIZE:
            return self.backend.materialize(request)
        if request.kind == BackendActionKind.PREFETCH:
            return self.backend.prefetch(request)
        if request.kind == BackendActionKind.STORE:
            return self.backend.store(request)
        if request.kind == BackendActionKind.RECOMPUTE:
            return self.backend.recompute(request)
        return self.backend.skip(request)

    def _fallback_after_rejection(
        self,
        request: BackendActionRequest,
        rejected: BackendActionResult,
    ) -> BackendActionResult:
        if request.kind == BackendActionKind.MATERIALIZE:
            fallback_kind = (
                BackendActionKind.RECOMPUTE
                if MaterializationCapability.FALLBACK_RECOMPUTE in request.context.descriptor.materialization.capabilities
                else BackendActionKind.SKIP
            )
        else:
            fallback_kind = BackendActionKind.SKIP

        fallback_decision = MaterializationDecision(
            disposition=ExecutionDisposition(fallback_kind.value),
            source=request.decision.source,
            target=request.decision.target,
            transfer=TransferMode(selected_backend=None),
            capability_check=request.decision.capability_check,
            fallback_reason=request.decision.fallback_reason or rejected.fallback_reason or FallbackReason.NO_TRANSFER_PATH,
        )
        fallback_request = BackendActionRequest(
            kind=fallback_kind,
            hook=request.hook,
            context=request.context,
            lookup=request.lookup,
            decision=fallback_decision,
        )
        fallback_result = self._dispatch(fallback_request)
        return BackendActionResult(
            requested_kind=request.kind,
            executed_kind=fallback_result.executed_kind,
            status=BackendActionStatus.FALLBACK,
            final_disposition=fallback_result.final_disposition,
            backend_name=fallback_result.backend_name,
            selected_backend=rejected.selected_backend,
            source=fallback_result.source,
            target=fallback_result.target,
            degraded=request.decision.capability_check.degraded,
            fallback_reason=request.decision.fallback_reason or rejected.fallback_reason or FallbackReason.NO_TRANSFER_PATH,
            detail=rejected.detail,
        )
