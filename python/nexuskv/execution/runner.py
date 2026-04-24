from __future__ import annotations

from dataclasses import dataclass

from nexuskv.connectors.base import LookupStatus
from nexuskv.contracts.generated import BufferKind, MaterializationCapability, TierKind, TransferBackend
from nexuskv.execution.types import (
    CapabilityCheckResult,
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
    def execute(self, request: MaterializationRequest) -> MaterializationOutcome:
        primary = self._decide_primary(request)
        prefetch = self._decide_prefetch(request)
        store = self._decide_store(request)
        return MaterializationOutcome(primary=primary, prefetch=prefetch, store=store)

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
