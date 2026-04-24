from __future__ import annotations

from dataclasses import dataclass, replace

from nexuskv.connectors.base import LookupStatus
from nexuskv.contracts.generated import (
    BufferKind,
    CacheEntry,
    DeviceClass,
    EntryIdentity,
    EntryLocation,
    EntryVersion,
    Granularity,
    MaterializationCapability,
    PolicyHint,
    TierKind,
    TransferBackend,
)
from nexuskv.execution.backend import (
    BaselineExecutionBackend,
    ExecutionBackend,
    RemoteSharedStoreExecutionBackend,
    StagedCopyExecutionBackend,
)
from nexuskv.execution.catalog import BackendCatalog, BackendRegistration
from nexuskv.execution.store import InMemoryEntryStore
from nexuskv.execution.types import (
    BackendActionKind,
    BackendActionRequest,
    BackendActionResult,
    BackendActionStatus,
    BackendSelection,
    CapabilityCheckResult,
    ExecutionStepOutcome,
    ExecutionDisposition,
    FallbackReason,
    MaterializationDecision,
    MaterializationOutcome,
    MaterializationRequest,
    PayloadDescriptor,
    PayloadHandle,
    PayloadLocation,
    PayloadOwnership,
    SourceTier,
    StateSliceDescriptor,
    TargetTier,
    TransferRequest,
    TransferStatus,
    TransferMode,
)


@dataclass(slots=True)
class BaselineExecutionRunner:
    backend: ExecutionBackend | None = None
    catalog: BackendCatalog | None = None

    def execute(self, request: MaterializationRequest) -> MaterializationOutcome:
        if self.catalog is None:
            self.catalog = self._build_default_catalog()

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
        initial_request = BackendActionRequest(
            kind=self._action_kind_for(decision.disposition),
            hook=request.hook,
            context=request.context,
            lookup=request.lookup,
            decision=decision,
            store_entry=self._build_store_entry(request, decision),
            payload_handle=self._build_input_payload_handle(request, decision),
            transfer_request=self._build_transfer_request(request, decision),
        )
        action_request, selection = self._prepare_action_request(initial_request)
        result = self._dispatch(action_request, selection)
        if result.status == BackendActionStatus.REJECTED:
            result = self._fallback_after_rejection(action_request, result)
        return ExecutionStepOutcome(decision=action_request.decision, result=result)

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
        backend, degraded, fallback_reason = self._select_backend(
            descriptor,
            preferred_backend=request.preferred_backend,
        )
        if request.allow_store_after_stage:
            return MaterializationDecision(
                ExecutionDisposition.STORE,
                source=SourceTier(tier=None),
                target=target,
                transfer=TransferMode(
                    selected_backend=backend,
                    degraded_from=request.preferred_backend if degraded else None,
                ),
                capability_check=CapabilityCheckResult(
                    supported=backend is not None,
                    degraded=degraded,
                    required_capability=None,
                    fallback_reason=fallback_reason,
                    selected_backend=backend,
                ),
                fallback_reason=fallback_reason,
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

    def _dispatch(
        self,
        request: BackendActionRequest,
        selection: tuple[ExecutionBackend, BackendSelection] | None,
    ) -> BackendActionResult:
        if selection is None:
            return BackendActionResult(
                requested_kind=request.kind,
                executed_kind=request.kind,
                status=BackendActionStatus.REJECTED,
                final_disposition=request.decision.disposition,
                backend_name="no-backend-selected",
                selected_backend=request.decision.transfer.selected_backend,
                source=request.decision.source,
                target=request.decision.target,
                degraded=request.decision.capability_check.degraded,
                fallback_reason=request.decision.fallback_reason or FallbackReason.NO_TRANSFER_PATH,
                payload_handle=request.payload_handle,
                transfer_session=None,
                detail="backend catalog could not select a compatible execution backend",
            )

        backend, _ = selection
        if request.kind == BackendActionKind.MATERIALIZE:
            return backend.materialize(request)
        if request.kind == BackendActionKind.PREFETCH:
            return backend.prefetch(request)
        if request.kind == BackendActionKind.STORE:
            return backend.store(request)
        if request.kind == BackendActionKind.RECOMPUTE:
            return backend.recompute(request)
        return backend.skip(request)

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
            store_entry=request.store_entry,
        )
        fallback_prepared, fallback_selection = self._prepare_action_request(fallback_request)
        fallback_result = self._dispatch(fallback_prepared, fallback_selection)
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
            payload_handle=fallback_result.payload_handle,
            transfer_session=fallback_result.transfer_session,
            detail=rejected.detail,
        )

    def _prepare_action_request(
        self,
        request: BackendActionRequest,
    ) -> tuple[BackendActionRequest, tuple[ExecutionBackend, BackendSelection] | None]:
        assert self.catalog is not None
        selection = self.catalog.select(request)
        if selection is None:
            return request, None

        _, selected = selection
        effective_decision = request.decision
        if selected.transfer_backend != request.decision.transfer.selected_backend or selected.degraded:
            effective_decision = replace(
                request.decision,
                transfer=TransferMode(
                    selected_backend=selected.transfer_backend,
                    degraded_from=(
                        request.decision.transfer.selected_backend
                        if selected.degraded and selected.transfer_backend != request.decision.transfer.selected_backend
                        else request.decision.transfer.degraded_from
                    ),
                ),
                capability_check=replace(
                    request.decision.capability_check,
                    degraded=request.decision.capability_check.degraded or selected.degraded,
                    fallback_reason=selected.fallback_reason or request.decision.capability_check.fallback_reason,
                    selected_backend=selected.transfer_backend,
                ),
                fallback_reason=selected.fallback_reason or request.decision.fallback_reason,
            )
        return replace(request, decision=effective_decision), selection

    def _build_default_catalog(self) -> BackendCatalog:
        if self.backend is not None:
            catalog = BackendCatalog()
            backend_name = getattr(self.backend, "backend_name", "custom-execution-backend")
            for transfer_backend in TransferBackend:
                catalog.register(
                    BackendRegistration(
                        name=backend_name,
                        backend=self.backend,
                        transfer_backend=transfer_backend,
                        action_kinds=(
                            BackendActionKind.MATERIALIZE,
                            BackendActionKind.PREFETCH,
                            BackendActionKind.STORE,
                        ),
                        priority=100,
                    )
                )
            catalog.register(
                BackendRegistration(
                    name=backend_name,
                    backend=self.backend,
                    transfer_backend=None,
                    action_kinds=(BackendActionKind.SKIP, BackendActionKind.RECOMPUTE),
                    priority=90,
                )
            )
            return catalog

        store = InMemoryEntryStore()
        baseline = BaselineExecutionBackend(store=store)
        staged = StagedCopyExecutionBackend()
        remote = RemoteSharedStoreExecutionBackend()

        catalog = BackendCatalog()
        catalog.register(
            BackendRegistration(
                name=baseline.backend_name,
                backend=baseline,
                transfer_backend=TransferBackend.BASELINE_TRANSPORT,
                action_kinds=(BackendActionKind.MATERIALIZE, BackendActionKind.PREFETCH, BackendActionKind.STORE),
                source_tiers=(TierKind.HOST_DRAM, TierKind.REMOTE_SHARED),
                target_tiers=(TierKind.HOST_DRAM, TierKind.REMOTE_SHARED),
                materialization_capabilities=(
                    MaterializationCapability.FULL,
                    MaterializationCapability.PREFETCH,
                ),
                priority=20,
            )
        )
        catalog.register(
            BackendRegistration(
                name=remote.backend_name,
                backend=remote,
                transfer_backend=TransferBackend.STAGED_COPY,
                action_kinds=(BackendActionKind.STORE,),
                target_tiers=(TierKind.REMOTE_SHARED,),
                priority=10,
            )
        )
        catalog.register(
            BackendRegistration(
                name=staged.backend_name,
                backend=staged,
                transfer_backend=TransferBackend.STAGED_COPY,
                action_kinds=(BackendActionKind.MATERIALIZE, BackendActionKind.PREFETCH),
                source_tiers=(TierKind.HOST_DRAM, TierKind.REMOTE_SHARED),
                target_tiers=(TierKind.DEVICE, TierKind.HOST_DRAM),
                materialization_capabilities=(
                    MaterializationCapability.FULL,
                    MaterializationCapability.PARTIAL,
                    MaterializationCapability.PREFETCH,
                ),
                priority=15,
            )
        )
        catalog.register(
            BackendRegistration(
                name=baseline.backend_name,
                backend=baseline,
                transfer_backend=None,
                action_kinds=(BackendActionKind.SKIP, BackendActionKind.RECOMPUTE),
                priority=5,
            )
        )
        return catalog

    def _build_store_entry(
        self,
        request: MaterializationRequest,
        decision: MaterializationDecision,
    ) -> CacheEntry | None:
        if decision.disposition != ExecutionDisposition.STORE:
            return None

        identity = request.lookup.query.identity
        entry_id = (
            f"runtime:{identity.tenant}:{identity.namespace}:{identity.model}:"
            f"{','.join(str(token) for token in identity.tokens)}"
        )
        locator = (
            f"remote://{entry_id}"
            if (decision.target.tier or TierKind.HOST_DRAM) == TierKind.REMOTE_SHARED
            else f"memory://{entry_id}"
        )
        return CacheEntry(
            identity=EntryIdentity(
                key=identity,
                entry_id=entry_id,
                version=EntryVersion(generation=1, lineage=f"runtime:{request.hook}"),
            ),
            descriptor=request.context.descriptor,
            location=EntryLocation(
                tier=decision.target.tier or TierKind.HOST_DRAM,
                locator=locator,
            ),
            policy_hint=PolicyHint(
                reusable=True,
                admission_hint="execution_store",
                eviction_hint="default",
            ),
        )

    def _build_input_payload_handle(
        self,
        request: MaterializationRequest,
        decision: MaterializationDecision,
    ) -> PayloadHandle | None:
        descriptor = self._payload_descriptor(request)
        if decision.disposition == ExecutionDisposition.STORE:
            source = self._select_target_tier(request.context.descriptor)
            return PayloadHandle(
                handle_id=f"runtime:{request.hook}:{request.context.tenant}:{request.context.namespace}:{request.context.model}",
                location=self._payload_location_from_target(source, locator="runtime://engine-state", handle_kind="runtime_state"),
                descriptor=descriptor,
                ownership=PayloadOwnership.BORROWED,
                opaque_ref="runtime-engine-state",
            )

        if request.lookup.match is not None:
            return PayloadHandle(
                handle_id=f"source:{request.lookup.match.entry.identity.entry_id}",
                location=self._payload_location_from_source(request.lookup.match.entry.location.tier, request.lookup.match.entry.location.locator),
                descriptor=descriptor,
                ownership=PayloadOwnership.CACHED,
                opaque_ref=request.lookup.match.entry.location.locator,
            )

        if request.lookup.partial_plan is not None:
            return PayloadHandle(
                handle_id=f"source:{request.lookup.partial_plan.entry.identity.entry_id}",
                location=self._payload_location_from_source(
                    request.lookup.partial_plan.entry.location.tier,
                    request.lookup.partial_plan.entry.location.locator,
                ),
                descriptor=descriptor,
                ownership=PayloadOwnership.CACHED,
                opaque_ref=request.lookup.partial_plan.entry.location.locator,
            )
        return None

    def _build_transfer_request(
        self,
        request: MaterializationRequest,
        decision: MaterializationDecision,
    ) -> TransferRequest | None:
        if decision.disposition in {ExecutionDisposition.SKIP, ExecutionDisposition.RECOMPUTE}:
            return None
        return TransferRequest(
            action_kind=self._action_kind_for(decision.disposition),
            source_handle=self._build_input_payload_handle(request, decision),
            desired_target=self._payload_location_from_target(decision.target),
            transfer_backend=decision.transfer.selected_backend,
            degraded_from=decision.transfer.degraded_from,
            required_capability=(
                None
                if decision.capability_check.required_capability is None
                else MaterializationCapability(decision.capability_check.required_capability)
            ),
            fallback_reason=decision.fallback_reason,
        )

    def _payload_descriptor(self, request: MaterializationRequest) -> PayloadDescriptor:
        return PayloadDescriptor(
            descriptor_id=request.context.descriptor.descriptor_id,
            engine_family=request.context.descriptor.engine_family,
            semantic_type=request.context.descriptor.semantic_type,
            state_slice=StateSliceDescriptor(
                granularity=request.context.descriptor.granularity,
                token_count=len(request.context.tokens),
                token_start=0,
                block_id=request.context.block_id,
                page_id=request.context.page_id,
            ),
            byte_size_hint=self._byte_size_hint(request.context.descriptor.granularity, len(request.context.tokens)),
        )

    def _payload_location_from_source(self, tier: TierKind | None, locator: str | None) -> PayloadLocation:
        return PayloadLocation(
            tier=tier,
            buffer_kind=self._buffer_kind_for_tier(tier),
            device_class=DeviceClass.CPU if tier == TierKind.HOST_DRAM else DeviceClass.CUDA if tier == TierKind.DEVICE else None,
            locator=locator,
            handle_kind="source_payload",
        )

    def _payload_location_from_target(
        self,
        target: TargetTier,
        locator: str | None = None,
        *,
        handle_kind: str = "target_payload",
    ) -> PayloadLocation:
        return PayloadLocation(
            tier=target.tier,
            buffer_kind=target.buffer_kind,
            device_class=target.device_class,
            locator=locator or (target.tier.value if target.tier is not None else None),
            handle_kind=handle_kind,
        )

    def _buffer_kind_for_tier(self, tier: TierKind | None) -> BufferKind | None:
        if tier == TierKind.DEVICE:
            return BufferKind.DEVICE
        if tier == TierKind.HOST_DRAM:
            return BufferKind.HOST_PINNED
        if tier == TierKind.LOCAL_SSD:
            return BufferKind.FILE_BACKED
        if tier == TierKind.REMOTE_SHARED:
            return BufferKind.REMOTE
        return None

    def _byte_size_hint(self, granularity: Granularity, token_count: int) -> int:
        unit = 4096 if granularity == Granularity.PAGE else 1024 if granularity == Granularity.BLOCK else 256
        return token_count * unit
