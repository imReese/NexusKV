from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

from nexuskv.connectors.base import EngineRequestContext, LookupOutcome
from nexuskv.contracts.generated import (
    BufferKind,
    CacheEntry,
    DeviceClass,
    EngineFamily,
    Granularity,
    MaterializationCapability,
    StateSemanticType,
    TierKind,
    TransferBackend,
)


class ExecutionDisposition(StrEnum):
    MATERIALIZE = "materialize"
    PREFETCH = "prefetch"
    STORE = "store"
    RECOMPUTE = "recompute"
    SKIP = "skip"


class FallbackReason(StrEnum):
    UNSUPPORTED_CAPABILITY = "unsupported_capability"
    PREFERRED_BACKEND_UNAVAILABLE = "preferred_backend_unavailable"
    NO_TRANSFER_PATH = "no_transfer_path"
    LOOKUP_UNSUPPORTED = "lookup_unsupported"
    CACHE_MISS = "cache_miss"
    ENGINE_POLICY = "engine_policy"


class BackendActionKind(StrEnum):
    MATERIALIZE = "materialize"
    PREFETCH = "prefetch"
    STORE = "store"
    SKIP = "skip"
    RECOMPUTE = "recompute"


class BackendActionStatus(StrEnum):
    SUCCEEDED = "succeeded"
    SKIPPED = "skipped"
    RECOMPUTED = "recomputed"
    FALLBACK = "fallback"
    REJECTED = "rejected"


class PayloadOwnership(StrEnum):
    OWNED = "owned"
    BORROWED = "borrowed"
    CACHED = "cached"
    EPHEMERAL = "ephemeral"


class TransferStatus(StrEnum):
    COMPLETED = "completed"
    REGISTERED = "registered"
    MISSING = "missing"
    FALLBACK = "fallback"
    REJECTED = "rejected"


@dataclass(slots=True)
class SourceTier:
    tier: TierKind | None
    locator: str | None = None


@dataclass(slots=True)
class TargetTier:
    tier: TierKind | None
    device_class: DeviceClass | None = None
    buffer_kind: BufferKind | None = None


@dataclass(slots=True)
class TransferMode:
    selected_backend: TransferBackend | None
    degraded_from: TransferBackend | None = None


@dataclass(slots=True)
class CapabilityCheckResult:
    supported: bool
    degraded: bool
    required_capability: str | None
    fallback_reason: FallbackReason | None
    selected_backend: TransferBackend | None


@dataclass(slots=True)
class StateSliceDescriptor:
    granularity: Granularity
    token_count: int
    token_start: int = 0
    block_id: int | None = None
    page_id: int | None = None


@dataclass(slots=True)
class PayloadDescriptor:
    descriptor_id: str
    engine_family: EngineFamily
    semantic_type: StateSemanticType
    state_slice: StateSliceDescriptor
    byte_size_hint: int | None = None


@dataclass(slots=True)
class PayloadLocation:
    tier: TierKind | None
    buffer_kind: BufferKind | None = None
    device_class: DeviceClass | None = None
    locator: str | None = None
    handle_kind: str = "opaque"


@dataclass(slots=True)
class PayloadHandle:
    handle_id: str
    location: PayloadLocation
    descriptor: PayloadDescriptor
    ownership: PayloadOwnership
    opaque_ref: str | None = None


@dataclass(slots=True)
class TransferRequest:
    action_kind: BackendActionKind
    source_handle: PayloadHandle | None
    desired_target: PayloadLocation
    transfer_backend: TransferBackend | None
    degraded_from: TransferBackend | None = None
    required_capability: MaterializationCapability | None = None
    fallback_reason: FallbackReason | None = None


@dataclass(slots=True)
class TransferResult:
    status: TransferStatus
    result_handle: PayloadHandle | None
    intermediate_handle: PayloadHandle | None = None
    detail: str | None = None


@dataclass(slots=True)
class TransferSession:
    session_id: str
    request: TransferRequest
    result: TransferResult


@dataclass(slots=True)
class MaterializationRequest:
    hook: str
    context: EngineRequestContext
    lookup: LookupOutcome
    preferred_backend: TransferBackend | None
    allow_store_after_stage: bool
    enable_prefetch: bool


@dataclass(slots=True)
class MaterializationDecision:
    disposition: ExecutionDisposition
    source: SourceTier
    target: TargetTier
    transfer: TransferMode
    capability_check: CapabilityCheckResult
    fallback_reason: FallbackReason | None


@dataclass(slots=True)
class MaterializationOutcome:
    primary: "ExecutionStepOutcome"
    prefetch: "ExecutionStepOutcome | None"
    store: "ExecutionStepOutcome"


@dataclass(slots=True)
class BackendActionRequest:
    kind: BackendActionKind
    hook: str
    context: EngineRequestContext
    lookup: LookupOutcome
    decision: MaterializationDecision
    store_entry: CacheEntry | None = None
    payload_handle: PayloadHandle | None = None
    transfer_request: TransferRequest | None = None


@dataclass(slots=True)
class BackendActionResult:
    requested_kind: BackendActionKind
    executed_kind: BackendActionKind
    status: BackendActionStatus
    final_disposition: ExecutionDisposition
    backend_name: str
    selected_backend: TransferBackend | None
    source: SourceTier
    target: TargetTier
    degraded: bool
    fallback_reason: FallbackReason | None
    payload_handle: PayloadHandle | None = None
    transfer_session: TransferSession | None = None
    detail: str | None = None


@dataclass(slots=True)
class ExecutionStepOutcome:
    decision: MaterializationDecision
    result: BackendActionResult


@dataclass(slots=True)
class BackendSelection:
    backend_name: str
    transfer_backend: TransferBackend | None
    degraded: bool
    fallback_reason: FallbackReason | None
    required_capability: MaterializationCapability | None
