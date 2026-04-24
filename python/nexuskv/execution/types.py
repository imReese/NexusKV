from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

from nexuskv.connectors.base import EngineRequestContext, LookupOutcome
from nexuskv.contracts.generated import BufferKind, DeviceClass, TierKind, TransferBackend


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
    primary: MaterializationDecision
    prefetch: MaterializationDecision | None
    store: MaterializationDecision
