from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

SCHEMA_VERSION = "nexuskv.contract.v1"

class EngineFamily(StrEnum):
    UNKNOWN = "unknown"
    SGLANG = "sglang"
    VLLM = "vllm"

class StateSemanticType(StrEnum):
    MHA_KV = "mha_kv"
    GQA_KV = "gqa_kv"
    MQA_KV = "mqa_kv"
    MLA_STATE = "mla_state"
    GENERIC_CONTAINER = "generic_container"

class TensorRole(StrEnum):
    KEY = "key"
    VALUE = "value"
    LATENT = "latent"
    POSITION = "position"
    AUXILIARY = "auxiliary"

class Granularity(StrEnum):
    TOKEN = "token"
    BLOCK = "block"
    PAGE = "page"
    SEGMENT = "segment"

class CompatibilityFlag(StrEnum):
    EXACT_REUSE = "exact_reuse"
    PREFIX_REUSE = "prefix_reuse"
    PAGE_REUSE = "page_reuse"
    BLOCK_REUSE = "block_reuse"
    WARM_START = "warm_start"
    POST_EVICTION_RELOAD = "post_eviction_reload"

class DeviceClass(StrEnum):
    CPU = "cpu"
    CUDA = "cuda"
    ROCM = "rocm"
    TPU = "tpu"
    OTHER = "other"

class BufferKind(StrEnum):
    DEVICE = "device"
    HOST_PINNED = "host_pinned"
    HOST_PAGEABLE = "host_pageable"
    REMOTE = "remote"
    FILE_BACKED = "file_backed"

class TransferBackend(StrEnum):
    BASELINE_TRANSPORT = "baseline_transport"
    STAGED_COPY = "staged_copy"
    RDMA = "rdma"
    ZERO_COPY = "zero_copy"

class TransferCapability(StrEnum):
    HOST_TO_DEVICE = "host_to_device"
    DEVICE_TO_HOST = "device_to_host"
    HOST_TO_STORE = "host_to_store"
    STORE_TO_HOST = "store_to_host"
    ASYNC = "async"
    ZERO_COPY_CANDIDATE = "zero_copy_candidate"

class TierKind(StrEnum):
    DEVICE = "device"
    HOST_DRAM = "host_dram"
    LOCAL_SSD = "local_ssd"
    REMOTE_SHARED = "remote_shared"
    OBJECT_STORE = "object_store"

class MaterializationCapability(StrEnum):
    FULL = "full"
    PARTIAL = "partial"
    PREFETCH = "prefetch"
    ASYNC_FETCH = "async_fetch"
    FALLBACK_RECOMPUTE = "fallback_recompute"

class MatchClassification(StrEnum):
    EXACT = "exact"
    PREFIX = "prefix"
    PARTIAL = "partial"

class PlanDisposition(StrEnum):
    FULL_REUSE = "full_reuse"
    PARTIAL_REUSE = "partial_reuse"
    RECOMPUTE = "recompute"

@dataclass(slots=True)
class TensorSpec:
    name: str
    role: TensorRole
    dtype: str
    shape: list[str]

@dataclass(slots=True)
class QuantizationMetadata:
    scheme: str
    bits: int
    group_size: int

@dataclass(slots=True)
class LayoutMetadata:
    layout: str
    page_tokens: int
    block_tokens: int
    packed: bool

@dataclass(slots=True)
class TransferPath:
    backend: TransferBackend
    capabilities: list[TransferCapability]

@dataclass(slots=True)
class MaterializationProfile:
    capabilities: list[MaterializationCapability]
    tier_kinds: list[TierKind]
    device_classes: list[DeviceClass]
    buffer_kinds: list[BufferKind]

@dataclass(slots=True)
class AttentionStateDescriptor:
    schema_version: str
    descriptor_id: str
    engine_family: EngineFamily
    semantic_type: StateSemanticType
    granularity: Granularity
    tensor_specs: list[TensorSpec]
    quantization: QuantizationMetadata
    layout: LayoutMetadata
    compatibility_flags: list[CompatibilityFlag]
    transfer_paths: list[TransferPath]
    materialization: MaterializationProfile
    layout_metadata: dict[str, str]

@dataclass(slots=True)
class KeyIdentity:
    tenant: str
    namespace: str
    model: str
    engine_family: EngineFamily
    semantic_type: StateSemanticType
    tokens: list[int]
    block_id: int | None
    page_id: int | None

@dataclass(slots=True)
class ReuseKey:
    identity: KeyIdentity

@dataclass(slots=True)
class QueryKey:
    identity: KeyIdentity

@dataclass(slots=True)
class EntryVersion:
    generation: int
    lineage: str

@dataclass(slots=True)
class EntryLocation:
    tier: TierKind
    locator: str

@dataclass(slots=True)
class PolicyHint:
    reusable: bool
    admission_hint: str
    eviction_hint: str

@dataclass(slots=True)
class EntryIdentity:
    key: KeyIdentity
    entry_id: str
    version: EntryVersion

@dataclass(slots=True)
class CacheEntry:
    identity: EntryIdentity
    descriptor: AttentionStateDescriptor
    location: EntryLocation
    policy_hint: PolicyHint

@dataclass(slots=True)
class MatchExtent:
    units: int
    granularity: Granularity

@dataclass(slots=True)
class RemainingWork:
    tokens: list[int]
    fetch_required: bool
    recompute_required: bool

@dataclass(slots=True)
class CompatibilitySignal:
    reusable: bool
    fallback_to_recompute: bool
    reason: str

@dataclass(slots=True)
class MatchResult:
    classification: MatchClassification
    matched_key: ReuseKey
    requested_key: QueryKey
    matched_extent: MatchExtent
    entry: CacheEntry
    remaining: RemainingWork
    compatibility: CompatibilitySignal

@dataclass(slots=True)
class ReusableSlice:
    tokens: list[int]
    source_tier: TierKind

@dataclass(slots=True)
class PartialHitPlan:
    disposition: PlanDisposition
    reusable: ReusableSlice
    remaining: RemainingWork
    entry: CacheEntry

__all__ = [
    "SCHEMA_VERSION",
    "EngineFamily",
    "StateSemanticType",
    "TensorRole",
    "Granularity",
    "CompatibilityFlag",
    "DeviceClass",
    "BufferKind",
    "TransferBackend",
    "TransferCapability",
    "TierKind",
    "MaterializationCapability",
    "MatchClassification",
    "PlanDisposition",
    "TensorSpec",
    "QuantizationMetadata",
    "LayoutMetadata",
    "TransferPath",
    "MaterializationProfile",
    "AttentionStateDescriptor",
    "KeyIdentity",
    "ReuseKey",
    "QueryKey",
    "EntryVersion",
    "EntryLocation",
    "PolicyHint",
    "EntryIdentity",
    "CacheEntry",
    "MatchExtent",
    "RemainingWork",
    "CompatibilitySignal",
    "MatchResult",
    "ReusableSlice",
    "PartialHitPlan",
]
