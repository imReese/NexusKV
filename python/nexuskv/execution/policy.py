from __future__ import annotations

import json
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path

from nexuskv.contracts.generated import (
    AttentionStateDescriptor,
    BufferKind,
    DeviceClass,
    MaterializationCapability,
    TierKind,
    TransferBackend,
)
from nexuskv.execution.types import ExecutionDisposition, FallbackReason


SCHEMA_VERSION = "nexuskv.execution_policy.v1"


class FallbackBehavior(StrEnum):
    DEGRADE = "degrade"
    RECOMPUTE = "recompute"
    SKIP = "skip"


class PlaceholderMode(StrEnum):
    DISABLED = "disabled"
    ADVISORY = "advisory"


@dataclass(slots=True)
class RecomputeFallbackPolicy:
    on_cache_miss: bool
    on_backend_rejection: bool
    on_capability_miss: bool
    on_policy_denial: bool


@dataclass(slots=True)
class TenantNamespacePolicy:
    mode: PlaceholderMode
    default_tenant: str
    default_namespace: str


@dataclass(slots=True)
class QuotaAdmissionPolicy:
    mode: PlaceholderMode
    max_payload_bytes: int
    max_entries: int


@dataclass(slots=True)
class ExecutionPolicy:
    schema_version: str
    enabled_transfer_backends: tuple[TransferBackend, ...]
    backend_priority_order: tuple[TransferBackend, ...]
    allowed_source_tiers: tuple[TierKind, ...]
    allowed_target_tiers: tuple[TierKind, ...]
    allowed_device_classes: tuple[DeviceClass, ...]
    allowed_buffer_kinds: tuple[BufferKind, ...]
    default_fallback_behavior: FallbackBehavior
    recompute_fallback_policy: RecomputeFallbackPolicy
    tenant_namespace_policy: TenantNamespacePolicy
    quota_admission_policy: QuotaAdmissionPolicy

    @classmethod
    def default(cls) -> "ExecutionPolicy":
        return cls(
            schema_version=SCHEMA_VERSION,
            enabled_transfer_backends=(
                TransferBackend.BASELINE_TRANSPORT,
                TransferBackend.STAGED_COPY,
                TransferBackend.RDMA,
                TransferBackend.ZERO_COPY,
            ),
            backend_priority_order=(
                TransferBackend.STAGED_COPY,
                TransferBackend.BASELINE_TRANSPORT,
                TransferBackend.RDMA,
                TransferBackend.ZERO_COPY,
            ),
            allowed_source_tiers=(
                TierKind.DEVICE,
                TierKind.HOST_DRAM,
                TierKind.LOCAL_SSD,
                TierKind.REMOTE_SHARED,
                TierKind.OBJECT_STORE,
            ),
            allowed_target_tiers=(
                TierKind.DEVICE,
                TierKind.HOST_DRAM,
                TierKind.LOCAL_SSD,
                TierKind.REMOTE_SHARED,
            ),
            allowed_device_classes=(
                DeviceClass.CPU,
                DeviceClass.CUDA,
                DeviceClass.ROCM,
                DeviceClass.TPU,
                DeviceClass.OTHER,
            ),
            allowed_buffer_kinds=(
                BufferKind.DEVICE,
                BufferKind.HOST_PINNED,
                BufferKind.HOST_PAGEABLE,
                BufferKind.REMOTE,
                BufferKind.FILE_BACKED,
            ),
            default_fallback_behavior=FallbackBehavior.DEGRADE,
            recompute_fallback_policy=RecomputeFallbackPolicy(
                on_cache_miss=True,
                on_backend_rejection=True,
                on_capability_miss=True,
                on_policy_denial=False,
            ),
            tenant_namespace_policy=TenantNamespacePolicy(
                mode=PlaceholderMode.ADVISORY,
                default_tenant="default",
                default_namespace="default",
            ),
            quota_admission_policy=QuotaAdmissionPolicy(
                mode=PlaceholderMode.ADVISORY,
                max_payload_bytes=0,
                max_entries=0,
            ),
        )

    @classmethod
    def from_file(cls, path: str | Path) -> "ExecutionPolicy":
        return cls.from_json(Path(path).read_text(encoding="utf-8"))

    @classmethod
    def from_json(cls, raw: str) -> "ExecutionPolicy":
        return cls.from_dict(json.loads(raw))

    @classmethod
    def from_dict(cls, data: dict) -> "ExecutionPolicy":
        policy = cls(
            schema_version=data["schema_version"],
            enabled_transfer_backends=tuple(_unique_enum_list(data["enabled_transfer_backends"], TransferBackend, "enabled_transfer_backends")),
            backend_priority_order=tuple(_unique_enum_list(data["backend_priority_order"], TransferBackend, "backend_priority_order")),
            allowed_source_tiers=tuple(_unique_enum_list(data["allowed_source_tiers"], TierKind, "allowed_source_tiers")),
            allowed_target_tiers=tuple(_unique_enum_list(data["allowed_target_tiers"], TierKind, "allowed_target_tiers")),
            allowed_device_classes=tuple(_unique_enum_list(data["allowed_device_classes"], DeviceClass, "allowed_device_classes")),
            allowed_buffer_kinds=tuple(_unique_enum_list(data["allowed_buffer_kinds"], BufferKind, "allowed_buffer_kinds")),
            default_fallback_behavior=FallbackBehavior(data["default_fallback_behavior"]),
            recompute_fallback_policy=RecomputeFallbackPolicy(**data["recompute_fallback_policy"]),
            tenant_namespace_policy=TenantNamespacePolicy(
                mode=PlaceholderMode(data["tenant_namespace_policy"]["mode"]),
                default_tenant=data["tenant_namespace_policy"]["default_tenant"],
                default_namespace=data["tenant_namespace_policy"]["default_namespace"],
            ),
            quota_admission_policy=QuotaAdmissionPolicy(
                mode=PlaceholderMode(data["quota_admission_policy"]["mode"]),
                max_payload_bytes=int(data["quota_admission_policy"]["max_payload_bytes"]),
                max_entries=int(data["quota_admission_policy"]["max_entries"]),
            ),
        )
        policy.validate()
        return policy

    def to_dict(self) -> dict:
        return {
            "schema_version": self.schema_version,
            "enabled_transfer_backends": [backend.value for backend in self.enabled_transfer_backends],
            "backend_priority_order": [backend.value for backend in self.backend_priority_order],
            "allowed_source_tiers": [tier.value for tier in self.allowed_source_tiers],
            "allowed_target_tiers": [tier.value for tier in self.allowed_target_tiers],
            "allowed_device_classes": [device.value for device in self.allowed_device_classes],
            "allowed_buffer_kinds": [buffer_kind.value for buffer_kind in self.allowed_buffer_kinds],
            "default_fallback_behavior": self.default_fallback_behavior.value,
            "recompute_fallback_policy": {
                "on_cache_miss": self.recompute_fallback_policy.on_cache_miss,
                "on_backend_rejection": self.recompute_fallback_policy.on_backend_rejection,
                "on_capability_miss": self.recompute_fallback_policy.on_capability_miss,
                "on_policy_denial": self.recompute_fallback_policy.on_policy_denial,
            },
            "tenant_namespace_policy": {
                "mode": self.tenant_namespace_policy.mode.value,
                "default_tenant": self.tenant_namespace_policy.default_tenant,
                "default_namespace": self.tenant_namespace_policy.default_namespace,
            },
            "quota_admission_policy": {
                "mode": self.quota_admission_policy.mode.value,
                "max_payload_bytes": self.quota_admission_policy.max_payload_bytes,
                "max_entries": self.quota_admission_policy.max_entries,
            },
        }

    def validate(self) -> None:
        if self.schema_version != SCHEMA_VERSION:
            raise ValueError(f"schema_version must be {SCHEMA_VERSION!r}")
        if not self.enabled_transfer_backends:
            raise ValueError("enabled_transfer_backends must not be empty")
        if not self.backend_priority_order:
            raise ValueError("backend_priority_order must not be empty")
        if set(self.backend_priority_order) != set(self.enabled_transfer_backends):
            raise ValueError("backend_priority_order must include every enabled transfer backend exactly once")
        if self.quota_admission_policy.max_payload_bytes < 0:
            raise ValueError("quota_admission_policy.max_payload_bytes must be >= 0")
        if self.quota_admission_policy.max_entries < 0:
            raise ValueError("quota_admission_policy.max_entries must be >= 0")

    def allows_transfer_backend(self, backend: TransferBackend | None) -> bool:
        return backend is None or backend in self.enabled_transfer_backends

    def allows_source_tier(self, tier: TierKind | None) -> bool:
        return not self.allowed_source_tiers or tier is None or tier in self.allowed_source_tiers

    def allows_target_tier(self, tier: TierKind | None) -> bool:
        return not self.allowed_target_tiers or tier is None or tier in self.allowed_target_tiers

    def allows_device_class(self, device_class: DeviceClass | None) -> bool:
        return not self.allowed_device_classes or device_class is None or device_class in self.allowed_device_classes

    def allows_buffer_kind(self, buffer_kind: BufferKind | None) -> bool:
        return not self.allowed_buffer_kinds or buffer_kind is None or buffer_kind in self.allowed_buffer_kinds

    def priority_for(self, backend: TransferBackend, default: int) -> int:
        if backend not in self.backend_priority_order:
            return default
        return self.backend_priority_order.index(backend)

    def allows_degraded_backend_selection(self) -> bool:
        return self.default_fallback_behavior == FallbackBehavior.DEGRADE

    def fallback_disposition(
        self,
        descriptor: AttentionStateDescriptor,
        reason: FallbackReason,
        *,
        action_is_primary: bool,
    ) -> ExecutionDisposition:
        if not action_is_primary:
            return ExecutionDisposition.SKIP
        if not self._recompute_allowed(reason):
            return ExecutionDisposition.SKIP
        if MaterializationCapability.FALLBACK_RECOMPUTE not in descriptor.materialization.capabilities:
            return ExecutionDisposition.SKIP
        return ExecutionDisposition.RECOMPUTE

    def _recompute_allowed(self, reason: FallbackReason) -> bool:
        if reason == FallbackReason.CACHE_MISS:
            return self.recompute_fallback_policy.on_cache_miss
        if reason == FallbackReason.UNSUPPORTED_CAPABILITY:
            return self.recompute_fallback_policy.on_capability_miss
        if reason == FallbackReason.ENGINE_POLICY:
            return self.recompute_fallback_policy.on_policy_denial
        return self.recompute_fallback_policy.on_backend_rejection


def _unique_enum_list(raw_values: list[str], enum_type, field_name: str) -> list:
    seen: set = set()
    values = []
    for raw in raw_values:
        value = enum_type(raw)
        if value in seen:
            raise ValueError(f"{field_name} contains duplicate value {raw!r}")
        seen.add(value)
        values.append(value)
    return values
