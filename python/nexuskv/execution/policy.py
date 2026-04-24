from __future__ import annotations

import json
from dataclasses import dataclass
from enum import StrEnum
import os
from pathlib import Path
from typing import Protocol

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
POLICY_PATH_ENV = "NEXUSKV_EXECUTION_POLICY_PATH"


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
class BackendCapabilityOverlay:
    enabled: bool | None = None
    priority_override: int | None = None
    allowed_source_tiers: tuple[TierKind, ...] = ()
    allowed_target_tiers: tuple[TierKind, ...] = ()
    allowed_device_classes: tuple[DeviceClass, ...] = ()
    allowed_buffer_kinds: tuple[BufferKind, ...] = ()
    allowed_materialization_capabilities: tuple[MaterializationCapability, ...] = ()
    allow_degraded_selection: bool | None = None


@dataclass(slots=True)
class PolicyReloadStatus:
    source_path: str | None
    generation: int
    reload_count: int
    last_reload_succeeded: bool
    last_error: str | None
    using_default_policy: bool
    changed: bool


class ExecutionPolicyProvider(Protocol):
    def active_policy(self) -> "ExecutionPolicy":
        raise NotImplementedError

    def status(self) -> PolicyReloadStatus:
        raise NotImplementedError

    def reload(self) -> PolicyReloadStatus:
        raise NotImplementedError


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
    backend_overlays: dict[TransferBackend, BackendCapabilityOverlay]

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
            backend_overlays={},
        )

    @classmethod
    def from_file(cls, path: str | Path) -> "ExecutionPolicy":
        return cls.from_json(Path(path).read_text(encoding="utf-8"))

    @classmethod
    def from_env(cls, env_var: str = POLICY_PATH_ENV) -> "ExecutionPolicy":
        raw = os.getenv(env_var)
        if not raw:
            raise FileNotFoundError(f"{env_var} is not set")
        return cls.from_file(raw)

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
            backend_overlays={
                TransferBackend(name): BackendCapabilityOverlay(
                    enabled=overlay.get("enabled"),
                    priority_override=(
                        None if overlay.get("priority_override") is None else int(overlay["priority_override"])
                    ),
                    allowed_source_tiers=tuple(
                        _unique_enum_list(overlay.get("allowed_source_tiers", []), TierKind, f"backend_overlays.{name}.allowed_source_tiers")
                    ),
                    allowed_target_tiers=tuple(
                        _unique_enum_list(overlay.get("allowed_target_tiers", []), TierKind, f"backend_overlays.{name}.allowed_target_tiers")
                    ),
                    allowed_device_classes=tuple(
                        _unique_enum_list(overlay.get("allowed_device_classes", []), DeviceClass, f"backend_overlays.{name}.allowed_device_classes")
                    ),
                    allowed_buffer_kinds=tuple(
                        _unique_enum_list(overlay.get("allowed_buffer_kinds", []), BufferKind, f"backend_overlays.{name}.allowed_buffer_kinds")
                    ),
                    allowed_materialization_capabilities=tuple(
                        _unique_enum_list(
                            overlay.get("allowed_materialization_capabilities", []),
                            MaterializationCapability,
                            f"backend_overlays.{name}.allowed_materialization_capabilities",
                        )
                    ),
                    allow_degraded_selection=overlay.get("allow_degraded_selection"),
                )
                for name, overlay in data.get("backend_overlays", {}).items()
            },
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
            "backend_overlays": {
                backend.value: {
                    key: value
                    for key, value in {
                        "enabled": overlay.enabled,
                        "priority_override": overlay.priority_override,
                        "allowed_source_tiers": [tier.value for tier in overlay.allowed_source_tiers],
                        "allowed_target_tiers": [tier.value for tier in overlay.allowed_target_tiers],
                        "allowed_device_classes": [device.value for device in overlay.allowed_device_classes],
                        "allowed_buffer_kinds": [buffer_kind.value for buffer_kind in overlay.allowed_buffer_kinds],
                        "allowed_materialization_capabilities": [
                            capability.value for capability in overlay.allowed_materialization_capabilities
                        ],
                        "allow_degraded_selection": overlay.allow_degraded_selection,
                    }.items()
                    if value not in (None, [], ())
                }
                for backend, overlay in self.backend_overlays.items()
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
        for backend, overlay in self.backend_overlays.items():
            if overlay.priority_override is not None and overlay.priority_override < 0:
                raise ValueError(f"backend_overlays.{backend.value}.priority_override must be >= 0")

    def allows_transfer_backend(self, backend: TransferBackend | None) -> bool:
        if backend is None:
            return True
        if backend not in self.enabled_transfer_backends:
            return False
        overlay = self.overlay_for(backend)
        return overlay.enabled is not False

    def allows_source_tier(self, tier: TierKind | None, backend: TransferBackend | None = None) -> bool:
        return self._allows_enum_value(tier, self.allowed_source_tiers, self.overlay_for(backend).allowed_source_tiers)

    def allows_target_tier(self, tier: TierKind | None, backend: TransferBackend | None = None) -> bool:
        return self._allows_enum_value(tier, self.allowed_target_tiers, self.overlay_for(backend).allowed_target_tiers)

    def allows_device_class(self, device_class: DeviceClass | None, backend: TransferBackend | None = None) -> bool:
        return self._allows_enum_value(
            device_class,
            self.allowed_device_classes,
            self.overlay_for(backend).allowed_device_classes,
        )

    def allows_buffer_kind(self, buffer_kind: BufferKind | None, backend: TransferBackend | None = None) -> bool:
        return self._allows_enum_value(
            buffer_kind,
            self.allowed_buffer_kinds,
            self.overlay_for(backend).allowed_buffer_kinds,
        )

    def allows_materialization_capability(
        self,
        capability: MaterializationCapability | None,
        backend: TransferBackend | None = None,
    ) -> bool:
        if capability is None:
            return True
        overlay_caps = self.overlay_for(backend).allowed_materialization_capabilities
        return not overlay_caps or capability in overlay_caps

    def priority_for(self, backend: TransferBackend, default: int) -> int:
        overlay = self.overlay_for(backend)
        if overlay.priority_override is not None:
            return overlay.priority_override
        if backend not in self.backend_priority_order:
            return default
        return self.backend_priority_order.index(backend)

    def allows_degraded_backend_selection(self, backend: TransferBackend | None = None) -> bool:
        if self.default_fallback_behavior != FallbackBehavior.DEGRADE:
            return False
        overlay = self.overlay_for(backend)
        return overlay.allow_degraded_selection is not False

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

    def overlay_for(self, backend: TransferBackend | None) -> BackendCapabilityOverlay:
        if backend is None:
            return BackendCapabilityOverlay()
        return self.backend_overlays.get(backend, BackendCapabilityOverlay())

    def _allows_enum_value(self, value, global_values: tuple, overlay_values: tuple) -> bool:
        return (
            (not global_values or value is None or value in global_values)
            and (not overlay_values or value is None or value in overlay_values)
        )


class FileExecutionPolicyProvider:
    def __init__(
        self,
        *,
        policy_path: str | Path | None = None,
        env_var: str = POLICY_PATH_ENV,
        default_policy: ExecutionPolicy | None = None,
    ) -> None:
        resolved_path = self._resolve_policy_path(policy_path, env_var)
        self._source_path = None if resolved_path is None else str(resolved_path)
        self._reload_count = 0
        self._generation = 1
        self._using_default_policy = resolved_path is None
        if resolved_path is None:
            self._active_policy = default_policy or ExecutionPolicy.default()
            self._status = PolicyReloadStatus(
                source_path=None,
                generation=self._generation,
                reload_count=self._reload_count,
                last_reload_succeeded=True,
                last_error=None,
                using_default_policy=True,
                changed=False,
            )
            return

        self._active_policy = ExecutionPolicy.from_file(resolved_path)
        self._status = PolicyReloadStatus(
            source_path=str(resolved_path),
            generation=self._generation,
            reload_count=self._reload_count,
            last_reload_succeeded=True,
            last_error=None,
            using_default_policy=False,
            changed=False,
        )

    def active_policy(self) -> ExecutionPolicy:
        return self._active_policy

    def status(self) -> PolicyReloadStatus:
        return self._status

    def reload(self) -> PolicyReloadStatus:
        if self._source_path is None:
            self._reload_count += 1
            self._status = PolicyReloadStatus(
                source_path=None,
                generation=self._generation,
                reload_count=self._reload_count,
                last_reload_succeeded=True,
                last_error=None,
                using_default_policy=True,
                changed=False,
            )
            return self._status

        self._reload_count += 1
        try:
            loaded = ExecutionPolicy.from_file(self._source_path)
        except Exception as exc:
            self._status = PolicyReloadStatus(
                source_path=self._source_path,
                generation=self._generation,
                reload_count=self._reload_count,
                last_reload_succeeded=False,
                last_error=str(exc),
                using_default_policy=self._using_default_policy,
                changed=False,
            )
            return self._status

        changed = loaded.to_dict() != self._active_policy.to_dict()
        if changed:
            self._generation += 1
            self._active_policy = loaded
            self._using_default_policy = False
        self._status = PolicyReloadStatus(
            source_path=self._source_path,
            generation=self._generation,
            reload_count=self._reload_count,
            last_reload_succeeded=True,
            last_error=None,
            using_default_policy=False,
            changed=changed,
        )
        return self._status

    @staticmethod
    def _resolve_policy_path(policy_path: str | Path | None, env_var: str) -> Path | None:
        if policy_path is not None:
            return Path(policy_path)
        raw = os.getenv(env_var)
        if raw:
            return Path(raw)
        return None


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
