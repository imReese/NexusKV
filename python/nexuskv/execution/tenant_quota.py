from __future__ import annotations

from dataclasses import dataclass, field
import threading


@dataclass(slots=True)
class TenantQuotaLimit:
    max_payload_bytes: int = 10 * 1024 * 1024 * 1024  # Default 10GB per tenant
    max_entries: int = 100_000
    max_transfers: int = 64
    max_pinned_bytes: int = 2 * 1024 * 1024 * 1024  # Default 2GB pinned limit


@dataclass(slots=True)
class TenantQuotaState:
    active_entries: int = 0
    active_payload_bytes: int = 0
    active_transfers: int = 0
    active_pinned_bytes: int = 0


@dataclass(slots=True)
class TenantQuotaManager:
    """Thread-safe multi-tenant quota manager with hard limits and active backpressure."""
    
    default_limit: TenantQuotaLimit = field(default_factory=TenantQuotaLimit)
    _tenant_limits: dict[str, TenantQuotaLimit] = field(default_factory=dict, init=False)
    _tenant_states: dict[str, TenantQuotaState] = field(default_factory=dict, init=False)
    _lock: threading.Lock = field(default_factory=threading.Lock, init=False)

    def set_tenant_limit(self, tenant_id: str, limit: TenantQuotaLimit) -> None:
        with self._lock:
            self._tenant_limits[tenant_id] = limit

    def get_tenant_limit(self, tenant_id: str) -> TenantQuotaLimit:
        with self._lock:
            return self._tenant_limits.get(tenant_id, self.default_limit)

    def get_tenant_state(self, tenant_id: str) -> TenantQuotaState:
        with self._lock:
            state = self._tenant_states.get(tenant_id)
            if state is None:
                return TenantQuotaState()
            return TenantQuotaState(
                active_entries=state.active_entries,
                active_payload_bytes=state.active_payload_bytes,
                active_transfers=state.active_transfers,
                active_pinned_bytes=state.active_pinned_bytes,
            )

    def check_and_reserve(
        self,
        tenant_id: str,
        requested_payload_bytes: int = 0,
        requested_pinned_bytes: int = 0,
    ) -> tuple[bool, str | None]:
        with self._lock:
            limit = self._tenant_limits.get(tenant_id, self.default_limit)
            state = self._tenant_states.setdefault(tenant_id, TenantQuotaState())

            if state.active_payload_bytes + requested_payload_bytes > limit.max_payload_bytes:
                return False, f"Tenant '{tenant_id}' payload quota exceeded: {state.active_payload_bytes + requested_payload_bytes} > {limit.max_payload_bytes}"
            
            if state.active_entries + 1 > limit.max_entries:
                return False, f"Tenant '{tenant_id}' entries quota exceeded: {state.active_entries + 1} > {limit.max_entries}"

            if state.active_pinned_bytes + requested_pinned_bytes > limit.max_pinned_bytes:
                return False, f"Tenant '{tenant_id}' pinned quota exceeded: {state.active_pinned_bytes + requested_pinned_bytes} > {limit.max_pinned_bytes}"

            # Reserve
            state.active_entries += 1
            state.active_payload_bytes += requested_payload_bytes
            state.active_pinned_bytes += requested_pinned_bytes
            return True, None

    def release(
        self,
        tenant_id: str,
        released_payload_bytes: int = 0,
        released_pinned_bytes: int = 0,
    ) -> None:
        with self._lock:
            state = self._tenant_states.get(tenant_id)
            if state is None:
                return
            state.active_entries = max(0, state.active_entries - 1)
            state.active_payload_bytes = max(0, state.active_payload_bytes - released_payload_bytes)
            state.active_pinned_bytes = max(0, state.active_pinned_bytes - released_pinned_bytes)
