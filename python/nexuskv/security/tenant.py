from __future__ import annotations

import hashlib
import hmac
from dataclasses import dataclass, field

from nexuskv.execution.policy import ExecutionPolicy, PlaceholderMode


class TenantAuthorizationError(PermissionError):
    pass


@dataclass(slots=True)
class TenantNamespaceAuthorizer:
    secret_salt: bytes = b"nexuskv_default_salt_2026"
    allowed_tenants: set[str] = field(default_factory=lambda: {"default", "default_tenant", "tenant-a", "tenant-b"})

    def authorize_request(
        self,
        policy: ExecutionPolicy,
        tenant: str,
        namespace: str,
    ) -> bool:
        mode = policy.tenant_namespace_policy.mode
        if mode == PlaceholderMode.DISABLED:
            return True

        if tenant not in self.allowed_tenants:
            if mode == PlaceholderMode.ENFORCED:
                raise TenantAuthorizationError(f"tenant {tenant!r} is not authorized")
            return False
        return True

    def compute_keyed_hash(
        self,
        tenant: str,
        namespace: str,
        model: str,
        tokens: list[int],
    ) -> str:
        h = hmac.new(self.secret_salt, digestmod=hashlib.sha256)
        raw_key = f"{tenant}:{namespace}:{model}:" + ",".join(str(t) for t in tokens)
        h.update(raw_key.encode("utf-8"))
        return h.hexdigest()
