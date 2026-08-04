import unittest

from nexuskv.execution.policy import ExecutionPolicy, TenantNamespacePolicy, PlaceholderMode
from nexuskv.security.tenant import TenantNamespaceAuthorizer, TenantAuthorizationError


class TestTenantSecurity(unittest.TestCase):
    def test_tenant_authorization_modes(self):
        authorizer = TenantNamespaceAuthorizer(allowed_tenants={"tenant1"})

        policy_advisory = ExecutionPolicy.default()
        policy_advisory.tenant_namespace_policy = TenantNamespacePolicy(
            mode=PlaceholderMode.ADVISORY,
            default_tenant="default",
            default_namespace="default",
        )

        # Authorized tenant
        self.assertTrue(authorizer.authorize_request(policy_advisory, "tenant1", "ns1"))

        # Unauthorized tenant under advisory returns False without exception
        self.assertFalse(authorizer.authorize_request(policy_advisory, "unauthorized_tenant", "ns1"))

        policy_enforced = ExecutionPolicy.default()
        policy_enforced.tenant_namespace_policy = TenantNamespacePolicy(
            mode=PlaceholderMode.ENFORCED,
            default_tenant="default",
            default_namespace="default",
        )

        # Unauthorized tenant under enforced raises TenantAuthorizationError
        with self.assertRaises(TenantAuthorizationError):
            authorizer.authorize_request(policy_enforced, "unauthorized_tenant", "ns1")

    def test_keyed_hash_prevents_collisions(self):
        authorizer1 = TenantNamespaceAuthorizer(secret_salt=b"salt1")
        authorizer2 = TenantNamespaceAuthorizer(secret_salt=b"salt2")

        hash1 = authorizer1.compute_keyed_hash("tenantA", "ns1", "model1", [1, 2, 3])
        hash2 = authorizer2.compute_keyed_hash("tenantA", "ns1", "model1", [1, 2, 3])

        self.assertNotEqual(hash1, hash2)


if __name__ == "__main__":
    unittest.main()
