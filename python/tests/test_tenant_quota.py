import unittest

from nexuskv.execution.tenant_quota import TenantQuotaManager, TenantQuotaLimit


class TestTenantQuotaManager(unittest.TestCase):
    def test_tenant_isolation_and_reservation(self):
        manager = TenantQuotaManager()
        
        # Set custom limit for tenant_A (1MB)
        manager.set_tenant_limit("tenant_A", TenantQuotaLimit(max_payload_bytes=1024 * 1024, max_entries=10))

        # Tenant A reserve 500KB - should succeed
        ok, reason = manager.check_and_reserve("tenant_A", requested_payload_bytes=500 * 1024)
        self.assertTrue(ok)
        self.assertIsNone(reason)

        # Tenant A reserve another 600KB - should fail (500K + 600K > 1MB)
        ok, reason = manager.check_and_reserve("tenant_A", requested_payload_bytes=600 * 1024)
        self.assertFalse(ok)
        self.assertIn("payload quota exceeded", reason)

        # Tenant B reserve 600KB - should succeed (separate isolation)
        ok, reason = manager.check_and_reserve("tenant_B", requested_payload_bytes=600 * 1024)
        self.assertTrue(ok)

        # Tenant A release 500KB
        manager.release("tenant_A", released_payload_bytes=500 * 1024)

        # Tenant A reserve 600KB again - should now succeed
        ok, reason = manager.check_and_reserve("tenant_A", requested_payload_bytes=600 * 1024)
        self.assertTrue(ok)


if __name__ == "__main__":
    unittest.main()
