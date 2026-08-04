import unittest

from nexuskv.execution.native_transport import NativeTransportManager


class TestNativeTransport(unittest.TestCase):
    def test_zero_copy_registration_lifecycle(self):
        manager = NativeTransportManager()
        reg = manager.register_zero_copy_region(
            handle_id="h-001",
            base_addr=0x7FFF0000,
            size_bytes=4096,
        )
        self.assertTrue(reg.is_registered)
        self.assertEqual(manager.get_registration("h-001").base_addr, 0x7FFF0000)

        unregistered = manager.unregister_zero_copy_region("h-001")
        self.assertTrue(unregistered)
        self.assertIsNone(manager.get_registration("h-001"))

    def test_pinned_memory_allocation_and_cleanup(self):
        manager = NativeTransportManager()
        buf = manager.allocate_pinned_memory(8192)
        self.assertNotEqual(buf.ptr, 0)
        self.assertEqual(buf.size_bytes, 8192)
        self.assertEqual(manager.active_pinned_bytes, 8192)

        freed = manager.free_pinned_memory(buf.ptr)
        self.assertTrue(freed)
        self.assertEqual(manager.active_pinned_bytes, 0)


if __name__ == "__main__":
    unittest.main()
