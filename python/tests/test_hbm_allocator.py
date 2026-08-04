import unittest

from nexuskv.execution.hbm import HbmBlockAllocator


class TestHbmAllocator(unittest.TestCase):
    def test_hbm_allocation_and_pin_lifecycle(self):
        allocator = HbmBlockAllocator(total_capacity_bytes=64 * 1024 * 1024, block_size_bytes=16 * 1024 * 1024)
        
        # Allocate 2 blocks
        b1 = allocator.allocate_block()
        b2 = allocator.allocate_block()
        self.assertEqual(allocator.active_block_count, 2)
        self.assertEqual(allocator.pinned_bytes, 32 * 1024 * 1024)

        # Unpin b1 (offload to DRAM)
        self.assertTrue(allocator.unpin_block(b1.block_id))
        self.assertEqual(allocator.pinned_bytes, 16 * 1024 * 1024)

        # Pin b1 back
        self.assertTrue(allocator.pin_block(b1.block_id))
        self.assertEqual(allocator.pinned_bytes, 32 * 1024 * 1024)

        # Free b2
        self.assertTrue(allocator.free_block(b2.block_id))
        self.assertEqual(allocator.active_block_count, 1)


if __name__ == "__main__":
    unittest.main()
