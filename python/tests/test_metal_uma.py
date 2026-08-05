import ctypes
import unittest

from nexuskv.execution.native_transport import (
    AppleMetalUmaAdapter,
    BirenBr100Adapter,
    CambriconMluAdapter,
    IntelGaudiLevelZeroAdapter,
    MooreThreadsMusaAdapter,
)


class TestMetalUmaAndExtendedChips(unittest.TestCase):
    def test_apple_metal_uma_zero_copy(self):
        adapter = AppleMetalUmaAdapter()

        # Simulate Mac Unified Memory Address
        raw_buffer = bytearray(1024 * 1024)
        char_array = (ctypes.c_char * len(raw_buffer)).from_buffer(raw_buffer)
        uma_ptr = ctypes.addressof(char_array)

        reg = adapter.register_metal_uma_buffer(
            "metal_buf_1", host_dram_ptr=uma_ptr, size_bytes=len(raw_buffer)
        )
        self.assertTrue(reg.is_registered)
        self.assertEqual(reg.base_addr, uma_ptr)

    def test_extended_chips_adapters(self):
        intel = IntelGaudiLevelZeroAdapter()
        reg_intel = intel.register_level_zero_ipc_handle("h1", ze_ptr=0x7FFF4000, size_bytes=4096)
        self.assertTrue(reg_intel.is_registered)

        cambricon = CambriconMluAdapter()
        reg_cambricon = cambricon.register_mlu_ipc_handle("h1", mlu_ptr=0x7FFF5000, size_bytes=4096)
        self.assertTrue(reg_cambricon.is_registered)

        moore = MooreThreadsMusaAdapter()
        reg_moore = moore.register_musa_ipc_handle("h1", musa_ptr=0x7FFF6000, size_bytes=4096)
        self.assertTrue(reg_moore.is_registered)

        biren = BirenBr100Adapter()
        reg_biren = biren.register_biren_buffer("b1", biren_ptr=0x7FFF7000, size_bytes=4096)
        self.assertTrue(reg_biren.is_registered)


if __name__ == "__main__":
    unittest.main()
