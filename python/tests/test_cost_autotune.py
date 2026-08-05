import unittest

from nexuskv.contracts.generated import TierKind
from nexuskv.planner.autotune import DynamicCostProfiler


class TestCostAutotune(unittest.TestCase):
    def test_dynamic_cost_profiler_feedback(self):
        profiler = DynamicCostProfiler()
        initial_prefill_time = profiler.get_current_prefill_time()

        # Record faster prefill sample (1us per token)
        profiler.record_prefill_sample(token_count=1000, duration_sec=0.001)  # 1us per token
        new_prefill_time = profiler.get_current_prefill_time()
        self.assertLess(new_prefill_time, initial_prefill_time)

        # Record higher bandwidth sample
        profiler.record_bandwidth_sample(
            TierKind.HOST_DRAM, payload_bytes=1000000, duration_sec=0.0001
        )  # 10GB/s
        bw = profiler.get_current_bandwidth(TierKind.HOST_DRAM)
        self.assertGreater(bw, 1.0e8)

    def test_multi_backend_hardware_adapters(self):
        from nexuskv.execution.native_transport import (
            AmdRocmHipIpcAdapter,
            CudaIpcHandleAdapter,
            GoogleTpuXlaAdapter,
            HuaweiAscendCannAdapter,
        )

        cuda_adapter = CudaIpcHandleAdapter()
        reg_cuda = cuda_adapter.register_cuda_ipc_handle(
            "h1", b"ipc_handle", uva_ptr=0x7FFF0000, size_bytes=4096
        )
        self.assertTrue(reg_cuda.is_registered)

        amd_adapter = AmdRocmHipIpcAdapter()
        reg_amd = amd_adapter.register_hip_ipc_handle("h1", hip_ptr=0x7FFF1000, size_bytes=4096)
        self.assertTrue(reg_amd.is_registered)

        tpu_adapter = GoogleTpuXlaAdapter()
        reg_tpu = tpu_adapter.register_tpu_buffer("b1", tpu_ptr=0x7FFF2000, size_bytes=4096)
        self.assertTrue(reg_tpu.is_registered)

        ascend_adapter = HuaweiAscendCannAdapter()
        reg_ascend = ascend_adapter.register_ascend_ipc_handle(
            "h1", acl_ptr=0x7FFF3000, size_bytes=4096
        )
        self.assertTrue(reg_ascend.is_registered)


if __name__ == "__main__":
    unittest.main()
