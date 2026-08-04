import unittest

from nexuskv.contracts.generated import TierKind
from nexuskv.planner.autotune import DynamicCostProfiler
from nexuskv.execution.native_transport import MooncakeTransferEngineAdapter, NIXLDriverAdapter


class TestCostAutotune(unittest.TestCase):
    def test_dynamic_cost_profiler_feedback(self):
        profiler = DynamicCostProfiler()
        initial_prefill_time = profiler.get_current_prefill_time()

        # Record faster prefill sample (1us per token)
        profiler.record_prefill_sample(token_count=1000, duration_sec=0.001)  # 1us per token
        new_prefill_time = profiler.get_current_prefill_time()
        self.assertLess(new_prefill_time, initial_prefill_time)

        # Record higher bandwidth sample
        profiler.record_bandwidth_sample(TierKind.HOST_DRAM, payload_bytes=1000000, duration_sec=0.0001)  # 10GB/s
        bw = profiler.get_current_bandwidth(TierKind.HOST_DRAM)
        self.assertGreater(bw, 1.0e8)

    def test_rdma_driver_adapters(self):
        mooncake = MooncakeTransferEngineAdapter()
        reg_mc = mooncake.register_rdma_pool(pool_id="p1", base_addr=0x1000, size_bytes=2048)
        self.assertTrue(reg_mc.is_registered)

        nixl = NIXLDriverAdapter()
        reg_nixl = nixl.register_nvlink_region(region_id="r1", base_addr=0x2000, size_bytes=4096)
        self.assertTrue(reg_nixl.is_registered)


if __name__ == "__main__":
    unittest.main()
