"""Unit tests for NIC Selector and Transport Failover State Machine."""

import unittest

from nexuskv.execution.failover import TransportFailoverEngine, TransportTier
from nexuskv.execution.nic_selector import NetworkInterfaceInfo, NICSelector


class TestNICSelectorAndFailover(unittest.TestCase):
    def test_nic_discovery_and_selection(self) -> None:
        selector = NICSelector()
        nics = selector.discover_nics()
        self.assertTrue(len(nics) > 0)

        best_nic = selector.select_best_nic(target_gpu_id=0)
        self.assertIsNotNone(best_nic.device_name)
        self.assertTrue(isinstance(best_nic, NetworkInterfaceInfo))

    def test_failover_cascade_same_node(self) -> None:
        engine = TransportFailoverEngine()
        result = engine.execute_with_failover(same_node=True, rdma_available=True)
        self.assertEqual(result.selected_tier, TransportTier.CUDA_IPC)
        self.assertFalse(result.is_degraded)

    def test_failover_cascade_inter_node(self) -> None:
        engine = TransportFailoverEngine()
        result = engine.execute_with_failover(same_node=False, rdma_available=True)
        self.assertEqual(result.selected_tier, TransportTier.RDMA_ROCE)

    def test_failover_cascade_all_failed(self) -> None:
        engine = TransportFailoverEngine()
        result = engine.execute_with_failover(same_node=False, rdma_available=False)
        self.assertEqual(result.selected_tier, TransportTier.FAIL_OPEN_RECOMPUTE)
        self.assertTrue(result.is_degraded)


if __name__ == "__main__":
    unittest.main()
