import os
import unittest

from nexuskv.execution.topology import PPTopologyManager


class TestPPTopology(unittest.TestCase):
    def test_pp_topology_discovery_defaults(self):
        mgr = PPTopologyManager()
        topo = mgr.get_topology()
        self.assertEqual(topo.pp_rank, 0)
        self.assertEqual(topo.pp_size, 1)
        self.assertTrue(topo.is_pipeline_leader)
        self.assertIsNone(topo.downstream_pp_rank)
        self.assertIsNone(topo.upstream_pp_rank)

    def test_pp_topology_discovery_custom_env(self):
        os.environ["PIPELINE_PARALLEL_RANK"] = "1"
        os.environ["PIPELINE_PARALLEL_SIZE"] = "3"
        try:
            mgr = PPTopologyManager()
            topo = mgr.get_topology()
            self.assertEqual(topo.pp_rank, 1)
            self.assertEqual(topo.pp_size, 3)
            self.assertFalse(topo.is_pipeline_leader)
            self.assertEqual(topo.downstream_pp_rank, 2)
            self.assertEqual(topo.upstream_pp_rank, 0)
        finally:
            os.environ.pop("PIPELINE_PARALLEL_RANK", None)
            os.environ.pop("PIPELINE_PARALLEL_SIZE", None)


if __name__ == "__main__":
    unittest.main()
