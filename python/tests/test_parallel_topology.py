import os
import unittest

from nexuskv.execution.runner import BaselineExecutionRunner, ParallelTopologyPolicy


class TestParallelTopologyPolicy(unittest.TestCase):
    def test_single_node_fast_path_defaults(self):
        policies = ParallelTopologyPolicy.resolve_topology_policy(
            pp_size=1, tp_size=1, cp_size=1, ep_size=1
        )
        self.assertTrue(policies["is_single_node_fast_path"])
        self.assertFalse(policies["enable_pp_min_prefix_lock"])
        self.assertFalse(policies["enable_tp_stride_alignment"])
        self.assertFalse(policies["enable_cp_sequence_partitioning"])
        self.assertFalse(policies["enable_ep_cxl_slice_routing"])

    def test_pipeline_parallel_policy(self):
        policies = ParallelTopologyPolicy.resolve_topology_policy(
            pp_size=2, tp_size=1, cp_size=1, ep_size=1
        )
        self.assertFalse(policies["is_single_node_fast_path"])
        self.assertTrue(policies["enable_pp_min_prefix_lock"])
        self.assertFalse(policies["enable_tp_stride_alignment"])

    def test_tensor_parallel_policy(self):
        policies = ParallelTopologyPolicy.resolve_topology_policy(
            pp_size=1, tp_size=4, cp_size=1, ep_size=1
        )
        self.assertFalse(policies["is_single_node_fast_path"])
        self.assertFalse(policies["enable_pp_min_prefix_lock"])
        self.assertTrue(policies["enable_tp_stride_alignment"])

    def test_runner_topology_resolution(self):
        os.environ["PIPELINE_PARALLEL_SIZE"] = "2"
        os.environ["TENSOR_PARALLEL_SIZE"] = "4"
        try:
            runner = BaselineExecutionRunner()
            policies = runner.resolve_topology_policy()
            self.assertTrue(policies["enable_pp_min_prefix_lock"])
            self.assertTrue(policies["enable_tp_stride_alignment"])
            self.assertFalse(policies["is_single_node_fast_path"])
        finally:
            os.environ.pop("PIPELINE_PARALLEL_SIZE", None)
            os.environ.pop("TENSOR_PARALLEL_SIZE", None)


if __name__ == "__main__":
    unittest.main()
