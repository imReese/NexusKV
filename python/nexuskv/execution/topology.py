from __future__ import annotations

import os
from dataclasses import dataclass, field

from nexuskv.logger import logger


@dataclass(slots=True)
class PPTopologyGroup:
    """Represents Pipeline Parallelism topology group awareness."""

    rank: int = 0
    world_size: int = 1
    pp_rank: int = 0
    pp_size: int = 1
    tp_rank: int = 0
    tp_size: int = 1
    master_addr: str = "127.0.0.1"
    peer_pp_ranks: list[int] = field(default_factory=list)
    node_ip_map: dict[int, str] = field(default_factory=dict)

    @property
    def is_pipeline_leader(self) -> bool:
        """Returns True if this Sidecar sits at Pipeline Stage 0."""
        return self.pp_rank == 0

    @property
    def downstream_pp_rank(self) -> int | None:
        """Returns the downstream PP stage rank, or None if last stage."""
        if self.pp_rank + 1 < self.pp_size:
            return self.pp_rank + 1
        return None

    @property
    def upstream_pp_rank(self) -> int | None:
        """Returns the upstream PP stage rank, or None if Stage 0."""
        if self.pp_rank > 0:
            return self.pp_rank - 1
        return None


class PPTopologyManager:
    """Auto-discovers PyTorch/MPI Pipeline Parallelism topology & P2P channels."""

    def __init__(self) -> None:
        self.topology = self._discover_topology()

    def _discover_topology(self) -> PPTopologyGroup:
        """Discovers PP_RANK, TP_RANK, and WORLD_SIZE from process environment variables."""
        rank = int(os.getenv("RANK", "0"))
        world_size = int(os.getenv("WORLD_SIZE", "1"))

        # Pipeline Parallelism Rank Discovery (PyTorch Distributed / SGLang / vLLM)
        pp_rank = int(
            os.getenv(
                "PIPELINE_PARALLEL_RANK",
                os.getenv("PP_RANK", os.getenv("TORCH_PP_RANK", "0")),
            )
        )
        pp_size = int(
            os.getenv(
                "PIPELINE_PARALLEL_SIZE",
                os.getenv("PP_SIZE", os.getenv("TORCH_PP_SIZE", "1")),
            )
        )

        # Tensor Parallelism Rank Discovery
        tp_rank = int(
            os.getenv(
                "TENSOR_PARALLEL_RANK",
                os.getenv("TP_RANK", os.getenv("TORCH_TP_RANK", "0")),
            )
        )
        tp_size = int(
            os.getenv(
                "TENSOR_PARALLEL_SIZE",
                os.getenv("TP_SIZE", os.getenv("TORCH_TP_SIZE", "1")),
            )
        )

        master_addr = os.getenv("MASTER_ADDR", "127.0.0.1")

        peer_pp_ranks = [i for i in range(pp_size) if i != pp_rank]
        node_ip_map = dict.fromkeys(range(pp_size), master_addr)

        group = PPTopologyGroup(
            rank=rank,
            world_size=world_size,
            pp_rank=pp_rank,
            pp_size=pp_size,
            tp_rank=tp_rank,
            tp_size=tp_size,
            master_addr=master_addr,
            peer_pp_ranks=peer_pp_ranks,
            node_ip_map=node_ip_map,
        )

        logger.info(
            f"Discovered PP Topology: PP_RANK={group.pp_rank}/{group.pp_size}, "
            f"TP_RANK={group.tp_rank}/{group.tp_size}, Leader={group.is_pipeline_leader}"
        )
        return group

    def get_topology(self) -> PPTopologyGroup:
        return self.topology
