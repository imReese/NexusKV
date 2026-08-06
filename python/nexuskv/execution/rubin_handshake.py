from __future__ import annotations

import time
from collections.abc import Sequence
from dataclasses import dataclass
from enum import StrEnum


class RubinNodeRole(StrEnum):
    PREFILL_CPX = "prefill_cpx"
    DECODE_HBM4 = "decode_hbm4"


class RubinHandshakeStatus(StrEnum):
    INITIATED = "initiated"
    ACKNOWLEDGED = "acknowledged"
    TRANSFERRING = "transferring"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass(slots=True)
class RubinHandshakeSession:
    session_id: str
    prefill_node_id: str
    decode_node_id: str
    token_count: int
    estimated_transfer_bandwidth_gbps: float
    status: RubinHandshakeStatus = RubinHandshakeStatus.INITIATED
    timestamp: float = 0.0
    completed_chunks: int = 0
    total_chunks: int = 1


class RubinCPXPrefillHandshake:
    """NVIDIA Rubin CPX Hardware-Assisted Prefill-Decode Disaggregation Handshake Engine.

    Manages physical hardware node pairing between Rubin CPX (Prefill specialized processor)
    and Rubin HBM4 Decode nodes over NVLink 6 / UALink 2.0.
    """

    def __init__(self, nvlink_bandwidth_gbps: float = 1800.0) -> None:
        self.nvlink_bandwidth_gbps = nvlink_bandwidth_gbps
        self.active_sessions: dict[str, RubinHandshakeSession] = {}

    def initiate_handshake(
        self,
        session_id: str,
        prefill_node_id: str,
        decode_node_id: str,
        prompt_tokens: Sequence[int],
        chunk_size_tokens: int = 2048,
    ) -> RubinHandshakeSession:
        token_count = len(prompt_tokens)
        total_chunks = max(1, (token_count + chunk_size_tokens - 1) // chunk_size_tokens)

        session = RubinHandshakeSession(
            session_id=session_id,
            prefill_node_id=prefill_node_id,
            decode_node_id=decode_node_id,
            token_count=token_count,
            estimated_transfer_bandwidth_gbps=self.nvlink_bandwidth_gbps,
            status=RubinHandshakeStatus.ACKNOWLEDGED,
            timestamp=time.time(),
            completed_chunks=0,
            total_chunks=total_chunks,
        )
        self.active_sessions[session_id] = session
        return session

    def update_transfer_progress(self, session_id: str, chunk_index: int) -> bool:
        session = self.active_sessions.get(session_id)
        if session is None:
            return False

        session.completed_chunks = max(session.completed_chunks, chunk_index + 1)
        if session.completed_chunks >= session.total_chunks:
            session.status = RubinHandshakeStatus.COMPLETED
        else:
            session.status = RubinHandshakeStatus.TRANSFERRING
        return True

    def complete_handshake(self, session_id: str) -> bool:
        session = self.active_sessions.get(session_id)
        if session is None:
            return False
        session.completed_chunks = session.total_chunks
        session.status = RubinHandshakeStatus.COMPLETED
        return True

    def get_session(self, session_id: str) -> RubinHandshakeSession | None:
        return self.active_sessions.get(session_id)
