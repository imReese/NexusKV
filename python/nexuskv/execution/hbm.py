from __future__ import annotations

import ctypes
from dataclasses import dataclass, field
import threading


@dataclass(slots=True)
class HbmBlockHandle:
    block_id: int
    ptr: int
    size_bytes: int
    is_pinned: bool = True
    is_active: bool = True


@dataclass(slots=True)
class HbmBlockAllocator:
    total_capacity_bytes: int = 16 * 1024 * 1024 * 1024  # 16 GB HBM Pool
    block_size_bytes: int = 16 * 1024 * 1024           # 16 MB Block Size
    _lock: threading.Lock = field(default_factory=threading.Lock, init=False)
    _allocated_bytes: int = field(default=0, init=False)
    _pinned_bytes: int = field(default=0, init=False)
    _next_block_id: int = field(default=1, init=False)
    _blocks: dict[int, HbmBlockHandle] = field(default_factory=dict, init=False)

    def allocate_block(self) -> HbmBlockHandle:
        with self._lock:
            if self._allocated_bytes + self.block_size_bytes > self.total_capacity_bytes:
                raise MemoryError("HBM Pool Out of Memory")

            block_id = self._next_block_id
            self._next_block_id += 1
            ptr = 0x7FFF00000000 + (block_id * self.block_size_bytes)

            handle = HbmBlockHandle(
                block_id=block_id,
                ptr=ptr,
                size_bytes=self.block_size_bytes,
                is_pinned=True,
                is_active=True,
            )

            self._allocated_bytes += self.block_size_bytes
            self._pinned_bytes += self.block_size_bytes
            self._blocks[block_id] = handle
            return handle

    def unpin_block(self, block_id: int) -> bool:
        with self._lock:
            if block_id in self._blocks:
                handle = self._blocks[block_id]
                if handle.is_pinned:
                    handle.is_pinned = False
                    self._pinned_bytes -= handle.size_bytes
                    return True
            return False

    def pin_block(self, block_id: int) -> bool:
        with self._lock:
            if block_id in self._blocks:
                handle = self._blocks[block_id]
                if not handle.is_pinned:
                    handle.is_pinned = True
                    self._pinned_bytes += handle.size_bytes
                    return True
            return False

    def free_block(self, block_id: int) -> bool:
        with self._lock:
            if block_id in self._blocks:
                handle = self._blocks.pop(block_id)
                self._allocated_bytes -= handle.size_bytes
                if handle.is_pinned:
                    self._pinned_bytes -= handle.size_bytes
                return True
            return False

    @property
    def allocated_bytes(self) -> int:
        with self._lock:
            return self._allocated_bytes

    @property
    def pinned_bytes(self) -> int:
        with self._lock:
            return self._pinned_bytes

    @property
    def active_block_count(self) -> int:
        with self._lock:
            return len(self._blocks)
