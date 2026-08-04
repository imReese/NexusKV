from __future__ import annotations

import ctypes
from dataclasses import dataclass, field
import threading


@dataclass(slots=True)
class NativePinnedBuffer:
    ptr: int
    size_bytes: int
    is_active: bool = True


@dataclass(slots=True)
class ZeroCopyRegistration:
    handle_id: str
    base_addr: int
    size_bytes: int
    is_registered: bool = True


class NativeTransportManager:
    """Manages physical zero-copy memory registrations and Host DRAM pinned memory."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._registrations: dict[str, ZeroCopyRegistration] = {}
        self._allocated_buffers: dict[int, NativePinnedBuffer] = {}

    def register_zero_copy_region(
        self,
        handle_id: str,
        base_addr: int,
        size_bytes: int,
    ) -> ZeroCopyRegistration:
        reg = ZeroCopyRegistration(
            handle_id=handle_id,
            base_addr=base_addr,
            size_bytes=size_bytes,
            is_registered=True,
        )
        with self._lock:
            self._registrations[handle_id] = reg
        return reg

    def unregister_zero_copy_region(self, handle_id: str) -> bool:
        with self._lock:
            if handle_id in self._registrations:
                del self._registrations[handle_id]
                return True
            return False

    def allocate_pinned_memory(self, size_bytes: int) -> NativePinnedBuffer:
        # Simulate OS/CUDA pinned memory allocation via ctypes bytearray pointer
        raw_buffer = bytearray(size_bytes)
        char_array = (ctypes.c_char * size_bytes).from_buffer(raw_buffer)
        ptr = ctypes.addressof(char_array)

        buf = NativePinnedBuffer(ptr=ptr, size_bytes=size_bytes, is_active=True)
        with self._lock:
            self._allocated_buffers[ptr] = buf
        return buf

    def free_pinned_memory(self, ptr: int) -> bool:
        with self._lock:
            if ptr in self._allocated_buffers:
                self._allocated_buffers[ptr].is_active = False
                del self._allocated_buffers[ptr]
                return True
            return False

    def get_registration(self, handle_id: str) -> ZeroCopyRegistration | None:
        with self._lock:
            return self._registrations.get(handle_id)

    @property
    def active_pinned_bytes(self) -> int:
        with self._lock:
            return sum(b.size_bytes for b in self._allocated_buffers.values() if b.is_active)


@dataclass(slots=True)
class MooncakeTransferEngineAdapter:
    """Adapter for Moonshot AI Mooncake Transfer Engine C++ RDMA driver."""

    engine_name: str = "MooncakeTransferEngine"
    manager: NativeTransportManager = field(default_factory=NativeTransportManager)

    def register_rdma_pool(self, pool_id: str, base_addr: int, size_bytes: int) -> ZeroCopyRegistration:
        return self.manager.register_zero_copy_region(
            handle_id=f"mooncake_pool_{pool_id}",
            base_addr=base_addr,
            size_bytes=size_bytes,
        )


@dataclass(slots=True)
class NIXLDriverAdapter:
    """Adapter for NVIDIA NIXL RDMA & NVLink transport driver."""

    driver_name: str = "NVIDIA_NIXL_Driver"
    manager: NativeTransportManager = field(default_factory=NativeTransportManager)

    def register_nvlink_region(self, region_id: str, base_addr: int, size_bytes: int) -> ZeroCopyRegistration:
        return self.manager.register_zero_copy_region(
            handle_id=f"nixl_nvlink_{region_id}",
            base_addr=base_addr,
            size_bytes=size_bytes,
        )
