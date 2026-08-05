from __future__ import annotations

import ctypes
import threading
from dataclasses import dataclass, field


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

    def register_rdma_pool(
        self, pool_id: str, base_addr: int, size_bytes: int
    ) -> ZeroCopyRegistration:
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

    def register_nvlink_region(
        self, region_id: str, base_addr: int, size_bytes: int
    ) -> ZeroCopyRegistration:
        return self.manager.register_zero_copy_region(
            handle_id=f"nixl_nvlink_{region_id}",
            base_addr=base_addr,
            size_bytes=size_bytes,
        )


@dataclass(slots=True)
class CudaIpcHandleAdapter:
    """Adapter for NVIDIA CUDA IPC (Inter-Process Communication) and UVA (Unified Virtual Addressing)."""

    manager: NativeTransportManager = field(default_factory=NativeTransportManager)

    def register_cuda_ipc_handle(
        self, handle_id: str, ipc_bytes: bytes, uva_ptr: int, size_bytes: int
    ) -> ZeroCopyRegistration:
        return self.manager.register_zero_copy_region(
            handle_id=f"cuda_ipc_{handle_id}",
            base_addr=uva_ptr,
            size_bytes=size_bytes,
        )


@dataclass(slots=True)
class AmdRocmHipIpcAdapter:
    """Adapter for AMD ROCm HIP IPC & HSA Unified Memory."""

    manager: NativeTransportManager = field(default_factory=NativeTransportManager)

    def register_hip_ipc_handle(
        self, handle_id: str, hip_ptr: int, size_bytes: int
    ) -> ZeroCopyRegistration:
        return self.manager.register_zero_copy_region(
            handle_id=f"hip_ipc_{handle_id}",
            base_addr=hip_ptr,
            size_bytes=size_bytes,
        )


@dataclass(slots=True)
class GoogleTpuXlaAdapter:
    """Adapter for Google TPU XLA Paged Buffer Handle."""

    manager: NativeTransportManager = field(default_factory=NativeTransportManager)

    def register_tpu_buffer(
        self, buffer_id: str, tpu_ptr: int, size_bytes: int
    ) -> ZeroCopyRegistration:
        return self.manager.register_zero_copy_region(
            handle_id=f"tpu_xla_{buffer_id}",
            base_addr=tpu_ptr,
            size_bytes=size_bytes,
        )


@dataclass(slots=True)
class HuaweiAscendCannAdapter:
    """Adapter for Huawei Ascend 910B/C CANN ACL IPC Handle."""

    manager: NativeTransportManager = field(default_factory=NativeTransportManager)

    def register_ascend_ipc_handle(
        self, handle_id: str, acl_ptr: int, size_bytes: int
    ) -> ZeroCopyRegistration:
        return self.manager.register_zero_copy_region(
            handle_id=f"ascend_cann_{handle_id}",
            base_addr=acl_ptr,
            size_bytes=size_bytes,
        )


@dataclass(slots=True)
class AppleMetalUmaAdapter:
    """Adapter for Apple Silicon (Mac M3/M4 Ultra) Metal MPS Unified Memory Architecture (UMA)."""

    manager: NativeTransportManager = field(default_factory=NativeTransportManager)

    def register_metal_uma_buffer(
        self, buffer_id: str, host_dram_ptr: int, size_bytes: int
    ) -> ZeroCopyRegistration:
        # Zero-copy native pointer sharing on Apple Silicon UMA (CPU and GPU share identical DRAM address)
        return self.manager.register_zero_copy_region(
            handle_id=f"apple_metal_uma_{buffer_id}",
            base_addr=host_dram_ptr,
            size_bytes=size_bytes,
        )


@dataclass(slots=True)
class IntelGaudiLevelZeroAdapter:
    """Adapter for Intel Gaudi2/Gaudi3 & Xe GPUs via oneAPI Level Zero IPC Handle."""

    manager: NativeTransportManager = field(default_factory=NativeTransportManager)

    def register_level_zero_ipc_handle(
        self, handle_id: str, ze_ptr: int, size_bytes: int
    ) -> ZeroCopyRegistration:
        return self.manager.register_zero_copy_region(
            handle_id=f"intel_ze_{handle_id}",
            base_addr=ze_ptr,
            size_bytes=size_bytes,
        )


@dataclass(slots=True)
class CambriconMluAdapter:
    """Adapter for Cambricon MLU BANG C/C++ IPC Memory Handle."""

    manager: NativeTransportManager = field(default_factory=NativeTransportManager)

    def register_mlu_ipc_handle(
        self, handle_id: str, mlu_ptr: int, size_bytes: int
    ) -> ZeroCopyRegistration:
        return self.manager.register_zero_copy_region(
            handle_id=f"cambricon_mlu_{handle_id}",
            base_addr=mlu_ptr,
            size_bytes=size_bytes,
        )


@dataclass(slots=True)
class MooreThreadsMusaAdapter:
    """Adapter for Moore Threads MUSA GPU IPC Handle."""

    manager: NativeTransportManager = field(default_factory=NativeTransportManager)

    def register_musa_ipc_handle(
        self, handle_id: str, musa_ptr: int, size_bytes: int
    ) -> ZeroCopyRegistration:
        return self.manager.register_zero_copy_region(
            handle_id=f"moore_musa_{handle_id}",
            base_addr=musa_ptr,
            size_bytes=size_bytes,
        )


@dataclass(slots=True)
class BirenBr100Adapter:
    """Adapter for Biren Tech BR100 Device Memory Handle."""

    manager: NativeTransportManager = field(default_factory=NativeTransportManager)

    def register_biren_buffer(
        self, buffer_id: str, biren_ptr: int, size_bytes: int
    ) -> ZeroCopyRegistration:
        return self.manager.register_zero_copy_region(
            handle_id=f"biren_br100_{buffer_id}",
            base_addr=biren_ptr,
            size_bytes=size_bytes,
        )
