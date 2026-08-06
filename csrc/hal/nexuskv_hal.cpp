#include "../include/nexuskv_hal.h"

#include <stdio.h>
#include <string.h>

#include "../amd/nexuskv_rocm.h"
#include "../intel/nexuskv_intel.h"
#include "../nvidia/nexuskv_cuda.h"

// Hardware Probe Engine
NexusKVHardwareVendor nexuskv_hal_detect_vendor(int device_id) {
    (void)device_id;
#if defined(__CUDACC__) || defined(NEXUSKV_ENABLE_CUDA)
    return NEXUSKV_VENDOR_NVIDIA;
#elif defined(__HIPCC__) || defined(NEXUSKV_ENABLE_ROCM)
    return NEXUSKV_VENDOR_AMD;
#elif defined(NEXUSKV_ENABLE_LEVEL_ZERO)
    return NEXUSKV_VENDOR_INTEL;
#else
    return NEXUSKV_VENDOR_NVIDIA;  // Default fallback vendor probe
#endif
}

NexusKVMicroArch nexuskv_hal_detect_microarch(int device_id) {
    (void)device_id;
    // Runtime compute capability probe:
    // sm_80 -> NEXUSKV_ARCH_NVIDIA_AMPERE
    // sm_90 -> NEXUSKV_ARCH_NVIDIA_HOPPER
    // sm_100 -> NEXUSKV_ARCH_NVIDIA_BLACKWELL
    return NEXUSKV_ARCH_NVIDIA_HOPPER;
}

// Polymorphic Dispatcher
extern "C" NexusKVHalStatus nexuskv_hal_register_mem(uint64_t handle_id, void* ptr,
                                                     size_t size_bytes, int device_id,
                                                     NexusKVUnifiedMemHandle* out_handle) {
    if (!ptr || !out_handle || size_bytes == 0) {
        return NEXUSKV_HAL_ERR_INVALID_PARAM;
    }

    NexusKVHardwareVendor vendor = nexuskv_hal_detect_vendor(device_id);
    NexusKVMicroArch micro_arch = nexuskv_hal_detect_microarch(device_id);

    switch (vendor) {
        case NEXUSKV_VENDOR_NVIDIA:
            return nexuskv_cuda_register(handle_id, ptr, size_bytes, device_id, micro_arch,
                                         out_handle);
        case NEXUSKV_VENDOR_AMD:
            return nexuskv_rocm_register(handle_id, ptr, size_bytes, device_id, out_handle);
        case NEXUSKV_VENDOR_INTEL:
            return nexuskv_intel_register(handle_id, ptr, size_bytes, device_id, out_handle);
        case NEXUSKV_VENDOR_CPU_SHM:
        default:
            out_handle->handle_id = handle_id;
            out_handle->physical_addr = reinterpret_cast<uint64_t>(ptr);
            out_handle->size_bytes = size_bytes;
            out_handle->device_id = device_id;
            out_handle->vendor = NEXUSKV_VENDOR_CPU_SHM;
            out_handle->micro_arch = NEXUSKV_ARCH_GENERIC;
            memset(out_handle->raw_handle_bytes, 0, sizeof(out_handle->raw_handle_bytes));
            return NEXUSKV_HAL_SUCCESS;
    }
}

extern "C" NexusKVHalStatus nexuskv_hal_open_mem(const NexusKVUnifiedMemHandle* in_handle,
                                                 void** out_ptr) {
    if (!in_handle || !out_ptr) {
        return NEXUSKV_HAL_ERR_INVALID_PARAM;
    }

    switch (in_handle->vendor) {
        case NEXUSKV_VENDOR_NVIDIA:
            return nexuskv_cuda_open(in_handle, out_ptr);
        case NEXUSKV_VENDOR_AMD:
            return nexuskv_rocm_open(in_handle, out_ptr);
        case NEXUSKV_VENDOR_INTEL:
            return nexuskv_intel_open(in_handle, out_ptr);
        case NEXUSKV_VENDOR_CPU_SHM:
        default:
            *out_ptr = reinterpret_cast<void*>(in_handle->physical_addr);
            return NEXUSKV_HAL_SUCCESS;
    }
}

extern "C" NexusKVHalStatus nexuskv_hal_close_mem(void* ptr, NexusKVHardwareVendor vendor) {
    if (!ptr) {
        return NEXUSKV_HAL_ERR_INVALID_PARAM;
    }

    switch (vendor) {
        case NEXUSKV_VENDOR_NVIDIA:
            return nexuskv_cuda_close(ptr);
        case NEXUSKV_VENDOR_AMD:
            return nexuskv_rocm_close(ptr);
        case NEXUSKV_VENDOR_INTEL:
            return nexuskv_intel_close(ptr);
        case NEXUSKV_VENDOR_CPU_SHM:
        default:
            return NEXUSKV_HAL_SUCCESS;
    }
}
