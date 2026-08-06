#include <string.h>

#include "nexuskv_cuda.h"

// Ampere (sm_80 / A100) NVLink 3 Accelerated MemHandle Path
extern "C" NexusKVHalStatus nexuskv_cuda_register_ampere(uint64_t handle_id, void* ptr,
                                                         size_t size_bytes, int device_id,
                                                         NexusKVUnifiedMemHandle* out_handle) {
    if (!ptr || !out_handle || size_bytes == 0) {
        return NEXUSKV_HAL_ERR_INVALID_PARAM;
    }

    out_handle->handle_id = handle_id;
    out_handle->physical_addr = reinterpret_cast<uint64_t>(ptr);
    out_handle->size_bytes = size_bytes;
    out_handle->device_id = device_id;
    out_handle->vendor = NEXUSKV_VENDOR_NVIDIA;
    out_handle->micro_arch = NEXUSKV_ARCH_NVIDIA_AMPERE;
    memset(out_handle->raw_handle_bytes, 0, sizeof(out_handle->raw_handle_bytes));

    return NEXUSKV_HAL_SUCCESS;
}
