#include <string.h>

#include "nexuskv_metal.h"

// Apple Silicon M1~M5 Metal 4 Unified Memory Zero-Copy Pointer Export/Import Path
extern "C" NexusKVHalStatus nexuskv_metal_register(uint64_t handle_id, void* ptr,
                                                    size_t size_bytes, int device_id,
                                                    NexusKVUnifiedMemHandle* out_handle) {
    if (!ptr || !out_handle || size_bytes == 0) {
        return NEXUSKV_HAL_ERR_INVALID_PARAM;
    }

    out_handle->handle_id = handle_id;
    out_handle->physical_addr = reinterpret_cast<uint64_t>(ptr);
    out_handle->size_bytes = size_bytes;
    out_handle->device_id = device_id;
    out_handle->vendor = NEXUSKV_VENDOR_APPLE_METAL;
    out_handle->micro_arch = NEXUSKV_ARCH_APPLE_M5;
    memset(out_handle->raw_handle_bytes, 0, sizeof(out_handle->raw_handle_bytes));

    return NEXUSKV_HAL_SUCCESS;
}

extern "C" NexusKVHalStatus nexuskv_metal_open(const NexusKVUnifiedMemHandle* in_handle,
                                                void** out_ptr) {
    if (!in_handle || !out_ptr) {
        return NEXUSKV_HAL_ERR_INVALID_PARAM;
    }

    *out_ptr = reinterpret_cast<void*>(in_handle->physical_addr);
    return NEXUSKV_HAL_SUCCESS;
}

extern "C" NexusKVHalStatus nexuskv_metal_close(void* ptr) {
    if (!ptr) {
        return NEXUSKV_HAL_ERR_INVALID_PARAM;
    }
    return NEXUSKV_HAL_SUCCESS;
}
