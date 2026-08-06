#include <string.h>

#include "nexuskv_cuda.h"

// Blackwell (sm_100 / B200 / GB200) NVLink 6 Multicast & Hardware Decompress Path
extern "C" NexusKVHalStatus nexuskv_cuda_register_blackwell(uint64_t handle_id, void* ptr,
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
    out_handle->micro_arch = NEXUSKV_ARCH_NVIDIA_BLACKWELL;
    memset(out_handle->raw_handle_bytes, 0, sizeof(out_handle->raw_handle_bytes));

    return NEXUSKV_HAL_SUCCESS;
}
