#ifndef NEXUSKV_METAL_H
#define NEXUSKV_METAL_H

#include "../include/nexuskv_hal.h"

#ifdef __cplusplus
extern "C" {
#endif

NexusKVHalStatus nexuskv_metal_register(uint64_t handle_id, void* ptr, size_t size_bytes,
                                        int device_id, NexusKVUnifiedMemHandle* out_handle);

NexusKVHalStatus nexuskv_metal_open(const NexusKVUnifiedMemHandle* in_handle, void** out_ptr);

NexusKVHalStatus nexuskv_metal_close(void* ptr);

#ifdef __cplusplus
}
#endif

#endif  // NEXUSKV_METAL_H
