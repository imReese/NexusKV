#ifndef NEXUSKV_ROCM_H
#define NEXUSKV_ROCM_H

#include "../include/nexuskv_hal.h"

#ifdef __cplusplus
extern "C" {
#endif

NexusKVHalStatus nexuskv_rocm_register(uint64_t handle_id, void* ptr, size_t size_bytes,
                                       int device_id, NexusKVUnifiedMemHandle* out_handle);

NexusKVHalStatus nexuskv_rocm_open(const NexusKVUnifiedMemHandle* in_handle, void** out_ptr);

NexusKVHalStatus nexuskv_rocm_close(void* ptr);

#ifdef __cplusplus
}
#endif

#endif  // NEXUSKV_ROCM_H
