#ifndef NEXUSKV_INTEL_H
#define NEXUSKV_INTEL_H

#include "../include/nexuskv_hal.h"

#ifdef __cplusplus
extern "C" {
#endif

NexusKVHalStatus nexuskv_intel_register(uint64_t handle_id, void* ptr, size_t size_bytes,
                                        int device_id, NexusKVUnifiedMemHandle* out_handle);

NexusKVHalStatus nexuskv_intel_open(const NexusKVUnifiedMemHandle* in_handle, void** out_ptr);

NexusKVHalStatus nexuskv_intel_close(void* ptr);

#ifdef __cplusplus
}
#endif

#endif  // NEXUSKV_INTEL_H
