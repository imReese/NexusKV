#ifndef NEXUSKV_CUDA_H
#define NEXUSKV_CUDA_H

#include "../include/nexuskv_hal.h"

#ifdef __cplusplus
extern "C" {
#endif

NexusKVHalStatus nexuskv_cuda_register(uint64_t handle_id, void* ptr, size_t size_bytes,
                                       int device_id, NexusKVMicroArch micro_arch,
                                       NexusKVUnifiedMemHandle* out_handle);

NexusKVHalStatus nexuskv_cuda_open(const NexusKVUnifiedMemHandle* in_handle, void** out_ptr);

NexusKVHalStatus nexuskv_cuda_close(void* ptr);

#ifdef __cplusplus
}
#endif

#endif  // NEXUSKV_CUDA_H
