#ifndef NEXUSKV_CUDA_H
#define NEXUSKV_CUDA_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
    uint64_t handle_id;
    uint64_t physical_addr;
    size_t size_bytes;
    int device_id;
    char ipc_handle_bytes[64];
} NexusKVCudaIpcRegion;

typedef enum {
    NEXUSKV_CUDA_SUCCESS = 0,
    NEXUSKV_CUDA_ERR_INVALID_PARAM = 1,
    NEXUSKV_CUDA_ERR_IPC_OPEN_FAILED = 2,
    NEXUSKV_CUDA_ERR_DEVICE_NOT_FOUND = 3
} NexusKVCudaStatus;

/**
 * Register a CUDA HBM memory region for zero-copy IPC export.
 */
NexusKVCudaStatus nexuskv_cuda_register_region(uint64_t handle_id, void* d_ptr, size_t size_bytes,
                                               int device_id, NexusKVCudaIpcRegion* out_region);

/**
 * Open a CUDA IPC handle from another process for zero-copy P2P access.
 */
NexusKVCudaStatus nexuskv_cuda_open_ipc_region(const NexusKVCudaIpcRegion* in_region,
                                               void** out_d_ptr);

/**
 * Close a previously opened CUDA IPC handle.
 */
NexusKVCudaStatus nexuskv_cuda_close_ipc_region(void* d_ptr);

#ifdef __cplusplus
}
#endif

#endif  // NEXUSKV_CUDA_H
