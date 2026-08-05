#include <string.h>

#include "nexuskv_cuda.h"

#ifdef __CUDACC__
#include <cuda_runtime.h>
#endif

extern "C" NexusKVCudaStatus nexuskv_cuda_register_region(uint64_t handle_id, void* d_ptr,
                                                          size_t size_bytes, int device_id,
                                                          NexusKVCudaIpcRegion* out_region) {
    if (!d_ptr || !out_region || size_bytes == 0) {
        return NEXUSKV_CUDA_ERR_INVALID_PARAM;
    }

    out_region->handle_id = handle_id;
    out_region->physical_addr = reinterpret_cast<uint64_t>(d_ptr);
    out_region->size_bytes = size_bytes;
    out_region->device_id = device_id;
    memset(out_region->ipc_handle_bytes, 0, sizeof(out_region->ipc_handle_bytes));

#ifdef __CUDACC__
    cudaIpcMemHandle_t handle;
    cudaError_t err = cudaIpcGetMemHandle(&handle, d_ptr);
    if (err != cudaSuccess) {
        return NEXUSKV_CUDA_ERR_IPC_OPEN_FAILED;
    }
    memcpy(out_region->ipc_handle_bytes, &handle, sizeof(handle));
#endif

    return NEXUSKV_CUDA_SUCCESS;
}

extern "C" NexusKVCudaStatus nexuskv_cuda_open_ipc_region(const NexusKVCudaIpcRegion* in_region,
                                                          void** out_d_ptr) {
    if (!in_region || !out_d_ptr) {
        return NEXUSKV_CUDA_ERR_INVALID_PARAM;
    }

#ifdef __CUDACC__
    cudaIpcMemHandle_t handle;
    memcpy(&handle, in_region->ipc_handle_bytes, sizeof(handle));

    cudaError_t err = cudaIpcOpenMemHandle(out_d_ptr, handle, cudaIpcMemLazyEnablePeerAccess);
    if (err != cudaSuccess) {
        return NEXUSKV_CUDA_ERR_IPC_OPEN_FAILED;
    }
#else
    *out_d_ptr = reinterpret_cast<void*>(in_region->physical_addr);
#endif

    return NEXUSKV_CUDA_SUCCESS;
}

extern "C" NexusKVCudaStatus nexuskv_cuda_close_ipc_region(void* d_ptr) {
    if (!d_ptr) {
        return NEXUSKV_CUDA_ERR_INVALID_PARAM;
    }

#ifdef __CUDACC__
    cudaError_t err = cudaIpcCloseMemHandle(d_ptr);
    if (err != cudaSuccess) {
        return NEXUSKV_CUDA_ERR_IPC_OPEN_FAILED;
    }
#endif

    return NEXUSKV_CUDA_SUCCESS;
}
