#include <string.h>

#include "nexuskv_cuda.h"

#ifdef __CUDACC__
#include <cuda_runtime.h>
#endif

extern "C" NexusKVHalStatus nexuskv_cuda_register(uint64_t handle_id, void* ptr, size_t size_bytes,
                                                  int device_id, NexusKVMicroArch micro_arch,
                                                  NexusKVUnifiedMemHandle* out_handle) {
    if (!ptr || !out_handle || size_bytes == 0) {
        return NEXUSKV_HAL_ERR_INVALID_PARAM;
    }

    out_handle->handle_id = handle_id;
    out_handle->physical_addr = reinterpret_cast<uint64_t>(ptr);
    out_handle->size_bytes = size_bytes;
    out_handle->device_id = device_id;
    out_handle->vendor = NEXUSKV_VENDOR_NVIDIA;
    out_handle->micro_arch = micro_arch;
    memset(out_handle->raw_handle_bytes, 0, sizeof(out_handle->raw_handle_bytes));

#ifdef __CUDACC__
    cudaIpcMemHandle_t handle;
    cudaError_t err = cudaIpcGetMemHandle(&handle, ptr);
    if (err != cudaSuccess) {
        return NEXUSKV_HAL_ERR_IPC_FAILED;
    }
    memcpy(out_handle->raw_handle_bytes, &handle, sizeof(handle));
#endif

    return NEXUSKV_HAL_SUCCESS;
}

extern "C" NexusKVHalStatus nexuskv_cuda_open(const NexusKVUnifiedMemHandle* in_handle,
                                              void** out_ptr) {
    if (!in_handle || !out_ptr) {
        return NEXUSKV_HAL_ERR_INVALID_PARAM;
    }

#ifdef __CUDACC__
    cudaIpcMemHandle_t handle;
    memcpy(&handle, in_handle->raw_handle_bytes, sizeof(handle));

    cudaError_t err = cudaIpcOpenMemHandle(out_ptr, handle, cudaIpcMemLazyEnablePeerAccess);
    if (err != cudaSuccess) {
        return NEXUSKV_HAL_ERR_IPC_FAILED;
    }
#else
    *out_ptr = reinterpret_cast<void*>(in_handle->physical_addr);
#endif

    return NEXUSKV_HAL_SUCCESS;
}

extern "C" NexusKVHalStatus nexuskv_cuda_close(void* ptr) {
    if (!ptr) {
        return NEXUSKV_HAL_ERR_INVALID_PARAM;
    }

#ifdef __CUDACC__
    cudaError_t err = cudaIpcCloseMemHandle(ptr);
    if (err != cudaSuccess) {
        return NEXUSKV_HAL_ERR_IPC_FAILED;
    }
#endif

    return NEXUSKV_HAL_SUCCESS;
}
