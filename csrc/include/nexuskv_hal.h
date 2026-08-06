#ifndef NEXUSKV_HAL_H
#define NEXUSKV_HAL_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef enum {
    NEXUSKV_VENDOR_UNKNOWN = 0,
    NEXUSKV_VENDOR_NVIDIA = 1,
    NEXUSKV_VENDOR_AMD = 2,
    NEXUSKV_VENDOR_INTEL = 3,
    NEXUSKV_VENDOR_ASCEND = 4,       // Huawei Ascend 华为昇腾
    NEXUSKV_VENDOR_APPLE_METAL = 5,  // Apple Silicon Metal
    NEXUSKV_VENDOR_CPU_SHM = 6
} NexusKVHardwareVendor;

typedef enum {
    NEXUSKV_ARCH_GENERIC = 0,
    NEXUSKV_ARCH_NVIDIA_AMPERE = 80,      // sm_80 / A100
    NEXUSKV_ARCH_NVIDIA_HOPPER = 90,      // sm_90 / H100
    NEXUSKV_ARCH_NVIDIA_BLACKWELL = 100,  // sm_100 / B200
    NEXUSKV_ARCH_NVIDIA_RUBIN = 120,      // sm_120 / Rubin NVLink 6
    NEXUSKV_ARCH_AMD_CDNA4 = 945,         // MI350X / MI400 CDNA 4
    NEXUSKV_ARCH_ASCEND_910C = 910,       // Ascend 910C HCCS
    NEXUSKV_ARCH_APPLE_M4 = 40,           // Apple M4 Series
    NEXUSKV_ARCH_APPLE_M5 = 50            // Apple M5 Series
} NexusKVMicroArch;

typedef enum {
    NEXUSKV_HAL_SUCCESS = 0,
    NEXUSKV_HAL_ERR_INVALID_PARAM = 1,
    NEXUSKV_HAL_ERR_IPC_FAILED = 2,
    NEXUSKV_HAL_ERR_UNSUPPORTED_VENDOR = 3
} NexusKVHalStatus;

typedef struct {
    NexusKVHardwareVendor vendor;
    NexusKVMicroArch micro_arch;

    // Fine-grained hardware SKU resource profiling
    size_t total_global_mem_bytes;  // Total HBM / GDDR / Unified Memory (e.g., 80GB / 141GB / 48GB)
    size_t memory_bandwidth_gbps;   // HBM/GDDR Bandwidth in GB/s (e.g., 3.35TB/s vs 4.8TB/s)
    uint32_t sm_count;              // Multiprocessor SM Count (e.g., 132 vs 114)
    bool has_nvlink;                // NVLink Interconnect Availability (SXM5 vs PCIe)
    uint32_t nvlink_bandwidth_gbps;  // Interconnect Bandwidth in GB/s (e.g., 900GB/s vs 128GB/s)
    bool is_unified_memory;          // UMA vs NUMA Discrete (e.g., Apple Silicon vs Discrete GPU)
} NexusKVDeviceCapabilities;

typedef struct {
    uint64_t handle_id;
    uint64_t physical_addr;
    size_t size_bytes;
    int device_id;
    NexusKVHardwareVendor vendor;
    NexusKVMicroArch micro_arch;
    uint8_t raw_handle_bytes[256];
} NexusKVUnifiedMemHandle;

// Hardware probe & SKU profiling API
NexusKVHardwareVendor nexuskv_hal_detect_vendor(int device_id);
NexusKVMicroArch nexuskv_hal_detect_microarch(int device_id);
NexusKVHalStatus nexuskv_hal_get_device_caps(int device_id, NexusKVDeviceCapabilities* out_caps);

// Unified HAL API
NexusKVHalStatus nexuskv_hal_register_mem(uint64_t handle_id, void* ptr, size_t size_bytes,
                                          int device_id, NexusKVUnifiedMemHandle* out_handle);

NexusKVHalStatus nexuskv_hal_open_mem(const NexusKVUnifiedMemHandle* in_handle, void** out_ptr);

NexusKVHalStatus nexuskv_hal_close_mem(void* ptr, NexusKVHardwareVendor vendor);

#ifdef __cplusplus
}
#endif

#endif  // NEXUSKV_HAL_H
