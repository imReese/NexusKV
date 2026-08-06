#ifndef NEXUSKV_CLIENT_H
#define NEXUSKV_CLIENT_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "nexuskv_hal.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct NexusKVClient NexusKVClient;

typedef struct {
    uint64_t handle_id;
    void* ptr;
    size_t size_bytes;
    int device_id;
} NexusKVAsyncBatchItem;

typedef void (*NexusKVAsyncCallback)(NexusKVHalStatus status, void* user_data);

// Client Lifecycle Management
NexusKVClient* nexuskv_client_create(const char* server_addr, int control_port);
void nexuskv_client_destroy(NexusKVClient* client);

// DMA Memory Locking API (POSIX mlock/munlock)
NexusKVHalStatus nexuskv_client_pin_memory(void* ptr, size_t size_bytes);
NexusKVHalStatus nexuskv_client_unpin_memory(void* ptr, size_t size_bytes);

// Asynchronous Batch Pipeline API
NexusKVHalStatus nexuskv_client_async_batch_put(NexusKVClient* client,
                                                const NexusKVAsyncBatchItem* items, size_t count,
                                                NexusKVAsyncCallback callback, void* user_data);

#ifdef __cplusplus
}
#endif

#endif  // NEXUSKV_CLIENT_H
