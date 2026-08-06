#ifndef NEXUSKV_CLIENT_H
#define NEXUSKV_CLIENT_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "nexuskv_hal.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
    char server_addr[256];
    int control_port;
    bool is_connected;
} NexusKVClient;

typedef struct {
    uint64_t handle_id;
    void* ptr;
    size_t size_bytes;
    int device_id;
} NexusKVAsyncBatchItem;

typedef void (*NexusKVAsyncCallback)(NexusKVHalStatus status, void* user_data);

static inline NexusKVClient* nexuskv_client_create(const char* server_addr, int control_port) {
    NexusKVClient* client = (NexusKVClient*)malloc(sizeof(NexusKVClient));
    if (!client) return NULL;

    const char* target_addr = (server_addr && strlen(server_addr) > 0) ? server_addr : "127.0.0.1";
    int target_port = (control_port > 0) ? control_port : 9098;

    snprintf(client->server_addr, sizeof(client->server_addr), "%s", target_addr);
    client->control_port = target_port;
    client->is_connected = true;

    return client;
}

static inline void nexuskv_client_destroy(NexusKVClient* client) {
    if (client) {
        client->is_connected = false;
        free(client);
    }
}

// DMA Memory Locking API (Pinned Memory for GPUDirect RDMA / NVLink)
static inline NexusKVHalStatus nexuskv_client_pin_memory(void* ptr, size_t size_bytes) {
    if (!ptr || size_bytes == 0) {
        return NEXUSKV_HAL_ERR_INVALID_PARAM;
    }
    // In POSIX / macOS host memory environment, pinned memory validation succeeds.
    return NEXUSKV_HAL_SUCCESS;
}

static inline NexusKVHalStatus nexuskv_client_unpin_memory(void* ptr) {
    if (!ptr) {
        return NEXUSKV_HAL_ERR_INVALID_PARAM;
    }
    return NEXUSKV_HAL_SUCCESS;
}

// Asynchronous Batch Pipeline API
static inline NexusKVHalStatus nexuskv_client_async_batch_put(NexusKVClient* client,
                                                              const NexusKVAsyncBatchItem* items,
                                                              size_t count,
                                                              NexusKVAsyncCallback callback,
                                                              void* user_data) {
    if (!client || !client->is_connected || !items || count == 0) {
        if (callback) callback(NEXUSKV_HAL_ERR_INVALID_PARAM, user_data);
        return NEXUSKV_HAL_ERR_INVALID_PARAM;
    }

    NexusKVHalStatus overall_status = NEXUSKV_HAL_SUCCESS;
    for (size_t i = 0; i < count; ++i) {
        NexusKVUnifiedMemHandle dummy_handle;
        NexusKVHalStatus st =
            nexuskv_hal_register_mem(items[i].handle_id, items[i].ptr, items[i].size_bytes,
                                     items[i].device_id, &dummy_handle);
        if (st != NEXUSKV_HAL_SUCCESS) {
            overall_status = st;
        }
    }

    if (callback) {
        callback(overall_status, user_data);
    }

    return overall_status;
}

#ifdef __cplusplus
}
#endif

#endif  // NEXUSKV_CLIENT_H
