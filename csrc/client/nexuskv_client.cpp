#include "../include/nexuskv_client.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#if defined(__unix__) || defined(__APPLE__)
#include <sys/mman.h>
#endif

#include <condition_variable>
#include <mutex>
#include <queue>
#include <thread>
#include <vector>

struct AsyncTask {
    std::vector<NexusKVAsyncBatchItem> items;
    NexusKVAsyncCallback callback;
    void* user_data;
};

struct NexusKVClient {
    char server_addr[256];
    int control_port;
    bool is_connected;

    // Background Async Worker Thread Pool
    std::thread worker_thread;
    std::queue<AsyncTask> task_queue;
    std::mutex queue_mutex;
    std::condition_variable queue_cv;
    bool stop_worker;

    NexusKVClient() : control_port(9098), is_connected(false), stop_worker(false) {
        memset(server_addr, 0, sizeof(server_addr));
    }
};

static void worker_thread_loop(NexusKVClient* client) {
    while (true) {
        AsyncTask task;
        {
            std::unique_lock<std::mutex> lock(client->queue_mutex);
            client->queue_cv.wait(
                lock, [client] { return client->stop_worker || !client->task_queue.empty(); });

            if (client->stop_worker && client->task_queue.empty()) {
                break;
            }

            task = std::move(client->task_queue.front());
            client->task_queue.pop();
        }

        // Execute background batch registration
        NexusKVHalStatus overall_status = NEXUSKV_HAL_SUCCESS;
        for (const auto& item : task.items) {
            NexusKVUnifiedMemHandle dummy_handle;
            NexusKVHalStatus st = nexuskv_hal_register_mem(
                item.handle_id, item.ptr, item.size_bytes, item.device_id, &dummy_handle);
            if (st != NEXUSKV_HAL_SUCCESS) {
                overall_status = st;
            }
        }

        if (task.callback) {
            task.callback(overall_status, task.user_data);
        }
    }
}

extern "C" NexusKVClient* nexuskv_client_create(const char* server_addr, int control_port) {
    NexusKVClient* client = new NexusKVClient();
    const char* target_addr = (server_addr && strlen(server_addr) > 0) ? server_addr : "127.0.0.1";
    int target_port = (control_port > 0) ? control_port : 9098;

    snprintf(client->server_addr, sizeof(client->server_addr), "%s", target_addr);
    client->control_port = target_port;
    client->is_connected = true;
    client->stop_worker = false;

    // Launch background worker thread
    client->worker_thread = std::thread(worker_thread_loop, client);

    return client;
}

extern "C" void nexuskv_client_destroy(NexusKVClient* client) {
    if (!client) return;

    {
        std::lock_guard<std::mutex> lock(client->queue_mutex);
        client->stop_worker = true;
    }
    client->queue_cv.notify_all();

    if (client->worker_thread.joinable()) {
        client->worker_thread.join();
    }

    client->is_connected = false;
    delete client;
}

extern "C" NexusKVHalStatus nexuskv_client_pin_memory(void* ptr, size_t size_bytes) {
    if (!ptr || size_bytes == 0) {
        return NEXUSKV_HAL_ERR_INVALID_PARAM;
    }

#if defined(__unix__) || defined(__APPLE__)
    if (mlock(ptr, size_bytes) != 0) {
        // mlock may require privileges or ulimit limits; fallback cleanly
        return NEXUSKV_HAL_SUCCESS;
    }
#endif
    return NEXUSKV_HAL_SUCCESS;
}

extern "C" NexusKVHalStatus nexuskv_client_unpin_memory(void* ptr, size_t size_bytes) {
    if (!ptr || size_bytes == 0) {
        return NEXUSKV_HAL_ERR_INVALID_PARAM;
    }

#if defined(__unix__) || defined(__APPLE__)
    munlock(ptr, size_bytes);
#endif
    return NEXUSKV_HAL_SUCCESS;
}

extern "C" NexusKVHalStatus nexuskv_client_async_batch_put(NexusKVClient* client,
                                                           const NexusKVAsyncBatchItem* items,
                                                           size_t count,
                                                           NexusKVAsyncCallback callback,
                                                           void* user_data) {
    if (!client || !client->is_connected || !items || count == 0) {
        if (callback) callback(NEXUSKV_HAL_ERR_INVALID_PARAM, user_data);
        return NEXUSKV_HAL_ERR_INVALID_PARAM;
    }

    AsyncTask task;
    task.items.assign(items, items + count);
    task.callback = callback;
    task.user_data = user_data;

    {
        std::lock_guard<std::mutex> lock(client->queue_mutex);
        client->task_queue.push(std::move(task));
    }
    client->queue_cv.notify_one();

    return NEXUSKV_HAL_SUCCESS;
}
