#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "../include/nexuskv_io_uring.h"

#if defined(__linux__)
#include <liburing.h>
#else
#include <fcntl.h>
#include <unistd.h>
#endif

#include <atomic>
#include <mutex>
#include <vector>

struct PendingIO {
    int fd;
    void* buf;
    size_t count;
    uint64_t offset;
    bool is_write;
};

struct NexusKVIoUringSwapEngine {
    size_t queue_depth;
    std::mutex engine_mutex;
    std::atomic<uint64_t> completed_ops;

#if defined(__linux__)
    struct io_uring ring;
    bool has_ring;
#else
    std::vector<PendingIO> pending_queue;
#endif

    NexusKVIoUringSwapEngine(size_t depth) : queue_depth(depth), completed_ops(0) {
#if defined(__linux__)
        int ret = io_uring_queue_init(depth, &ring, IORING_SETUP_SQPOLL);
        if (ret < 0) {
            // Fallback to standard io_uring if SQPOLL privileges unavailable
            ret = io_uring_queue_init(depth, &ring, 0);
        }
        has_ring = (ret == 0);
#endif
    }

    ~NexusKVIoUringSwapEngine() {
#if defined(__linux__)
        if (has_ring) {
            io_uring_queue_exit(&ring);
        }
#endif
    }
};

extern "C" NexusKVIoUringSwapEngine* nexuskv_iouring_create(size_t queue_depth) {
    size_t depth = (queue_depth > 0) ? queue_depth : 256;
    return new NexusKVIoUringSwapEngine(depth);
}

extern "C" void nexuskv_iouring_destroy(NexusKVIoUringSwapEngine* engine) {
    if (engine) {
        delete engine;
    }
}

extern "C" NexusKVHalStatus nexuskv_iouring_async_write(NexusKVIoUringSwapEngine* engine, int fd,
                                                        const void* buf, size_t count,
                                                        uint64_t offset) {
    if (!engine || fd < 0 || !buf || count == 0) {
        return NEXUSKV_HAL_ERR_INVALID_PARAM;
    }

    std::lock_guard<std::mutex> lock(engine->engine_mutex);

#if defined(__linux__)
    if (engine->has_ring) {
        struct io_uring_sqe* sqe = io_uring_get_sqe(&engine->ring);
        if (!sqe) return NEXUSKV_HAL_ERR_IPC_FAILED;
        io_uring_prep_write(sqe, fd, buf, count, offset);
        return NEXUSKV_HAL_SUCCESS;
    }
#endif

    // Cross-Platform POSIX Direct Pwrite Fallback
    ssize_t written = pwrite(fd, buf, count, offset);
    if (written > 0) {
        engine->completed_ops.fetch_add(1, std::memory_order_relaxed);
        return NEXUSKV_HAL_SUCCESS;
    }
    return NEXUSKV_HAL_ERR_IPC_FAILED;
}

extern "C" NexusKVHalStatus nexuskv_iouring_async_read(NexusKVIoUringSwapEngine* engine, int fd,
                                                       void* buf, size_t count, uint64_t offset) {
    if (!engine || fd < 0 || !buf || count == 0) {
        return NEXUSKV_HAL_ERR_INVALID_PARAM;
    }

    std::lock_guard<std::mutex> lock(engine->engine_mutex);

#if defined(__linux__)
    if (engine->has_ring) {
        struct io_uring_sqe* sqe = io_uring_get_sqe(&engine->ring);
        if (!sqe) return NEXUSKV_HAL_ERR_IPC_FAILED;
        io_uring_prep_read(sqe, fd, buf, count, offset);
        return NEXUSKV_HAL_SUCCESS;
    }
#endif

    // Cross-Platform POSIX Direct Pread Fallback
    ssize_t read_bytes = pread(fd, buf, count, offset);
    if (read_bytes > 0) {
        engine->completed_ops.fetch_add(1, std::memory_order_relaxed);
        return NEXUSKV_HAL_SUCCESS;
    }
    return NEXUSKV_HAL_ERR_IPC_FAILED;
}

extern "C" int nexuskv_iouring_submit_and_wait(NexusKVIoUringSwapEngine* engine, int min_complete) {
    if (!engine) return -1;

    std::lock_guard<std::mutex> lock(engine->engine_mutex);

#if defined(__linux__)
    if (engine->has_ring) {
        return io_uring_submit_and_wait(&engine->ring, min_complete);
    }
#endif

    return static_cast<int>(engine->completed_ops.exchange(0, std::memory_order_relaxed));
}
