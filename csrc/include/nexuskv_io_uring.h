#ifndef NEXUSKV_IO_URING_H
#define NEXUSKV_IO_URING_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "nexuskv_hal.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct NexusKVIoUringSwapEngine NexusKVIoUringSwapEngine;

NexusKVIoUringSwapEngine* nexuskv_iouring_create(size_t queue_depth);
void nexuskv_iouring_destroy(NexusKVIoUringSwapEngine* engine);

NexusKVHalStatus nexuskv_iouring_async_write(NexusKVIoUringSwapEngine* engine, int fd,
                                             const void* buf, size_t count, uint64_t offset);

NexusKVHalStatus nexuskv_iouring_async_read(NexusKVIoUringSwapEngine* engine, int fd, void* buf,
                                            size_t count, uint64_t offset);

int nexuskv_iouring_submit_and_wait(NexusKVIoUringSwapEngine* engine, int min_complete);

#ifdef __cplusplus
}
#endif

#endif  // NEXUSKV_IO_URING_H
