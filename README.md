# NexusKV Architecture Overview

![NexusKV Architecture](https://via.placeholder.com/800x500.png?text=NexusKV+Architecture+Diagram)

## Core Modules

### 1. Log Module (WAL)
- **Segmented Log**: 分片日志存储，单个文件不超过512MB
- **Memory Mapped I/O**: 零拷贝快速读写
- **Batch Commit**: 批量提交降低磁盘IO次数
- **CRC32C Checksum**: 数据完整性校验

### 2. Storage Engine
- **LSM Tree + B+ Tree Hybrid**
- **Value Log Separation**
- **Bloom Filter Acceleration**

### 3. Distributed Layer
- **Raft Consensus**
- **Consistent Hashing**
- **gRPC Streaming**

### 4. Cache System
- **LRU Memory Cache**
- **SSD Cold Cache**
- **SingleFlight Control**

## Data Flow
Client → gRPC API → Log Module → Storage Engine → Distributed Sync → Cache