// pkg/wal/wal.go
package wal

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"sync"
	"syscall"
	"time"

	"go.uber.org/zap"
)

var (
	// 条目头结构: 4字节长度 + 8字节索引 + 4字节CRC
	headerSize = 16
	castagnoli = crc32.MakeTable(crc32.Castagnoli)
)

type LogEntry struct {
	Data  []byte
	Index uint64 // 全局唯一递增序号
	CRC32 uint32 // 校验码
}

type WAL struct {
	mu          sync.Mutex
	activeFile  *os.File      // 当前写入文件
	buffer      []*LogEntry   // 批量缓冲
	batchSize   int           // 批量提交阈值
	segmentDir  string        // 日志分片存储目录
	segmentSize int64         // 单个分片最大尺寸
	nextIndex   uint64        // 下一条目索引
	notifyChan  chan struct{} // 刷盘信号通道
	closeChan   chan struct{} // 关闭刷盘通道
	logger      *zap.Logger
	segments    []*SegmentMeta
}

type SegmentMeta struct {
	StartIndex uint64 // 本分片起始日志索引
	EndIndex   uint64 // 本分片结束日志索引（初始为0）
	FilePath   string
	IsSealed   bool // 是否已关闭（正常关闭会有结束标记）
}

const (
	DefaultSegmentSize = 512 * 1024 * 1024 // 512MB
	DefaultBatchSize   = 1000              // 每批次提交条目数
)

// NewWAL 初始化WAL示例, 自动恢复现有日志
func NewWAL(segmentDir string) (*WAL, error) {
	if err := os.MkdirAll(segmentDir, 0755); err != nil {
		return nil, fmt.Errorf("create segment dir failed: %v", err)
	}

	wal := &WAL{
		segmentDir:  segmentDir,
		batchSize:   DefaultBatchSize,
		segmentSize: DefaultSegmentSize,
		notifyChan:  make(chan struct{}, 1),
		closeChan:   make(chan struct{}),
		logger:      logger,
		segments:    make([]*SegmentMeta, 0),
	}

	if err := wal.recoverSegments(); err != nil {
		return nil, err
	}

	go wal.backgroundFlush()

	return wal, nil
}

// Append 线程安全写入(非阻塞)
func (w *WAL) Append(entry *LogEntry) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	entry.Index = w.nextIndex
	entry.CRC32 = crc32.Checksum(entry.Data, castagnoli)
	w.nextIndex++

	w.buffer = append(w.buffer, entry)

	if len(w.buffer) >= w.batchSize {
		select {
		case w.notifyChan <- struct{}{}:
		default:
		}
	}
	return nil
}

// backgroundFlusher 定时刷盘(默认每秒+批量双触发)
func (w *WAL) backgroundFlush() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			w.flush()
		case <-w.notifyChan:
			w.flush()
		case <-w.closeChan:
			return
		}
	}
}

// Flush 主动刷盘(暴露给外部调用)
func (w *WAL) flush() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if len(w.buffer) == 0 {
		return nil
	}

	buf := make([]byte, 0, len(w.buffer)*128)
	for _, entry := range w.buffer {
		head := make([]byte, headerSize)
		binary.BigEndian.PutUint32(head[0:4], uint32(len(entry.Data)))
		binary.BigEndian.PutUint64(head[4:12], entry.Index)
		binary.BigEndian.PutUint32(head[12:16], entry.CRC32)
		buf = append(buf, head...)
		buf = append(buf, entry.Date...)
	}

	if _, err := w.activeFile.Write(buf); err != nil {
		return fmt.Errorf("write WAL failed: %v", err)
	}

	if err := w.activeFile.Sync(); err != nil {
		return fmt.Errorf("fsync failed: %v", err)
	}

	if stat, _ := w.activeFile.Stat(); stat.Size() >= w.segmentSize {
		if err := w.rotateSegment(); err != nil {
			return err
		}
	}

	w.buffer = w.buffer[:0]
	return nil
}

// Read 读取指定索引开始的条目
func (w *WAL) Read(fromIndex uint64) ([]*LogEntry, error) {
	w.mu.RLock()
	defer w.mu.RUnlock()

	var entries []*LogEntry

	// 遍历所有可能包含该索引的分片
	for _, seg := range w.listSegments() {
		if seg.EndIndex < fromIndex {
			continue
		}

		file, err := os.Open(seg.FilePath)
		if err != nil {
			return nil, err
		}
		defer file.Close()

		// 内存映射加速读取
		data, err := syscall.Mmap(int(file.Fd()), 0, int(seg.FileSize),
			syscall.PROT_READ, syscall.MAP_SHARED)
		if err != nil {
			return nil, err
		}
		defer syscall.Munmap(data)

		pos := 0
		for pos < len(data) {
			if pos+headerSize > len(data) {
				break
			}

			dataLen := binary.BigEndian.Uint32(data[pos : pos+4])
			index := binary.BigEndian.Uint64(data[pos+4 : pos+12])
			storedCRC := binary.BigEndian.Uint32(data[pos+12 : pos+16])

			if index < fromIndex {
				pos += headerSize + int(dataLen)
				continue
			}

			if pos+headerSize+int(dataLen) > len(data) {
				break
			}

			entryData := data[pos+headerSize : pos+headerSize+int(dataLen)]
			actualCRC := crc32.Checksum(entryData, castagnoli)

			if actualCRC != storedCRC {
				return nil, fmt.Errorf("CRC mismatch at index %d", index)
			}

			entries = append(entries, &LogEntry{
				Index: index,
				Data:  entryData,
				CRC32: actualCRC,
			})

			pos += headerSize + int(dataLen)
		}
	}

	return entries, nil
}

// recoverSegments 恢复现有分片
func (w *WAL) recoverSegments() error {
	// 扫描目录并排序分片文件
	// ...（具体实现参考之前的崩溃恢复设计）

	// 初始化第一个分片
	return w.rotateSegment()
}

// Close 安全关闭
func (w *WAL) Close() error {
	close(w.closeChan)
	return w.activeFile.Close()
}
