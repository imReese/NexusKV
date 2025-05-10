package storage

import (
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"
)

type Engine interface {
	GetCurrentCacheSize() int
	GetCurrentFlushInterval() time.Duration
	UpdateCacheSize(size int) error
	UpdateFlushInterval(interval time.Duration) error
	Close() error
}

// 保持结构体名称为HybridEngine
type HybridEngine struct {
	lsmTree       *LSMTree
	bpTree        *BPlusTree
	cacheSize     int
	flushInterval time.Duration
	logger        *zap.Logger
	mu            sync.RWMutex
}

type LSMOption func(*LSMConfig)
type BPTreeOption func(*BPConfig)

type LSMConfig struct {
	MemtableSize    int           // Memtable最大大小(bytes)
	SSTableDir      string        // SSTable存储目录
	MergeInterval   time.Duration // 合并间隔
	BloomFilterBits int           // 布隆过滤器位数
}

type BPConfig struct {
	Order            int // B+树的阶数
	NodeCacheSize    int // 节点缓存大小
	PersistThreshold int // 持久化阈值
}

var (
	DefaultLSMConfig = LSMConfig{
		MemtableSize:    64 << 20, //64MB
		SSTableDir:      "sstables",
		MergeInterval:   1 * time.Hour,
		BloomFilterBits: 10,
	}

	DefaultBPConfig = BPConfig{
		Order:            100,
		NodeCacheSize:    1000,
		PersistThreshold: 10000,
	}
)

func NewHybridEngine(opts ...interface{}) *HybridEngine {
	lsmConfig := DefaultLSMConfig
	bpConfig := DefaultBPConfig

	for _, opt := range opts {
		switch o := opt.(type) {
		case LSMOption:
			o(&lsmConfig)
		case BPTreeOption:
			o(&bpConfig)
		}
	}

	engine := &HybridEngine{
		lsmTree:       NewLSMTree(lsmConfig),
		bpTree:        NewBPlusTree(bpConfig),
		cacheSize:     10000,
		flushInterval: 10 * time.Second,
	}

	go engine.flushLoop()

	return engine
}

func WithLSMTreeConfig(cfg LSMConfig) LSMOption {
	return func(lsm *LSMConfig) {
		*lsm = cfg
	}
}

func WithBPlusTreeConfig(cfg BPConfig) BPTreeOption {
	return func(bp *BPConfig) {
		*bp = cfg
	}
}

func (e *HybridEngine) GetCurrentCacheSize() int {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.cacheSize
}

func (e *HybridEngine) GetCurrentFlushInterval() time.Duration {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.flushInterval
}

func (e *HybridEngine) UpdateCacheSize(size int) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.cacheSize = size
	return nil
}

func (e *HybridEngine) UpdateFlushInterval(interval time.Duration) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.flushInterval = interval
	return nil
}

func (e *HybridEngine) flushLoop() {
	ticker := time.NewTicker(e.flushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			e.mu.Lock()
			if err := e.lsmTree.Flush(); err != nil {
				e.logger.Error("LSM tree flush failed", zap.Error(err))
			}
			if err := e.bpTree.Flush(); err != nil {
				e.logger.Error("B+ tree flush failed", zap.Error(err))
			}
			e.mu.Unlock()
		}
	}
}

func (e *HybridEngine) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	var errs []error
	if err := e.lsmTree.Close(); err != nil {
		errs = append(errs, fmt.Errorf("lsm tree close: %w", err))
	}
	if err := e.bpTree.Close(); err != nil {
		errs = append(errs, fmt.Errorf("b+ tree close: %w", err))
	}
	if len(errs) > 0 {
		return fmt.Errorf("multiple errors: %v", errs)
	}
	return nil
}
