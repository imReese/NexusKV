package storage

import (
	"sync"

	"go.uber.org/zap"
)

type LSMTree struct {
	memtable *MemTable
	sstables []*SSTable
	config   LSMConfig
	logger   *zap.Logger
	mu       sync.RWMutex
}

type MemTable struct {
	data map[string][]byte
	size int
}

type SSTable struct {
	filepath string
}

func NewLSMTree(config LSMConfig) *LSMTree {
	return &LSMTree{
		memtable: &MemTable{
			data: make(map[string][]byte),
		},
		config: config,
	}
}

func (t *LSMTree) Flush() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	// 实现刷盘逻辑
	return nil
}

func (t *LSMTree) Close() error {
	// 实现关闭逻辑
	return nil
}
