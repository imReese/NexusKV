package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"go.uber.org/zap"
)

type BPlusTree struct {
	root   *BPNode
	order  int
	cache  *NodeCache
	config BPConfig
	logger *zap.Logger
	mu     sync.RWMutex
}

type BPNode struct {
	keys   []string
	values [][]byte
	isLeaf bool
}

type NodeCache struct {
	data map[string]*BPNode
	size int
}

func NewBPlusTree(config BPConfig) *BPlusTree {
	return &BPlusTree{
		order:  config.Order,
		config: config,
		logger: zap.NewNop(),
		root: &BPNode{
			isLeaf: true,
		},
		cache: &NodeCache{
			data: make(map[string]*BPNode),
		},
	}
}

func (t *BPlusTree) Put(key string, val []byte) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.root.keys = append(t.root.keys, key)
	t.root.values = append(t.root.values, val)
	t.cache.data[key] = t.root
}

func (t *BPlusTree) Get(key string) ([]byte, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	for i, k := range t.root.keys {
		if k == key {
			return t.root.values[i], true
		}
	}
	return nil, false
}

func (t *BPlusTree) Flush() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if len(t.root.keys) == 0 {
		return nil
	}

	dir := "bptree_idx"
	_ = os.MkdirAll(dir, 0755)
	idxPath := filepath.Join(dir, fmt.Sprintf("bptree_%d.idx", time.Now().UnixNano()))

	f, err := os.Create(idxPath)
	if err != nil {
		return fmt.Errorf("create bptree index failed: %w", err)
	}
	defer f.Close()

	for i, k := range t.root.keys {
		v := t.root.values[i]
		_, _ = fmt.Fprintf(f, "%s:%d\n", k, len(v))
	}
	return f.Sync()
}

func (t *BPlusTree) Close() error {
	return t.Flush()
}
