package storage

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

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
	entryCnt int
}

func NewLSMTree(config LSMConfig) *LSMTree {
	_ = os.MkdirAll(config.SSTableDir, 0755)
	return &LSMTree{
		memtable: &MemTable{
			data: make(map[string][]byte),
		},
		config: config,
		logger: zap.NewNop(),
	}
}

func (t *LSMTree) Put(key string, val []byte) {
	t.mu.Lock()
	defer t.mu.Unlock()

	old, exists := t.memtable.data[key]
	if exists {
		t.memtable.size -= len(old)
	}
	t.memtable.data[key] = val
	t.memtable.size += len(key) + len(val)
}

func (t *LSMTree) Get(key string) ([]byte, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	val, exists := t.memtable.data[key]
	return val, exists
}

func (t *LSMTree) Flush() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if len(t.memtable.data) == 0 {
		return nil
	}

	sstName := fmt.Sprintf("sstable_%d.db", time.Now().UnixNano())
	sstPath := filepath.Join(t.config.SSTableDir, sstName)

	f, err := os.Create(sstPath)
	if err != nil {
		return fmt.Errorf("create sstable failed: %w", err)
	}
	defer f.Close()

	entryCount := 0
	for k, v := range t.memtable.data {
		keyLen := uint32(len(k))
		valLen := uint32(len(v))

		head := make([]byte, 8)
		binary.BigEndian.PutUint32(head[0:4], keyLen)
		binary.BigEndian.PutUint32(head[4:8], valLen)

		if _, err := f.Write(head); err != nil {
			return err
		}
		if _, err := f.Write([]byte(k)); err != nil {
			return err
		}
		if _, err := f.Write(v); err != nil {
			return err
		}
		entryCount++
	}

	if err := f.Sync(); err != nil {
		return fmt.Errorf("sstable fsync failed: %w", err)
	}

	t.sstables = append(t.sstables, &SSTable{
		filepath: sstPath,
		entryCnt: entryCount,
	})

	t.memtable.data = make(map[string][]byte)
	t.memtable.size = 0
	return nil
}

func (t *LSMTree) Close() error {
	return t.Flush()
}
