package storage

import (
	"os"
	"testing"
	"time"
)

func TestHybridEngineLifecycle(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "nexuskv_storage_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	engine := NewHybridEngine(
		WithLSMTreeConfig(LSMConfig{
			MemtableSize:  1024 * 1024,
			SSTableDir:    tempDir,
			MergeInterval: time.Hour,
		}),
	)

	if engine.GetCurrentCacheSize() != 10000 {
		t.Fatalf("expected cache size 10000, got %d", engine.GetCurrentCacheSize())
	}

	_ = engine.UpdateCacheSize(20000)
	if engine.GetCurrentCacheSize() != 20000 {
		t.Fatalf("expected updated cache size 20000, got %d", engine.GetCurrentCacheSize())
	}

	if err := engine.Close(); err != nil {
		t.Fatalf("engine close error: %v", err)
	}
}

func TestLSMTreeFlush(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "nexuskv_lsm_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	lsm := NewLSMTree(LSMConfig{
		SSTableDir: tempDir,
	})

	lsm.Put("prefix_token_001", []byte("sample_payload_bytes"))
	val, ok := lsm.Get("prefix_token_001")
	if !ok || string(val) != "sample_payload_bytes" {
		t.Fatalf("expected payload match")
	}

	if err := lsm.Flush(); err != nil {
		t.Fatalf("lsm flush error: %v", err)
	}
}
