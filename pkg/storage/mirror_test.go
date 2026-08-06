// pkg/storage/mirror_test.go
package storage

import (
	"encoding/json"
	"testing"
)

func TestCacheMirrorEngineLifecycle(t *testing.T) {
	peers := []string{"192.168.1.10:9098", "192.168.1.11:9098"}
	engine := NewCacheMirrorEngine(peers)

	if !engine.GetPeerHealth("192.168.1.10:9098") {
		t.Fatal("expected peer to be healthy")
	}

	packet, err := engine.ReplicateBlock(OpInsertBlock, "tenant_a", "llama_70b", 1001, 4096)
	if err != nil {
		t.Fatalf("unexpected error replicating block: %v", err)
	}

	if engine.GetSyncedBlockCount() != 1 {
		t.Fatalf("expected 1 synced block, got %d", engine.GetSyncedBlockCount())
	}

	data, err := json.Marshal(packet)
	if err != nil {
		t.Fatalf("failed to marshal packet: %v", err)
	}

	backupEngine := NewCacheMirrorEngine(nil)
	if err := backupEngine.ApplyMirrorPacket(data); err != nil {
		t.Fatalf("failed to apply mirror packet: %v", err)
	}

	if backupEngine.GetSyncedBlockCount() != 1 {
		t.Fatalf("expected backup engine to have 1 synced block, got %d", backupEngine.GetSyncedBlockCount())
	}

	// Test Eviction
	evictPacket, err := engine.ReplicateBlock(OpEvictBlock, "tenant_a", "llama_70b", 1001, 0)
	if err != nil {
		t.Fatalf("unexpected error replicating eviction: %v", err)
	}
	evictData, _ := json.Marshal(evictPacket)
	backupEngine.ApplyMirrorPacket(evictData)

	if backupEngine.GetSyncedBlockCount() != 0 {
		t.Fatalf("expected 0 synced blocks after eviction, got %d", backupEngine.GetSyncedBlockCount())
	}
}
