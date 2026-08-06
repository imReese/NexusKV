// pkg/storage/mirror.go
package storage

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

type MirrorOpType string

const (
	OpInsertBlock MirrorOpType = "INSERT"
	OpEvictBlock  MirrorOpType = "EVICT"
	OpUpdateTTL   MirrorOpType = "UPDATE_TTL"
)

type MirrorPacket struct {
	SequenceID uint64       `json:"sequence_id"`
	OpType     MirrorOpType `json:"op_type"`
	TenantID   string       `json:"tenant_id"`
	ModelName  string       `json:"model_name"`
	BlockID    uint64       `json:"block_id"`
	Size       uint64       `json:"size"`
	Timestamp  int64        `json:"timestamp"`
}

type PeerHealthStatus struct {
	PeerAddr  string    `json:"peer_addr"`
	IsHealthy bool      `json:"is_healthy"`
	LastSeen  time.Time `json:"last_seen"`
}

type CacheMirrorEngine struct {
	mu           sync.RWMutex
	sequence     uint64
	peers        map[string]*PeerHealthStatus
	syncedBlocks map[uint64]*MirrorPacket
}

func NewCacheMirrorEngine(peerAddrs []string) *CacheMirrorEngine {
	engine := &CacheMirrorEngine{
		peers:        make(map[string]*PeerHealthStatus),
		syncedBlocks: make(map[uint64]*MirrorPacket),
	}
	for _, addr := range peerAddrs {
		engine.peers[addr] = &PeerHealthStatus{
			PeerAddr:  addr,
			IsHealthy: true,
			LastSeen:  time.Now(),
		}
	}
	return engine
}

func (e *CacheMirrorEngine) ReplicateBlock(op MirrorOpType, tenant, model string, blockID, size uint64) (*MirrorPacket, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.sequence++
	packet := &MirrorPacket{
		SequenceID: e.sequence,
		OpType:     op,
		TenantID:   tenant,
		ModelName:  model,
		BlockID:    blockID,
		Size:       size,
		Timestamp:  time.Now().UnixNano(),
	}

	if op == OpInsertBlock {
		e.syncedBlocks[blockID] = packet
	} else if op == OpEvictBlock {
		delete(e.syncedBlocks, blockID)
	}

	return packet, nil
}

func (e *CacheMirrorEngine) ApplyMirrorPacket(data []byte) error {
	var packet MirrorPacket
	if err := json.Unmarshal(data, &packet); err != nil {
		return fmt.Errorf("failed to unmarshal mirror packet: %w", err)
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	if packet.OpType == OpInsertBlock {
		e.syncedBlocks[packet.BlockID] = &packet
	} else if packet.OpType == OpEvictBlock {
		delete(e.syncedBlocks, packet.BlockID)
	}

	return nil
}

func (e *CacheMirrorEngine) GetSyncedBlockCount() int {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return len(e.syncedBlocks)
}

func (e *CacheMirrorEngine) UpdatePeerHeartbeat(peerAddr string, isHealthy bool) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if status, exists := e.peers[peerAddr]; exists {
		status.IsHealthy = isHealthy
		status.LastSeen = time.Now()
	} else {
		e.peers[peerAddr] = &PeerHealthStatus{
			PeerAddr:  peerAddr,
			IsHealthy: isHealthy,
			LastSeen:  time.Now(),
		}
	}
}

func (e *CacheMirrorEngine) GetPeerHealth(peerAddr string) bool {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if status, exists := e.peers[peerAddr]; exists {
		return status.IsHealthy
	}
	return false
}
