package storage

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// NexusKVPagedGeometry represents an engine-agnostic token page block geometry.
type NexusKVPagedGeometry struct {
	BlockSize   int `json:"block_size"`   // Tokens per page block (e.g. 16, 32)
	StrideBytes int `json:"stride_bytes"` // Physical bytes per token slot (e.g. 4096, 576)
}

// SyncDeltaDescriptor represents an append-only delta replication update for Decode phase.
type SyncDeltaDescriptor struct {
	DescriptorID     string               `json:"descriptor_id"`
	Geometry         NexusKVPagedGeometry `json:"geometry"`
	AppendedPageSlots []uint64             `json:"appended_page_slots"`
	PhysicalHandles  []uint64             `json:"physical_handles"`
	TimestampNano    int64                `json:"timestamp_nano"`
}

type DescriptorMirror struct {
	mu           sync.RWMutex
	peerAddrs    []string
	localStore   map[string][]byte
	deltaLog     []SyncDeltaDescriptor
	isStandby    bool
	activeTarget string
}

func NewDescriptorMirror(peerAddrs []string, isStandby bool) *DescriptorMirror {
	return &DescriptorMirror{
		peerAddrs:  peerAddrs,
		localStore: make(map[string][]byte),
		deltaLog:   make([]SyncDeltaDescriptor, 0),
		isStandby:  isStandby,
	}
}

func (m *DescriptorMirror) ReplicateDescriptor(ctx context.Context, descID string, payload []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.localStore[descID] = payload
	return nil
}

func (m *DescriptorMirror) ReplicateDelta(ctx context.Context, delta SyncDeltaDescriptor) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	delta.TimestampNano = time.Now().UnixNano()
	m.deltaLog = append(m.deltaLog, delta)
	return nil
}

func (m *DescriptorMirror) GetDescriptor(descID string) ([]byte, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	val, ok := m.localStore[descID]
	return val, ok
}

func (m *DescriptorMirror) GetDeltaCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return len(m.deltaLog)
}

func (m *DescriptorMirror) SetActiveTarget(target string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.activeTarget = target
}

func (m *DescriptorMirror) HealthCheck() string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.isStandby {
		return fmt.Sprintf("STANDBY (Tracking Active: %s, Cached Descriptors: %d, Deltas: %d)", m.activeTarget, len(m.localStore), len(m.deltaLog))
	}
	return fmt.Sprintf("ACTIVE (Peer Count: %d, Cached Descriptors: %d, Deltas: %d)", len(m.peerAddrs), len(m.localStore), len(m.deltaLog))
}
