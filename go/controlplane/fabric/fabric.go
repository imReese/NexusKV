package fabric

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// Lease represents a metadata lease assigned to a worker/client.
type Lease struct {
	LeaseID   string    `json:"lease_id"`
	Tenant    string    `json:"tenant"`
	Namespace string    `json:"namespace"`
	EntryID   string    `json:"entry_id"`
	HolderID  string    `json:"holder_id"`
	Epoch     uint64    `json:"epoch"`
	ExpiresAt time.Time `json:"expires_at"`
}

func (l *Lease) IsExpired() bool {
	return time.Now().After(l.ExpiresAt)
}

// LeaseManager manages active leases and eviction.
type LeaseManager struct {
	mu     sync.Mutex
	leases map[string]*Lease
}

func NewLeaseManager() *LeaseManager {
	return &LeaseManager{
		leases: make(map[string]*Lease),
	}
}

func (m *LeaseManager) AcquireLease(leaseID, tenant, namespace, entryID, holderID string, ttl time.Duration, epoch uint64) (*Lease, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	existing, ok := m.leases[leaseID]
	if ok && !existing.IsExpired() && existing.HolderID != holderID {
		return nil, fmt.Errorf("lease %s is already held by %s", leaseID, existing.HolderID)
	}

	lease := &Lease{
		LeaseID:   leaseID,
		Tenant:    tenant,
		Namespace: namespace,
		EntryID:   entryID,
		HolderID:  holderID,
		Epoch:     epoch,
		ExpiresAt: time.Now().Add(ttl),
	}
	m.leases[leaseID] = lease
	return lease, nil
}

func (m *LeaseManager) RenewLease(leaseID, holderID string, ttl time.Duration) (*Lease, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	lease, ok := m.leases[leaseID]
	if !ok {
		return nil, errors.New("lease not found")
	}
	if lease.HolderID != holderID {
		return nil, errors.New("holder ID mismatch")
	}
	lease.ExpiresAt = time.Now().Add(ttl)
	return lease, nil
}

func (m *LeaseManager) RevokeLease(leaseID string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	_, ok := m.leases[leaseID]
	if ok {
		delete(m.leases, leaseID)
		return true
	}
	return false
}

func (m *LeaseManager) RevokeLeasesForHolder(holderID string) int {
	m.mu.Lock()
	defer m.mu.Unlock()

	revokedCount := 0
	for id, lease := range m.leases {
		if lease.HolderID == holderID {
			delete(m.leases, id)
			revokedCount++
		}
	}
	return revokedCount
}

// EpochTracker maintains monotonic epochs for global metadata invalidation.
type EpochTracker struct {
	epoch uint64
}

func NewEpochTracker() *EpochTracker {
	return &EpochTracker{epoch: 1}
}

func (e *EpochTracker) CurrentEpoch() uint64 {
	return atomic.LoadUint64(&e.epoch)
}

func (e *EpochTracker) IncrementEpoch() uint64 {
	return atomic.AddUint64(&e.epoch, 1)
}

// GarbageCollector tracks unreferenced entries marked for garbage collection.
type GarbageCollector struct {
	mu            sync.Mutex
	tombstones    map[string]time.Time
	retentionTime time.Duration
}

func NewGarbageCollector(retentionTime time.Duration) *GarbageCollector {
	return &GarbageCollector{
		tombstones:    make(map[string]time.Time),
		retentionTime: retentionTime,
	}
}

func (gc *GarbageCollector) MarkTombstone(entryID string) {
	gc.mu.Lock()
	defer gc.mu.Unlock()
	gc.tombstones[entryID] = time.Now()
}

func (gc *GarbageCollector) CollectGarbage() []string {
	gc.mu.Lock()
	defer gc.mu.Unlock()

	now := time.Now()
	var collected []string
	for entryID, t := range gc.tombstones {
		if now.Sub(t) >= gc.retentionTime {
			collected = append(collected, entryID)
			delete(gc.tombstones, entryID)
		}
	}
	return collected
}
