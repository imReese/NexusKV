package fabric

import (
	"testing"
	"time"
)

func TestLeaseManagerAcquireRenewRevoke(t *testing.T) {
	lm := NewLeaseManager()

	lease, err := lm.AcquireLease("lease-1", "tenant-a", "ns1", "entry-1", "worker-1", 50*time.Millisecond, 1)
	if err != nil {
		t.Fatalf("failed to acquire lease: %v", err)
	}
	if lease.IsExpired() {
		t.Fatalf("new lease should not be expired")
	}

	// Conflict acquire by another worker should fail
	_, err = lm.AcquireLease("lease-1", "tenant-a", "ns1", "entry-1", "worker-2", 50*time.Millisecond, 1)
	if err == nil {
		t.Fatalf("expected error acquiring unexpired lease from another worker")
	}

	// Renew lease
	renewed, err := lm.RenewLease("lease-1", "worker-1", 100*time.Millisecond)
	if err != nil {
		t.Fatalf("failed to renew lease: %v", err)
	}
	if renewed.HolderID != "worker-1" {
		t.Fatalf("holder ID mismatch: got %s", renewed.HolderID)
	}

	// Revoke lease
	revoked := lm.RevokeLease("lease-1")
	if !revoked {
		t.Fatalf("expected lease revocation to succeed")
	}
}

func TestEpochTracker(t *testing.T) {
	et := NewEpochTracker()
	if et.CurrentEpoch() != 1 {
		t.Fatalf("initial epoch should be 1, got %d", et.CurrentEpoch())
	}
	next := et.IncrementEpoch()
	if next != 2 {
		t.Fatalf("incremented epoch should be 2, got %d", next)
	}
}

func TestGarbageCollector(t *testing.T) {
	gc := NewGarbageCollector(20 * time.Millisecond)
	gc.MarkTombstone("entry-old")

	// Immediate collect should return nothing
	collected := gc.CollectGarbage()
	if len(collected) != 0 {
		t.Fatalf("expected 0 collected entries, got %d", len(collected))
	}

	time.Sleep(30 * time.Millisecond)
	collected = gc.CollectGarbage()
	if len(collected) != 1 || collected[0] != "entry-old" {
		t.Fatalf("expected entry-old collected, got %v", collected)
	}
}
