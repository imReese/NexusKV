package storage

import (
	"context"
	"testing"
)

func TestDescriptorMirrorReplication(t *testing.T) {
	peers := []string{"localhost:9098", "localhost:90981"}
	mirror := NewDescriptorMirror(peers, false)

	ctx := context.Background()
	payload := []byte("test-descriptor-data")
	err := mirror.ReplicateDescriptor(ctx, "desc-1", payload)
	if err != nil {
		t.Fatalf("failed to replicate descriptor: %v", err)
	}

	data, ok := mirror.GetDescriptor("desc-1")
	if !ok || string(data) != "test-descriptor-data" {
		t.Fatalf("descriptor payload mismatch: %s", string(data))
	}

	health := mirror.HealthCheck()
	if health == "" {
		t.Fatalf("health check returned empty string")
	}
}

func TestEngineAgnosticDeltaReplication(t *testing.T) {
	peers := []string{"localhost:9098"}
	mirror := NewDescriptorMirror(peers, false)

	ctx := context.Background()
	delta := SyncDeltaDescriptor{
		DescriptorID: "desc-delta-1",
		Geometry: NexusKVPagedGeometry{
			BlockSize:   16,
			StrideBytes: 4096,
		},
		AppendedPageSlots: []uint64{10, 11},
		PhysicalHandles:   []uint64{0x1000, 0x2000},
	}

	err := mirror.ReplicateDelta(ctx, delta)
	if err != nil {
		t.Fatalf("failed to replicate delta: %v", err)
	}

	if mirror.GetDeltaCount() != 1 {
		t.Fatalf("expected 1 delta log entry, got %d", mirror.GetDeltaCount())
	}
}
