package cluster

import (
	"testing"
)

func TestConsistentHashRingLifecycle(t *testing.T) {
	ring := NewConsistentHashRing(50)

	ring.AddNode("worker-1")
	ring.AddNode("worker-2")
	ring.AddNode("worker-3")

	node, err := ring.GetNode("token_sequence_prefix_1001")
	if err != nil {
		t.Fatalf("expected node, got error: %v", err)
	}

	if node == "" {
		t.Fatalf("expected valid node name, got empty string")
	}

	ring.RemoveNode("worker-1")
	node2, err := ring.GetNode("token_sequence_prefix_1001")
	if err != nil {
		t.Fatalf("expected node after removal, got error: %v", err)
	}

	if node2 == "" {
		t.Fatalf("expected valid node name after removal, got empty string")
	}
}
