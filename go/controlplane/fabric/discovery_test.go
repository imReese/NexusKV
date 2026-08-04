package fabric

import (
	"testing"
	"time"
)

func TestNodeDiscoveryAndHeartbeatTimeout(t *testing.T) {
	lm := NewLeaseManager()
	_, err := lm.AcquireLease("lease-1", "t1", "ns1", "e1", "worker-node-1", 10*time.Minute, 1)
	if err != nil {
		t.Fatalf("failed to acquire lease: %v", err)
	}

	ds := NewNodeDiscoveryService(lm)
	node := ds.RegisterNode("worker-node-1", "192.168.1.10:8080", "worker", map[string]string{"gpu": "h100"})
	if node.Status != NodeStatusOnline {
		t.Fatalf("expected node status ONLINE, got %s", node.Status)
	}

	onlineNodes := ds.GetOnlineNodes()
	if len(onlineNodes) != 1 {
		t.Fatalf("expected 1 online node, got %d", len(onlineNodes))
	}

	// Pulse heartbeat
	err = ds.PulseHeartbeat("worker-node-1")
	if err != nil {
		t.Fatalf("failed to pulse heartbeat: %v", err)
	}

	// Fast forward heartbeat timeout check with 0s timeout
	timedOut, revoked := ds.SweepTimedOutNodes(-1 * time.Second)
	if len(timedOut) != 1 || timedOut[0] != "worker-node-1" {
		t.Fatalf("expected worker-node-1 timed out, got %v", timedOut)
	}
	if revoked != 1 {
		t.Fatalf("expected 1 lease revoked on node timeout, got %d", revoked)
	}

	onlineNodes = ds.GetOnlineNodes()
	if len(onlineNodes) != 0 {
		t.Fatalf("expected 0 online nodes after timeout, got %d", len(onlineNodes))
	}
}
