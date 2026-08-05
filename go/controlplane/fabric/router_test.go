package fabric

import (
	"testing"
)

func TestCacheAwareClusterRouter(t *testing.T) {
	lm := NewLeaseManager()
	router := NewCacheAwareClusterRouter(lm)

	w1 := router.RegisterWorker("gpu-node-01", "192.168.1.10:8080")
	w2 := router.RegisterWorker("gpu-node-02", "192.168.1.11:8080")

	prefixKey := "t1:ns1:llama-70b:prefix-hash-123"

	router.UpdateWorkerCache("gpu-node-01", prefixKey, 512)
	router.UpdateWorkerCache("gpu-node-02", prefixKey, 8192)

	best, matchedLen, err := router.SelectBestWorkerNode(prefixKey, 8192)
	if err != nil {
		t.Fatalf("routing error: %v", err)
	}

	if best.NodeID != "gpu-node-02" {
		t.Fatalf("expected gpu-node-02 to be selected, got %s", best.NodeID)
	}
	if matchedLen != 8192 {
		t.Fatalf("expected matchedLen 8192, got %d", matchedLen)
	}

	// Test load penalty fallback
	w2.ActiveTransfers = 1000 // Heavily loaded worker
	best, _, err = router.SelectBestWorkerNode(prefixKey, 8192)
	if err != nil {
		t.Fatalf("routing error: %v", err)
	}
	if best.NodeID != "gpu-node-01" {
		t.Fatalf("expected idle worker gpu-node-01 to be selected under load penalty, got %s", best.NodeID)
	}
}
