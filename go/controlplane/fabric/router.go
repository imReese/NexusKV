package fabric

import (
	"errors"
	"sync"
)

type WorkerNodeMetrics struct {
	NodeID          string            `json:"node_id"`
	Address         string            `json:"address"`
	ActiveTransfers int               `json:"active_transfers"`
	HbmUsageBytes   uint64            `json:"hbm_usage_bytes"`
	CachedPrefixes  map[string]int    `json:"cached_prefixes"` // tenant+ns+model -> max cached token len
}

type CacheAwareClusterRouter struct {
	mu           sync.RWMutex
	workers      map[string]*WorkerNodeMetrics
	leaseManager *LeaseManager
}

func NewCacheAwareClusterRouter(leaseManager *LeaseManager) *CacheAwareClusterRouter {
	return &CacheAwareClusterRouter{
		workers:      make(map[string]*WorkerNodeMetrics),
		leaseManager: leaseManager,
	}
}

func (r *CacheAwareClusterRouter) RegisterWorker(nodeID, address string) *WorkerNodeMetrics {
	r.mu.Lock()
	defer r.mu.Unlock()

	w := &WorkerNodeMetrics{
		NodeID:         nodeID,
		Address:        address,
		CachedPrefixes: make(map[string]int),
	}
	r.workers[nodeID] = w
	return w
}

func (r *CacheAwareClusterRouter) UpdateWorkerCache(nodeID, prefixKey string, cachedLen int) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if w, ok := r.workers[nodeID]; ok {
		w.CachedPrefixes[prefixKey] = cachedLen
	}
}

func (r *CacheAwareClusterRouter) SelectBestWorkerNode(prefixKey string, promptLen int) (*WorkerNodeMetrics, int, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.workers) == 0 {
		return nil, 0, errors.New("no worker nodes registered in router")
	}

	var bestWorker *WorkerNodeMetrics
	bestScore := -1e9
	bestPrefixLen := 0

	for _, w := range r.workers {
		cachedLen := w.CachedPrefixes[prefixKey]
		if cachedLen > promptLen {
			cachedLen = promptLen
		}

		// Calculate score: prefix match gain minus active transfer load penalty
		score := float64(cachedLen)*10.0 - float64(w.ActiveTransfers)*50.0

		if score > bestScore {
			bestScore = score
			bestWorker = w
			bestPrefixLen = cachedLen
		}
	}

	return bestWorker, bestPrefixLen, nil
}
