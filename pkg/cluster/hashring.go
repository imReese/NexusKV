package cluster

import (
	"fmt"
	"hash/fnv"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
)

type NodeStatus string

const (
	NodeHealthy  NodeStatus = "healthy"
	NodeDegraded NodeStatus = "degraded"
	NodeOffline  NodeStatus = "offline"
)

type MigrationPlan struct {
	SourceNode string
	TargetNode string
	KeyRange   uint32
}

type NodeMetrics struct {
	TotalRequests uint64
	ActiveKeys    uint64
	Status        NodeStatus
}

type ConsistentHashRing struct {
	mu           sync.RWMutex
	virtualNodes int
	ring         []uint32
	nodeMap      map[uint32]string
	nodes        map[string]*NodeMetrics
}

func NewConsistentHashRing(virtualNodes int) *ConsistentHashRing {
	if virtualNodes <= 0 {
		virtualNodes = 100
	}
	return &ConsistentHashRing{
		virtualNodes: virtualNodes,
		nodeMap:      make(map[uint32]string),
		nodes:        make(map[string]*NodeMetrics),
	}
}

func (c *ConsistentHashRing) hash(key string) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return h.Sum32()
}

func (c *ConsistentHashRing) AddNode(node string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if metrics, exists := c.nodes[node]; exists {
		metrics.Status = NodeHealthy
		return
	}
	c.nodes[node] = &NodeMetrics{Status: NodeHealthy}

	for i := 0; i < c.virtualNodes; i++ {
		vKey := node + "#" + strconv.Itoa(i)
		h := c.hash(vKey)
		c.ring = append(c.ring, h)
		c.nodeMap[h] = node
	}
	sort.Slice(c.ring, func(i, j int) bool {
		return c.ring[i] < c.ring[j]
	})
}

func (c *ConsistentHashRing) SetNodeStatus(node string, status NodeStatus) []MigrationPlan {
	c.mu.Lock()
	defer c.mu.Unlock()

	var plans []MigrationPlan
	metrics, exists := c.nodes[node]
	if !exists {
		return plans
	}

	oldStatus := metrics.Status
	metrics.Status = status

	if status == NodeOffline && oldStatus != NodeOffline {
		// Generate failover migration plan for offline node
		for _, vHash := range c.ring {
			if c.nodeMap[vHash] == node {
				nextHealthy := c.findNextHealthyNodeUnlocked(vHash)
				if nextHealthy != "" && nextHealthy != node {
					plans = append(plans, MigrationPlan{
						SourceNode: node,
						TargetNode: nextHealthy,
						KeyRange:   vHash,
					})
				}
			}
		}
	}

	return plans
}

func (c *ConsistentHashRing) RemoveNode(node string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, exists := c.nodes[node]; !exists {
		return
	}
	delete(c.nodes, node)

	newRing := make([]uint32, 0, len(c.ring)-c.virtualNodes)
	for i := 0; i < c.virtualNodes; i++ {
		vKey := node + "#" + strconv.Itoa(i)
		h := c.hash(vKey)
		delete(c.nodeMap, h)
	}

	for _, h := range c.ring {
		if _, exists := c.nodeMap[h]; exists {
			newRing = append(newRing, h)
		}
	}
	c.ring = newRing
}

func (c *ConsistentHashRing) GetNode(key string) (string, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if len(c.ring) == 0 {
		return "", fmt.Errorf("hash ring is empty")
	}

	h := c.hash(key)
	idx := sort.Search(len(c.ring), func(i int) bool {
		return c.ring[i] >= h
	})

	if idx == len(c.ring) {
		idx = 0
	}

	startIdx := idx
	for {
		targetNode := c.nodeMap[c.ring[idx]]
		metrics, ok := c.nodes[targetNode]
		if ok && metrics.Status != NodeOffline {
			atomic.AddUint64(&metrics.TotalRequests, 1)
			return targetNode, nil
		}

		idx = (idx + 1) % len(c.ring)
		if idx == startIdx {
			break
		}
	}

	return "", fmt.Errorf("no healthy worker nodes available in hash ring")
}

func (c *ConsistentHashRing) findNextHealthyNodeUnlocked(startHash uint32) string {
	idx := sort.Search(len(c.ring), func(i int) bool {
		return c.ring[i] > startHash
	})
	if idx == len(c.ring) {
		idx = 0
	}

	startIdx := idx
	for {
		targetNode := c.nodeMap[c.ring[idx]]
		metrics, ok := c.nodes[targetNode]
		if ok && metrics.Status != NodeOffline {
			return targetNode
		}
		idx = (idx + 1) % len(c.ring)
		if idx == startIdx {
			break
		}
	}
	return ""
}

func (c *ConsistentHashRing) GetActiveNodeCount() int {
	c.mu.RLock()
	defer c.mu.RUnlock()

	count := 0
	for _, metrics := range c.nodes {
		if metrics.Status != NodeOffline {
			count++
		}
	}
	return count
}
