package cluster

import (
	"context"
	"fmt"
	"hash/fnv"
	"io"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hashicorp/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health/grpc_health_v1"
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
	nodes        map[string]*NodeMetrics
	ring         []uint32
	nodeMap      map[uint32]string
	virtualNodes int
	stopProber   chan struct{}
	version      uint64
}

func NewConsistentHashRing(virtualNodes int) *ConsistentHashRing {
	if virtualNodes <= 0 {
		virtualNodes = 100
	}
	return &ConsistentHashRing{
		virtualNodes: virtualNodes,
		ring:         make([]uint32, 0),
		nodeMap:      make(map[uint32]string),
		nodes:        make(map[string]*NodeMetrics),
		stopProber:   make(chan struct{}),
	}
}

func (c *ConsistentHashRing) Apply(l *raft.Log) interface{} {
	cmdStr := string(l.Data)
	if len(cmdStr) > 9 && cmdStr[:9] == "ADD_NODE:" {
		nodeAddr := cmdStr[9:]
		c.AddNode(nodeAddr)
		return nil
	}
	if len(cmdStr) > 12 && cmdStr[:12] == "REMOVE_NODE:" {
		nodeAddr := cmdStr[12:]
		c.RemoveNode(nodeAddr)
		return nil
	}
	return fmt.Errorf("unknown command")
}

func (c *ConsistentHashRing) Snapshot() (raft.FSMSnapshot, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	// Clone the current node list for the snapshot
	nodes := make([]string, 0, len(c.nodes))
	for n := range c.nodes {
		nodes = append(nodes, n)
	}

	return &fsmSnapshot{nodes: nodes}, nil
}

func (c *ConsistentHashRing) Restore(rc io.ReadCloser) error {
	defer rc.Close()

	// Simply read all lines and recreate the ring
	data, err := io.ReadAll(rc)
	if err != nil {
		return err
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// Reset state
	c.nodes = make(map[string]*NodeMetrics)
	c.ring = make([]uint32, 0)
	c.nodeMap = make(map[uint32]string)
	c.version++

	if len(data) == 0 {
		return nil
	}

	nodes := strings.Split(string(data), "\n")
	for _, node := range nodes {
		if node == "" {
			continue
		}
		c.nodes[node] = &NodeMetrics{Status: NodeHealthy}
		for i := 0; i < c.virtualNodes; i++ {
			vKey := node + "#" + strconv.Itoa(i)
			h := c.hash(vKey)
			c.ring = append(c.ring, h)
			c.nodeMap[h] = node
		}
	}

	sort.Slice(c.ring, func(i, j int) bool {
		return c.ring[i] < c.ring[j]
	})

	return nil
}

type fsmSnapshot struct {
	nodes []string
}

func (f *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	err := func() error {
		data := strings.Join(f.nodes, "\n")
		if _, err := sink.Write([]byte(data)); err != nil {
			return err
		}
		return sink.Close()
	}()

	if err != nil {
		sink.Cancel()
	}
	return err
}

func (f *fsmSnapshot) Release() {}

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
	c.version++
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
	c.version++
}

func (c *ConsistentHashRing) GetTopologyInfo() (uint64, []string, []uint32, map[uint32]string) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	nodes := make([]string, 0, len(c.nodes))
	for n := range c.nodes {
		nodes = append(nodes, n)
	}

	ringCopy := make([]uint32, len(c.ring))
	copy(ringCopy, c.ring)

	mapCopy := make(map[uint32]string)
	for k, v := range c.nodeMap {
		mapCopy[k] = v
	}

	return c.version, nodes, ringCopy, mapCopy
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

func (c *ConsistentHashRing) StartHealthProber(interval time.Duration) {
	ticker := time.NewTicker(interval)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-c.stopProber:
				return
			case <-ticker.C:
				c.mu.RLock()
				var nodesToCheck []string
				for node := range c.nodes {
					nodesToCheck = append(nodesToCheck, node)
				}
				c.mu.RUnlock()

				for _, nodeAddr := range nodesToCheck {
					go func(addr string) {
						ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
						defer cancel()

						conn, err := grpc.DialContext(ctx, addr, grpc.WithInsecure(), grpc.WithBlock())
						if err != nil {
							c.SetNodeStatus(addr, NodeOffline)
							return
						}
						defer conn.Close()

						client := grpc_health_v1.NewHealthClient(conn)
						resp, err := client.Check(ctx, &grpc_health_v1.HealthCheckRequest{})

						if err != nil || resp.Status != grpc_health_v1.HealthCheckResponse_SERVING {
							c.SetNodeStatus(addr, NodeOffline)
						} else {
							c.SetNodeStatus(addr, NodeHealthy)
						}
					}(nodeAddr)
				}
			}
		}
	}()
}

func (c *ConsistentHashRing) StopHealthProber() {
	select {
	case <-c.stopProber:
	default:
		close(c.stopProber)
	}
}
