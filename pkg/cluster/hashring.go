package cluster

import (
	"fmt"
	"hash/fnv"
	"sort"
	"strconv"
	"sync"
)

type ConsistentHashRing struct {
	mu           sync.RWMutex
	virtualNodes int
	ring         []uint32
	nodeMap      map[uint32]string
	nodes        map[string]bool
}

func NewConsistentHashRing(virtualNodes int) *ConsistentHashRing {
	if virtualNodes <= 0 {
		virtualNodes = 100
	}
	return &ConsistentHashRing{
		virtualNodes: virtualNodes,
		nodeMap:      make(map[uint32]string),
		nodes:        make(map[string]bool),
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

	if c.nodes[node] {
		return
	}
	c.nodes[node] = true

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

func (c *ConsistentHashRing) RemoveNode(node string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.nodes[node] {
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

	return c.nodeMap[c.ring[idx]], nil
}
