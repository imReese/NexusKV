package fabric

import (
	"fmt"
	"sync"
	"time"
)

type NodeStatus string

const (
	NodeStatusOnline  NodeStatus = "ONLINE"
	NodeStatusSuspect NodeStatus = "SUSPECT"
	NodeStatusOffline NodeStatus = "OFFLINE"
)

type ClusterNode struct {
	NodeID        string            `json:"node_id"`
	Address       string            `json:"address"`
	Role          string            `json:"role"`
	Status        NodeStatus        `json:"status"`
	LastHeartbeat time.Time         `json:"last_heartbeat"`
	Metadata      map[string]string `json:"metadata"`
}

type NodeDiscoveryService struct {
	mu           sync.RWMutex
	nodes        map[string]*ClusterNode
	leaseManager *LeaseManager
}

func NewNodeDiscoveryService(leaseManager *LeaseManager) *NodeDiscoveryService {
	return &NodeDiscoveryService{
		nodes:        make(map[string]*ClusterNode),
		leaseManager: leaseManager,
	}
}

func (s *NodeDiscoveryService) RegisterNode(nodeID, address, role string, metadata map[string]string) *ClusterNode {
	s.mu.Lock()
	defer s.mu.Unlock()

	if metadata == nil {
		metadata = make(map[string]string)
	}

	node := &ClusterNode{
		NodeID:        nodeID,
		Address:       address,
		Role:          role,
		Status:        NodeStatusOnline,
		LastHeartbeat: time.Now(),
		Metadata:      metadata,
	}

	s.nodes[nodeID] = node
	return node
}

func (s *NodeDiscoveryService) PulseHeartbeat(nodeID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	node, ok := s.nodes[nodeID]
	if !ok {
		return fmt.Errorf("node %s not registered", nodeID)
	}

	node.LastHeartbeat = time.Now()
	node.Status = NodeStatusOnline
	return nil
}

func (s *NodeDiscoveryService) SweepTimedOutNodes(heartbeatTimeout time.Duration) ([]string, int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	var timedOutNodes []string
	var revokedLeases int

	for id, node := range s.nodes {
		if node.Status == NodeStatusOnline && now.Sub(node.LastHeartbeat) > heartbeatTimeout {
			node.Status = NodeStatusOffline
			timedOutNodes = append(timedOutNodes, id)

			if s.leaseManager != nil {
				revokedLeases += s.leaseManager.RevokeLeasesForHolder(id)
			}
		}
	}

	return timedOutNodes, revokedLeases
}

func (s *NodeDiscoveryService) GetOnlineNodes() []*ClusterNode {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var result []*ClusterNode
	for _, n := range s.nodes {
		if n.Status == NodeStatusOnline {
			result = append(result, n)
		}
	}
	return result
}
