package cp

import (
	"context"
	"fmt"
	"time"

	hc_raft "github.com/hashicorp/raft"
	"github.com/imReese/NexusKV/pkg/cluster"
	"github.com/imReese/NexusKV/pkg/raft"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type ControlPlaneServiceServerImpl struct {
	UnimplementedControlPlaneServiceServer
	raftNode *raft.Node
	hashRing *cluster.ConsistentHashRing
}

func NewControlPlaneServiceServer(raftNode *raft.Node, hashRing *cluster.ConsistentHashRing) *ControlPlaneServiceServerImpl {
	return &ControlPlaneServiceServerImpl{
		raftNode: raftNode,
		hashRing: hashRing,
	}
}

func (s *ControlPlaneServiceServerImpl) RegisterNode(ctx context.Context, req *RegisterNodeRequest) (*RegisterNodeResponse, error) {
	if !s.raftNode.IsLeader() {
		return &RegisterNodeResponse{
			Success:  false,
			LeaderId: s.raftNode.LeaderID(),
		}, nil
	}

	cmd := fmt.Sprintf("ADD_NODE:%s", req.NodeAddr)

	err := s.raftNode.Propose([]byte(cmd), 2*time.Second)
	if err != nil {
		if err == hc_raft.ErrNotLeader {
			// If it lost leadership during propose
			return &RegisterNodeResponse{
				Success:  false,
				LeaderId: s.raftNode.LeaderID(),
			}, nil
		}
		return nil, status.Errorf(codes.Internal, "Failed to propose node registration: %v", err)
	}

	return &RegisterNodeResponse{
		Success: true,
	}, nil
}

func (s *ControlPlaneServiceServerImpl) GetTopology(ctx context.Context, req *GetTopologyRequest) (*GetTopologyResponse, error) {
	version, nodes, ring, nodeMap := s.hashRing.GetTopologyInfo()

	if version == req.CurrentVersion {
		// Client already has the latest version, no need to send the payload again.
		return &GetTopologyResponse{
			Version: version,
		}, nil
	}

	return &GetTopologyResponse{
		Version: version,
		Nodes:   nodes,
		Ring:    ring,
		NodeMap: nodeMap,
	}, nil
}
