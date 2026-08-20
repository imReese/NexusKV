package raft

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
	"go.uber.org/zap"
)

type NodeConfig struct {
	ID        string
	RaftAddr  string // TCP address for Raft communication
	DataDir   string // Directory for Raft state (BoltDB + snapshots)
	Bootstrap bool   // True if this node should bootstrap the cluster
	FSM       raft.FSM
	Logger    *zap.Logger
}

type Node struct {
	raft   *raft.Raft
	logger *zap.Logger
	id     string
}

func NewNode(cfg NodeConfig) (*Node, error) {
	if err := os.MkdirAll(cfg.DataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data dir: %v", err)
	}

	raftConfig := raft.DefaultConfig()
	raftConfig.LocalID = raft.ServerID(cfg.ID)

	// Setup Network Transport
	addr, err := net.ResolveTCPAddr("tcp", cfg.RaftAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve tcp addr: %v", err)
	}

	transport, err := raft.NewTCPTransport(cfg.RaftAddr, addr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("failed to create transport: %v", err)
	}

	// Setup BoltDB for LogStore and StableStore
	boltDB, err := raftboltdb.New(raftboltdb.Options{
		Path: filepath.Join(cfg.DataDir, "raft.db"),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create boltdb: %v", err)
	}

	// Setup File Snapshot Store
	snapshotStore, err := raft.NewFileSnapshotStore(cfg.DataDir, 3, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("failed to create snapshot store: %v", err)
	}

	// Create Raft node
	raftNode, err := raft.NewRaft(raftConfig, cfg.FSM, boltDB, boltDB, snapshotStore, transport)
	if err != nil {
		return nil, fmt.Errorf("failed to create raft node: %v", err)
	}

	// Bootstrap cluster if required
	if cfg.Bootstrap {
		configuration := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      raftConfig.LocalID,
					Address: transport.LocalAddr(),
				},
			},
		}
		raftNode.BootstrapCluster(configuration)
	}

	return &Node{
		raft:   raftNode,
		logger: cfg.Logger,
		id:     cfg.ID,
	}, nil
}

func (n *Node) IsLeader() bool {
	return n.raft.State() == raft.Leader
}

func (n *Node) LeaderID() string {
	leaderAddr, leaderID := n.raft.LeaderWithID()
	if leaderID != "" {
		return string(leaderID)
	}
	return string(leaderAddr)
}

func (n *Node) Propose(cmd []byte, timeout time.Duration) error {
	future := n.raft.Apply(cmd, timeout)
	return future.Error()
}

func (n *Node) ApplyConfig(cfg Config) error {
	reloadable := n.raft.ReloadableConfig()
	reloadable.ElectionTimeout = cfg.ElectionTimeout
	reloadable.HeartbeatTimeout = cfg.HeartbeatTimeout
	return n.raft.ReloadConfig(reloadable)
}

func (n *Node) Shutdown(ctx context.Context) error {
	future := n.raft.Shutdown()
	return future.Error()
}
