package raft

import (
	"context"

	"google.golang.org/grpc"
)

type EtcdConfig struct {
	Endpoints []string
}

type Transport struct{}

type Node struct {
	config Config
}

type NodeConfig struct {
	Storage    interface{}
	WAL        interface{}
	Transport  *Transport
	EtcdConfig EtcdConfig
}

func NewGRPCTransport() *Transport {
	return &Transport{}
}

func NewNode(cfg NodeConfig) (*Node, error) {
	return &Node{
		config: Config{},
	}, nil
}

func (n *Node) ApplyConfig(cfg Config) error {
	n.config = cfg
	return nil
}

func (n *Node) Shutdown(ctx context.Context) error {
	return nil
}

func RegisterRaftServiceServer(server grpc.ServiceRegistrar, node *Node) {
}
