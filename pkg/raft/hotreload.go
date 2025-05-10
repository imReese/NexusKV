// pkg/raft/hotreload.go
package raft

import (
	"errors"
	"time"

	"github.com/imReese/NexusKV/pkg/config"
	"go.uber.org/zap"
)

type Config struct {
	ElectionTimeout  time.Duration
	HeartbeatTimeout time.Duration
}

type Node interface {
	ApplyConfig(config Config) error
}

type RaftReloadHandler struct {
	node   Node
	logger *zap.Logger
}

func NewRaftReloadHandler(node Node, logger *zap.Logger) *RaftReloadHandler {
	return &RaftReloadHandler{
		node:   node,
		logger: logger,
	}
}

func (h *RaftReloadHandler) OnConfigReload(newCfg *config.ServerConfig) error {
	if newCfg.Raft.ElectionTimeout <= 0 || newCfg.Raft.HeartbeatTimeout <= 0 {
		return errors.New("raft timeouts must be positive values")
	}
	if newCfg.Raft.ElectionTimeout <= newCfg.Raft.HeartbeatTimeout {
		return errors.New("election timeout must be greater than heartbeat timeout")
	}

	h.node.ApplyConfig(Config{
		ElectionTimeout:  newCfg.Raft.ElectionTimeout,
		HeartbeatTimeout: newCfg.Raft.HeartbeatTimeout,
	})
	h.logger.Info("Raft config reloaded",
		zap.Duration("new_election_timeout", newCfg.Raft.ElectionTimeout),
		zap.Duration("new_heartbeat_timeout", newCfg.Raft.HeartbeatTimeout))
	return nil
}
