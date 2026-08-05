// pkg/config/config.go
package config

import (
	"os"
	"time"

	"go.uber.org/zap"
	"gopkg.in/yaml.v3"
)

type LogConfig struct {
	RunDir    string `yaml:"run_dir"`
	BackupDir string `yaml:"backup_dir"`
	Level     string `yaml:"level"`
	MaxSize   int    `yaml:"max_size"` // MB
	MaxBackup int    `yaml:"max_backups"`
	MaxAge    int    `yaml:"max_age"` // days
}

type RaftConfig struct {
	ElectionTimeout  time.Duration `yaml:"election_timeout"`
	HeartbeatTimeout time.Duration `yaml:"heartbeat_timeout"`
	SnapshotInterval time.Duration `yaml:"snapshot_interval"`
}

type StorageConfig struct {
	CacheSize     int           `yaml:"cache_size"`
	FlushInterval time.Duration `yaml:"flush_interval"`
	MaxValueSize  int64         `yaml:"max_value_size"`
}

type CacheRouterConfig struct {
	Strategy          string  `yaml:"strategy"`
	LoadPenaltyWeight float64 `yaml:"load_penalty_weight"`
}

type HbmPoolConfig struct {
	TotalCapacityBytes uint64 `yaml:"total_capacity_bytes"`
	BlockSizeBytes     uint64 `yaml:"block_size_bytes"`
}

type ClusterSyncConfig struct {
	SyncIntervalMs int `yaml:"sync_interval_ms"`
	LeaseTtlMs     int `yaml:"lease_ttl_ms"`
}

type ServerConfig struct {
	Port          string            `yaml:"port"`
	DataDir       string            `yaml:"data_dir"`
	EtcdEndpoints []string          `yaml:"etcd_endpoints"`
	Log           LogConfig         `yaml:"log"`
	Raft          RaftConfig        `yaml:"raft"`
	Storage       StorageConfig     `yaml:"storage"`
	CacheRouter   CacheRouterConfig `yaml:"cache_router"`
	HbmPool       HbmPoolConfig     `yaml:"hbm_pool"`
	ClusterSync   ClusterSyncConfig `yaml:"cluster_sync"`
}

func LoadConfig(path string, logger *zap.Logger) (*ServerConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var cfg ServerConfig
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}

	if cfg.Port == "" {
		cfg.Port = "8080"
	}
	if cfg.DataDir == "" {
		cfg.DataDir = "/opt/nexus-kv/data"
	}
	logger.Info("Loaded server config.",
		zap.String("config_path", path),
		zap.Any("config", cfg),
	)
	return &cfg, nil
}
