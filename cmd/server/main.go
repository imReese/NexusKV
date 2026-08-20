// cmd/server/main.go
package main

import (
	"context"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/imReese/NexusKV/pkg/cluster"
	"github.com/imReese/NexusKV/pkg/config"
	"github.com/imReese/NexusKV/pkg/cp"
	"github.com/imReese/NexusKV/pkg/health"
	"github.com/imReese/NexusKV/pkg/log"
	"github.com/imReese/NexusKV/pkg/metrics"
	"github.com/imReese/NexusKV/pkg/raft"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health/grpc_health_v1"
)

const (
	defaultPort     = "9098"
	defaultDataDir  = "/opt/nexus-kv/data"
	shutdownTimeout = 10 * time.Second
	etcdEndpoints   = "localhost:2379"
	serviceName     = "nexus-kv"
	logRunDir       = "/var/log/nexus-kv/run"
	logBackupDir    = "/var/log/nexus-kv/bak"
)

func setupLogger() (*zap.Logger, error) {
	defaultCfg := config.LogConfig{
		RunDir:    logRunDir,
		BackupDir: logBackupDir,
		Level:     "info",
		MaxSize:   100,
		MaxBackup: 30,
		MaxAge:    90,
	}
	return log.SetupLoggerFromConfig(defaultCfg)
}

func initRaftNode(logger *zap.Logger, hashRing *cluster.ConsistentHashRing) (*raft.Node, error) {
	// For testing purpose, we default to bootstrap = true for the single node.
	// In real cluster, we would get this from env or config.
	return raft.NewNode(raft.NodeConfig{
		ID:        "node-1", // hardcoded for test
		RaftAddr:  "127.0.0.1:9099",
		DataDir:   defaultDataDir,
		Bootstrap: true,
		FSM:       hashRing,
		Logger:    logger,
	})
}

func initGRPCServer(raftNode *raft.Node, hashRing *cluster.ConsistentHashRing) *grpc.Server {
	server := grpc.NewServer(
		grpc.MaxRecvMsgSize(1024*1024*10), // 10MB
		grpc.ConnectionTimeout(30*time.Second),
	)

	// 注册控制面服务
	cpServer := cp.NewControlPlaneServiceServer(raftNode, hashRing)
	cp.RegisterControlPlaneServiceServer(server, cpServer)

	// 注册健康检查服务
	healthServer := health.NewHealthServer()
	grpc_health_v1.RegisterHealthServer(server, healthServer)

	// 注册指标服务
	metrics.Init()

	return server
}

func startServer(grpcServer *grpc.Server, lis net.Listener, logger *zap.Logger) {
	go func() {
		logger.Info("Starting gRPC server",
			zap.String("address", lis.Addr().String()))

		if err := grpcServer.Serve(lis); err != nil {
			logger.Error("gRPC server failed",
				zap.Error(err))
		}
	}()
}

func initConfigWatcher(logger *zap.Logger, raftNode *raft.Node) *config.ConfigWatcher {
	watcher := config.NewConfigWatcher("config/nexus-kv.yaml", logger, 5*time.Second)
	config.RegisterReloadHandler(log.NewLogReloadHandler(logger))
	config.RegisterReloadHandler(raft.NewRaftReloadHandler(raftNode, logger))
	return watcher
}

func waitForShutdown(grpcServer *grpc.Server, raftNode *raft.Node, logger *zap.Logger) {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	logger.Info("Shutting down server...")

	ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()

	stopped := make(chan struct{})
	go func() {
		grpcServer.GracefulStop()
		close(stopped)
	}()

	select {
	case <-stopped:
		logger.Info("gRPC server stopped gracefully")
	case <-ctx.Done():
		logger.Warn("Forcing gRPC server shutdown")
		grpcServer.Stop()
	}

	if err := raftNode.Shutdown(ctx); err != nil {
		logger.Error("Error shutting down Raft node", zap.Error(err))
	}

	logger.Info("Server shutdown complete")
}

func main() {
	logger, err := setupLogger()
	if err != nil {
		panic(err)
	}
	defer logger.Sync()

	cfg, err := config.LoadConfig("config/nexus-kv.yaml", logger)
	if err != nil {
		logger.Fatal("Failed to load config", zap.Error(err))
	}

	logger, err = log.SetupLoggerFromConfig(cfg.Log)
	defer logger.Sync()

	if err := os.MkdirAll(defaultDataDir, 0755); err != nil {
		logger.Fatal("Failed to create data directory", zap.Error(err))
	}

	hashRing := cluster.NewConsistentHashRing(100)
	hashRing.StartHealthProber(10 * time.Second)
	defer hashRing.StopHealthProber()

	raftNode, err := initRaftNode(logger, hashRing)
	if err != nil {
		logger.Fatal("Failed to init raft", zap.Error(err))
	}

	grpcServer := initGRPCServer(raftNode, hashRing)
	lis, err := net.Listen("tcp", ":"+defaultPort)
	if err != nil {
		logger.Fatal("Failed to listen", zap.Error(err))
	}

	startServer(grpcServer, lis, logger)
	watcher := initConfigWatcher(logger, raftNode)
	defer watcher.Stop()
	watcher.Start()

	logger.Info("Server started successfully",
		zap.String("version", "1.0.0"),
		zap.String("port", defaultPort),
		zap.String("data_dir", defaultDataDir),
		zap.Strings("etcd_endpoints", []string{etcdEndpoints}),
	)

	waitForShutdown(grpcServer, raftNode, logger)
}
