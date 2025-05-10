// pkg/storage/hotreload.go
package storage

import (
	"errors"
	"fmt"
	"sync"

	"github.com/imReese/NexusKV/pkg/config"
	"go.uber.org/zap"
)

type StorageReloadHandler struct {
	engine Engine
	logger *zap.Logger
	mu     sync.Mutex
}

func NewStorageReloadHandler(engine Engine, logger *zap.Logger) *StorageReloadHandler {
	return &StorageReloadHandler{
		engine: engine,
		logger: logger,
	}
}

func (h *StorageReloadHandler) OnConfigReload(newCfg *config.ServerConfig) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	oldCacheSize := h.engine.GetCurrentCacheSize()
	oldInterval := h.engine.GetCurrentFlushInterval()

	if newCfg.Storage.CacheSize <= 0 {
		return errors.New("cache size must be positive")
	}
	if newCfg.Storage.FlushInterval <= 0 {
		return errors.New("flush interval must be positive")
	}
	h.logger.Info("Applying storage config changes",
		zap.Int("old_cache_size", h.engine.GetCurrentCacheSize()),
		zap.Duration("old_flush_interval", h.engine.GetCurrentFlushInterval()),
	)
	if err := h.engine.UpdateCacheSize(newCfg.Storage.CacheSize); err != nil {
		_ = h.engine.UpdateCacheSize(oldCacheSize)
		return fmt.Errorf("failed to update cache size: %w", err)
	}
	if err := h.engine.UpdateFlushInterval(newCfg.Storage.FlushInterval); err != nil {
		_ = h.engine.UpdateFlushInterval(oldInterval)
		return fmt.Errorf("failed to update flush interval: %w", err)
	}
	h.logger.Info("Storage config reloaded",
		zap.Int("new_cache_size", newCfg.Storage.CacheSize),
		zap.Duration("new_flush_interval", newCfg.Storage.FlushInterval))
	return nil
}
