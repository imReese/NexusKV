// pkg/config/watcher.go
package config

import (
	"os"
	"reflect"
	"sync"
	"time"

	"go.uber.org/zap"
)

type HotReloadHandler interface {
	OnConfigReload(newCfg *ServerConfig) error
}

type ConfigWatcher struct {
	logger     *zap.Logger
	configPath string
	lastMod    time.Time
	interval   time.Duration
	stopCh     chan struct{}
}

var (
	handlers []HotReloadHandler
	mu       sync.Mutex
)

func NewConfigWatcher(configPath string, logger *zap.Logger, interval time.Duration) *ConfigWatcher {
	return &ConfigWatcher{
		configPath: configPath,
		logger:     logger,
		interval:   interval,
		stopCh:     make(chan struct{}),
	}
}

func (w *ConfigWatcher) checkModified() bool {
	info, err := os.Stat(w.configPath)
	if err != nil {
		return false
	}
	if info.ModTime().After(w.lastMod) {
		w.lastMod = info.ModTime()
		return true
	}
	return false
}

func (w *ConfigWatcher) watchLoop() {
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()
	for {
		select {
		case <-w.stopCh:
			w.logger.Info("Config watcher stopped")
			return
		case <-ticker.C:
			if w.checkModified() {
				cfg, err := LoadConfig(w.configPath, w.logger)
				if err != nil {
					w.logger.Error("Reload config failed", zap.Error(err))
					continue
				}
				w.handleReload(cfg)
			}
		}
	}
}

func (w *ConfigWatcher) Start() {
	go w.watchLoop()
}

func (w *ConfigWatcher) Stop() {
	close(w.stopCh)
}

func RegisterReloadHandler(h HotReloadHandler) {
	mu.Lock()
	defer mu.Unlock()

	handlers = append(handlers, h)
}

func (w *ConfigWatcher) handleReload(newCfg *ServerConfig) {
	mu.Lock()
	handlersCopy := make([]HotReloadHandler, len(handlers))
	copy(handlersCopy, handlers)
	mu.Unlock()

	w.logger.Info("Config file reloaded",
		zap.Any("new_config", newCfg))

	for _, h := range handlersCopy {
		if err := h.OnConfigReload(newCfg); err != nil {
			w.logger.Error("Reload handler failed",
				zap.Error(err),
				zap.String("handler", reflect.TypeOf(h).String()))
		}
	}
}
