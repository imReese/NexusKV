// pkg/log/hotreload.go
package log

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/imReese/NexusKV/pkg/config"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
)

type LogReloadHandler struct {
	currentLogger *zap.Logger
}

func NewLogReloadHandler(initialLogger *zap.Logger) *LogReloadHandler {
	return &LogReloadHandler{
		currentLogger: initialLogger,
	}
}

func (h *LogReloadHandler) OnConfigReload(newCfg *config.ServerConfig) error {
	// 配置校验
	if err := validateLogConfig(newCfg.Log); err != nil {
		return fmt.Errorf("invalid log config: %w", err)
	}

	// 原子性操作准备
	oldLogger := h.currentLogger
	newLogger, err := SetupLoggerFromConfig(newCfg.Log)
	if err != nil {
		return err
	}

	// 使用atomic保证指针替换的原子性
	atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(&h.currentLogger)), unsafe.Pointer(newLogger))

	// 异步关闭旧logger
	go func() {
		time.Sleep(1 * time.Second)
		if err := oldLogger.Sync(); err != nil {
			newLogger.Error("Failed to sync old logger", zap.Error(err))
		}
	}()

	return nil
}

func SetupLoggerFromConfig(cfg config.LogConfig) (*zap.Logger, error) {
	if err := os.MkdirAll(cfg.RunDir, 0755); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(cfg.BackupDir, 0755); err != nil {
		return nil, err
	}
	logFile := &lumberjack.Logger{
		Filename:   filepath.Join(cfg.RunDir, "nexuskv.log"),
		MaxSize:    cfg.MaxSize, // MB
		MaxBackups: cfg.MaxBackup,
		MaxAge:     cfg.MaxAge, // days
		Compress:   true,
		LocalTime:  true,
	}
	encoderConfig := zapcore.EncoderConfig{
		TimeKey:        "time",
		LevelKey:       "level",
		NameKey:        "logger",
		CallerKey:      "caller",
		FunctionKey:    zapcore.OmitKey,
		MessageKey:     "msg",
		StacktraceKey:  "stacktrace",
		LineEnding:     zapcore.DefaultLineEnding,
		EncodeLevel:    zapcore.CapitalLevelEncoder,
		EncodeTime:     zapcore.ISO8601TimeEncoder,
		EncodeDuration: zapcore.StringDurationEncoder,
		EncodeCaller:   zapcore.ShortCallerEncoder,
	}

	var level zapcore.Level
	if err := level.UnmarshalText([]byte(cfg.Level)); err != nil {
		level = zapcore.InfoLevel
	}

	core := zapcore.NewCore(
		zapcore.NewJSONEncoder(encoderConfig),
		zapcore.AddSync(logFile),
		level,
	)

	options := []zap.Option{
		zap.AddCaller(),
		zap.AddStacktrace(zapcore.ErrorLevel),
	}
	return zap.New(core, options...), nil
}

func validateLogConfig(cfg config.LogConfig) error {
	if cfg.RunDir == "" || cfg.BackupDir == "" {
		return errors.New("log directories must be specified")
	}

	if cfg.MaxSize <= 0 || cfg.MaxSize > 1024 {
		return errors.New("max_size must be between 1-1024 MB")
	}

	if cfg.MaxBackup < 0 || cfg.MaxBackup > 100 {
		return errors.New("max_backups must be between 0-100")
	}

	var level zapcore.Level
	if err := level.UnmarshalText([]byte(cfg.Level)); err != nil {
		return fmt.Errorf("invalid log level: %w", err)
	}

	return nil
}
