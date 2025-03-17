package logger

import (
	"io"
	"log/slog"
	"os"
)

// For mapping config logger to app logger levels
var loggerLevelMap = map[string]slog.Level{
	"debug": slog.LevelDebug,
	"info":  slog.LevelInfo,
	"warn":  slog.LevelWarn,
	"error": slog.LevelError,
}

func InitLogger(enc, lvl string) *slog.Logger {
	opts := &slog.HandlerOptions{
		Level: getLoggerLevel(lvl),
	}
	var logger *slog.Logger
	if enc == "json" {
		logger = slog.New(slog.NewJSONHandler(os.Stdout, opts))
	} else {
		logger = slog.New(slog.NewTextHandler(os.Stdout, opts))
	}
	return logger
}

func getLoggerLevel(lvl string) slog.Level {
	level, exist := loggerLevelMap[lvl]
	if !exist {
		return slog.LevelDebug
	}
	return level
}

// DisableLogging temporarily disables slog output
// Usage:
//
// originalLogger := DisableLogging()
// defer RestoreLogging(originalLogger)
func DisableLogging() *slog.Logger {
	originalLogger := slog.Default()
	// Suppress logs
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))
	return originalLogger
}

// RestoreLogging restores the original logger
func RestoreLogging(originalLogger *slog.Logger) {
	slog.SetDefault(originalLogger)
}
