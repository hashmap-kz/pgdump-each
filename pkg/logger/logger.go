package logger

import (
	"fmt"
	"io"
	"log"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"gopgdump/internal/naming"
)

// For mapping config logger to app logger levels
var loggerLevelMap = map[string]slog.Level{
	"debug": slog.LevelDebug,
	"info":  slog.LevelInfo,
	"warn":  slog.LevelWarn,
	"error": slog.LevelError,
}

func InitLogger(dir, enc, lvl string) *slog.Logger {
	// init logfile
	if dir == "" {
		dir = os.TempDir()
	}
	logFile := filepath.Join(dir, fmt.Sprintf("gopgdump-%s.log", time.Now().Format(naming.TimestampLayout)))
	file, err := os.OpenFile(logFile, os.O_TRUNC|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		log.Fatal(err)
	}
	writer := io.MultiWriter(file, os.Stdout)

	// init logger
	opts := &slog.HandlerOptions{
		Level: getLoggerLevel(lvl),
	}
	var logger *slog.Logger
	if enc == "json" {
		logger = slog.New(slog.NewJSONHandler(writer, opts))
	} else {
		logger = slog.New(slog.NewTextHandler(writer, opts))
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
