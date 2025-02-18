package main

import (
	"fmt"
	"gopgdump/internal/remote"
	"log/slog"

	"gopgdump/internal/cleaner"

	"gopgdump/internal/retention"

	"gopgdump/config"
	"gopgdump/pkg/logger"

	"gopgdump/internal/backup"
)

func main() {
	cfg := config.LoadConfigFromFile("config.yml")
	slog.SetDefault(logger.InitLogger(cfg.Logger.Format, cfg.Logger.Level))

	// Before concurrent tasks are run
	// 1) remove all '*.dirty' dirs, if any
	// 2) process purge jobs

	err := retention.PurgeOldDirs()
	if err != nil {
		slog.Error("retention", slog.String("err", err.Error()))
	}
	err = cleaner.DropDirtyDirs()
	if err != nil {
		slog.Error("clean", slog.String("err", err.Error()))
	}

	// make backups
	backup.RunPgDumps()
	backup.RunPgBasebackups()

	// sync with remotes, if any
	err = remote.SyncLocalWithRemote()
	if err != nil {
		slog.Error("remote", slog.String("err", err.Error()))
	}

	fmt.Println("All backups completed.")
}
