package main

import (
	"fmt"
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

	dumpsResults := backup.RunPgDumps()
	baseBackupResults := backup.RunPgBasebackups()

	// print results

	for _, r := range dumpsResults {
		server := fmt.Sprintf("%s:%d/%s", r.Host, r.Port, r.Dbname)
		if r.Err != nil {
			slog.Error("pg_dump_result",
				slog.String("server", server),
				slog.Any("err", r.Err),
			)
		} else {
			slog.Info("pg_dump_result",
				slog.String("server", server),
				slog.Any("status", "ok"),
			)
		}
	}
	for _, r := range baseBackupResults {
		server := fmt.Sprintf("%s:%d", r.Host, r.Port)
		if r.Err != nil {
			slog.Error("pg_basebackup_result",
				slog.String("server", server),
				slog.Any("err", r.Err),
			)
		} else {
			slog.Info("pg_basebackup_result",
				slog.String("server", server),
				slog.Any("status", "ok"),
			)
		}
	}

	slog.Info("All backups completed.")
}
