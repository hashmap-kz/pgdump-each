package main

import (
	"fmt"
	"gopgdump/internal/retention"
	"log/slog"

	"gopgdump/config"
	"gopgdump/pkg/logger"

	"gopgdump/internal/backup"
)

func main() {
	cfg := config.LoadConfigFromFile("config.yml")
	slog.SetDefault(logger.InitLogger(cfg.Logger.Format, cfg.Logger.Level))

	// TODO: before concurrent tasks
	// 1) remove all '*.dirty' dirs, if any
	// 2) process purge jobs
	//

	err := retention.PurgeOldDirs()
	if err != nil {
		slog.Error("retention", slog.String("err", err.Error()))
	}

	backup.RunBackup()
	fmt.Println("All backups completed.")
}
