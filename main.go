package main

import (
	"fmt"
	"log"
	"log/slog"

	"gopgdump/internal/local"

	"gopgdump/internal/remote"

	"gopgdump/internal/cleaner"

	"gopgdump/internal/retention"

	"gopgdump/config"
	"gopgdump/pkg/logger"

	"gopgdump/internal/backup"

	_ "github.com/jackc/pgx/v5"
)

func main() {
	cfg := config.LoadConfigFromFile("config.yml")
	slog.SetDefault(logger.InitLogger(cfg.Logger.Format, cfg.Logger.Level))

	// TODO:
	_, err2 := local.FindAllBackupsV2()
	if err2 != nil {
		log.Fatal(err2)
	}

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
