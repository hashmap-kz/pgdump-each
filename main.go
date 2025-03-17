package main

import (
	"fmt"
	"log/slog"

	"gopgdump/internal/notifier"

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

	// print results, send messages
	n := notifier.NewTgNotifier()
	var results []*backup.ResultInfo
	results = append(results, dumpsResults...)
	results = append(results, baseBackupResults...)

	for _, r := range results {
		server := fmt.Sprintf("%s:%d/%s", r.Host, r.Port, r.Dbname)
		if r.Dbname == "" {
			server = fmt.Sprintf("%s:%d", r.Host, r.Port)
		}
		if r.Err != nil {
			slog.Error(r.Mode+"_result",
				slog.String("server", server),
				slog.Any("err", r.Err),
			)
			n.SendMessage(&notifier.AlertRequest{
				Status:  notifier.NotifyStatusError,
				Message: fmt.Sprintf("%s failed!\nserver: %s.\nerror: %s", r.Mode, server, err.Error()),
			})
		} else {
			slog.Info(r.Mode+"_result",
				slog.Any("status", "ok"),
				slog.String("server", server),
			)
			n.SendMessage(&notifier.AlertRequest{
				Status:  notifier.NotifyStatusInfo,
				Message: fmt.Sprintf("%s success!\nserver: %s", r.Mode, server),
			})
		}
	}

	slog.Info("All backups completed.")
}
