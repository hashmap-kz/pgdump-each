package main

import (
	"flag"
	"fmt"
	"log"
	"log/slog"
	"os"

	"gopgdump/internal/notifier"

	"gopgdump/internal/cleaner"

	"gopgdump/internal/retention"

	"gopgdump/config"
	"gopgdump/pkg/logger"

	"gopgdump/internal/backup"
)

// TODO: add to linker opts with a tag name
var Version = "1.0.5"

func main() {
	// parse cmd args
	var configPath string
	flag.StringVar(&configPath, "config", "", "Example: -config /etc/gopgdump/config.yml")
	showVersion := flag.Bool("v", false, "Print version and exit")
	flag.Parse()

	if *showVersion {
		fmt.Println(Version)
		os.Exit(0)
	}

	if configPath == "" {
		flag.Usage()
		log.Fatal("config-path not provided")
	}

	cfg := config.LoadConfigFromFile(configPath)
	slog.SetDefault(logger.InitLogger(cfg.LogDir, cfg.Logger.Format, cfg.Logger.Level))

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

	var errors bool
	for _, r := range results {
		server := fmt.Sprintf("%s:%d/%s", r.Host, r.Port, r.Dbname)
		if r.Dbname == "" {
			server = fmt.Sprintf("%s:%d", r.Host, r.Port)
		}
		if r.Err != nil {
			errors = true
			slog.Error(r.Mode+"_result",
				slog.String("server", server),
				slog.Any("err", r.Err),
			)
			n.SendMessage(&notifier.AlertRequest{
				Status:  notifier.NotifyStatusError,
				Message: fmt.Sprintf("%s failed!\nserver: %s.\nerror: %s", r.Mode, server, r.Err.Error()),
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

	if errors {
		slog.Info("All backups completed. With errors. See logs for details.")
	} else {
		slog.Info("All backups completed. No errors")
	}
}
