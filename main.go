package main

import (
	"fmt"
	"log/slog"

	"gopgdump/config"
	"gopgdump/pkg/logger"

	"gopgdump/internal/backup"
)

func main() {
	slog.SetDefault(logger.InitLogger("text", "debug"))
	_ = config.LoadConfigFromFile("config.yml")

	// TODO: before concurrent tasks
	// 1) remove all '*.dirty' dirs, if any
	// 2) process purge jobs
	//

	backup.RunBackup()
	fmt.Println("All backups completed.")
}
