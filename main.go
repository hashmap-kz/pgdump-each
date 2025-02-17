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

	// Define your databases here
	databases := []config.BackupConfig{
		{
			Host:     "localhost",
			Port:     "5432",
			Username: "postgres",
			Password: "postgres",
			Dbname:   "bookstore",
			Opts: map[string]string{
				"connect_timeout": "5",
				"sslmode":         "disable",
			},
			Schemas: []string{
				"public",
			},
		},
		{
			Host:     "10.40.240.189",
			Port:     "5432",
			Username: "postgres",
			Password: "postgres",
			Dbname:   "keycloak_base",
		},
		{
			Host:     "10.40.240.165",
			Port:     "30201",
			Username: "postgres",
			Password: "postgres",
			Dbname:   "vault",
		},
	}

	// TODO: before concurrent tasks
	// 1) remove all '*.dirty' dirs, if any
	// 2) process purge jobs
	//

	backup.RunBackup(databases)
	fmt.Println("All backups completed.")
}
