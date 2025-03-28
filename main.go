package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/exec"

	"gopgdump/internal/dump"
	"gopgdump/internal/restore"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/spf13/cobra"
)

// WorkingTimestamp holds 'base working' timestamp for backup/retain tasks
// remember timestamp for all backups
// it is easy to sort/retain when all backups in one iteration use one timestamp
var (
	connStr   string
	inputPath string
	outputDir string
	pgBinPath string

	// TODO: maxConcur - how many pg_dump may be run at once
	// parallelDatabases int

	// TODO: --jobs parameter for each pg_dump
	// pgDumpJobs        int

)

func checkRequired() error {
	// ensure envs
	for _, requiredEnv := range []string{
		"PGHOST",
		"PGPORT",
		"PGUSER",
		"PGPASSWORD",
	} {
		if os.Getenv(requiredEnv) == "" {
			return fmt.Errorf("required variable not set: %s", requiredEnv)
		}
	}

	// ensure binaries
	for _, requiredBin := range []string{
		"pg_dump",
	} {
		if _, err := exec.LookPath(requiredBin); err != nil {
			return fmt.Errorf("required binary not found: %s", requiredBin)
		}
	}
	return nil
}

func setEnvFromConnStr(connStr string) error {
	cfg, err := pgconn.ParseConfig(connStr)
	if err != nil {
		return fmt.Errorf("failed to parse connStr: %w", err)
	}

	if cfg.Host == "" || cfg.Port == 0 {
		return fmt.Errorf("connStr: host and port are required")
	}

	os.Setenv("PGHOST", cfg.Host)
	os.Setenv("PGPORT", fmt.Sprintf("%d", cfg.Port))

	if cfg.User != "" {
		os.Setenv("PGUSER", cfg.User)
	}
	if cfg.Password != "" {
		os.Setenv("PGPASSWORD", cfg.Password)
	}

	return nil
}

func validateConnStr(ctx context.Context, connStr string) error {
	conn, err := pgx.Connect(ctx, connStr)
	if err != nil {
		return fmt.Errorf("failed to connect to PostgreSQL: %w", err)
	}
	defer conn.Close(ctx)
	if err := conn.Ping(ctx); err != nil {
		return fmt.Errorf("ping failed: %w", err)
	}
	return nil
}

func checkRoutine(ctx context.Context) error {
	if err := setEnvFromConnStr(connStr); err != nil {
		return err
	}
	if err := validateConnStr(ctx, connStr); err != nil {
		return err
	}
	if err := checkRequired(); err != nil {
		return err
	}
	return nil
}

func main() {
	// root

	rootCmd := &cobra.Command{
		Use:   "pgdump-each",
		Short: "PostgreSQL backup and restore utility",
	}

	rootCmd.PersistentFlags().StringVarP(&connStr, "connstr", "c", "", `
PostgreSQL connection string (required)
postgres://user:pass@host:port?sslmode=disable
`)

	rootCmd.PersistentFlags().StringVarP(&pgBinPath, "pgbin-path", "b", "", `
Explicitly specify the path to PostgreSQL binaries (optional)
/usr/lib/postgresql/17/bin
`)

	if err := rootCmd.MarkPersistentFlagRequired("connstr"); err != nil {
		log.Fatal(err)
	}

	// backup

	backupCmd := &cobra.Command{
		Use:   "dump",
		Short: "Dump all databases",
		RunE: func(_ *cobra.Command, _ []string) error {
			ctx := context.Background()
			if err := checkRoutine(ctx); err != nil {
				return err
			}
			return dump.RunDumpJobs(ctx, &dump.ClusterDumpContext{
				ConnStr:   connStr,
				OutputDir: outputDir,
				PgBinPath: pgBinPath,
			})
		},
	}
	backupCmd.Flags().StringVarP(&outputDir, "output", "D", "", "Directory to store backups (required)")
	if err := backupCmd.MarkFlagRequired("output"); err != nil {
		log.Fatal(err)
	}

	// restore

	restoreCmd := &cobra.Command{
		Use:   "restore",
		Short: "Restore all databases from input",
		RunE: func(_ *cobra.Command, _ []string) error {
			ctx := context.Background()
			if err := checkRoutine(ctx); err != nil {
				return err
			}
			return restore.RunRestoreJobs(ctx, &restore.ClusterRestoreContext{
				ConnStr:   connStr,
				InputDir:  inputPath,
				PgBinPath: pgBinPath,
			})
		},
	}
	restoreCmd.Flags().StringVarP(&inputPath, "input", "D", "", "Path to backup directory (required)")
	if err := restoreCmd.MarkFlagRequired("input"); err != nil {
		log.Fatal(err)
	}

	// runner

	rootCmd.AddCommand(backupCmd, restoreCmd)
	if err := rootCmd.Execute(); err != nil {
		log.Fatal(err)
	}
}
