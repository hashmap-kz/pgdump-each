package main

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/spf13/cobra"
)

const (
	TimestampLayout = "20060102150405"

	// TODO: CLI
	clusterName = "local-cluster"
)

// WorkingTimestamp holds 'base working' timestamp for backup/retain tasks
// remember timestamp for all backups
// it is easy to sort/retain when all backups in one iteration use one timestamp
var (
	connStr   string
	inputPath string
	outputDir string

	WorkingTimestamp = time.Now().Truncate(time.Second).Format(TimestampLayout)
)

func getDatabases(ctx context.Context) ([]string, error) {
	conn, err := pgx.Connect(ctx, fmt.Sprintf("postgres://%s:%s/postgres", os.Getenv("PGHOST"), os.Getenv("PGPORT")))
	if err != nil {
		return nil, err
	}
	defer conn.Close(ctx)

	rows, err := conn.Query(ctx, `
	SELECT 	datname FROM pg_database 
	WHERE 	datistemplate = false
	AND 	datname <> 'postgres'
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var databases []string
	for rows.Next() {
		var db string
		if err := rows.Scan(&db); err != nil {
			return nil, err
		}
		databases = append(databases, db)
	}
	if rows.Err() != nil {
		return nil, err
	}
	return databases, nil
}

func dumpCluster(ctx context.Context, stageDir string) error {
	databases, err := getDatabases(ctx)
	if err != nil {
		return nil
	}

	workerCount := 3
	dbChan := make(chan string, len(databases))
	erChan := make(chan error, len(databases))
	var wg sync.WaitGroup

	// Start worker goroutines
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for db := range dbChan {
				dumpErr := dumpDatabase(db, stageDir)
				if dumpErr != nil {
					erChan <- dumpErr
				}
			}
		}()
	}

	// Send databases to the pgDumpWorker channel
	for _, db := range databases {
		dbChan <- db
	}
	close(dbChan) // Close the task channel once all tasks are submitted

	// Wait for all workers to finish
	go func() {
		wg.Wait()
		close(erChan)
	}()

	var lastErr error
	for e := range erChan {
		slog.Error("dump-error", slog.Any("err", e))
		lastErr = e
	}
	return lastErr
}

// dumpDatabase executes pg_dump for a given database.
func dumpDatabase(db, stageDir string) error {
	var err error

	// need in case backup is failed
	tmpDest := filepath.Join(stageDir, db+".dirty")
	// rename to target, if everything is success
	okDest := filepath.Join(stageDir, db+".dmp")
	// prepare directory
	err = os.MkdirAll(tmpDest, 0o755)
	if err != nil {
		return fmt.Errorf("cannot create target dir %s, cause: %w", tmpDest, err)
	}

	// prepare args with optional filters

	args := []string{
		"--dbname=" + db,
		"--file=" + tmpDest + "/data",
		"--format=directory",
		"--jobs=" + fmt.Sprintf("%d", 2),
		"--compress=1",
		"--no-password",
		"--verbose",
		"--verbose", // yes, twice
	}

	// execute dump CMD
	var stderrBuf bytes.Buffer
	cmd := exec.Command("pg_dump", args...)
	cmd.Stderr = &stderrBuf
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("failed to dump %s: %v - %s", db, err, stderrBuf.String())
	}

	// if everything is ok, just rename a temporary dir into the target one
	err = os.Rename(tmpDest, okDest)
	if err != nil {
		return fmt.Errorf("cannot rename %s to %s, cause: %w", tmpDest, okDest, err)
	}

	// save dump logs
	err = os.WriteFile(filepath.Join(okDest, "dump.log"), stderrBuf.Bytes(), 0o600)
	if err != nil {
		slog.Warn("logs", slog.String("err-save-logs", err.Error()))
	}

	slog.Info("backup",
		slog.String("status", "ok"),
		slog.String("path", filepath.ToSlash(okDest)),
	)
	return nil
}

func runDumps(ctx context.Context) error {
	stageDir := filepath.Join(outputDir, fmt.Sprintf("%s-%s.dirty", WorkingTimestamp, clusterName))
	finalDir := filepath.Join(outputDir, fmt.Sprintf("%s-%s.dmp", WorkingTimestamp, clusterName))

	if err := os.MkdirAll(stageDir, 0o755); err != nil {
		return err
	}
	defer os.RemoveAll(stageDir)

	// run jobs
	if err := dumpCluster(ctx, stageDir); err != nil {
		return err
	}

	// ONLY if ALL backups were successfully finished, rename staging to final
	if err := os.Rename(stageDir, finalDir); err != nil {
		return err
	}

	return nil
}

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

	_ = os.Setenv("PGHOST", cfg.Host)
	_ = os.Setenv("PGPORT", fmt.Sprintf("%d", cfg.Port))

	if cfg.User != "" {
		_ = os.Setenv("PGUSER", cfg.User)
	}
	if cfg.Password != "" {
		_ = os.Setenv("PGPASSWORD", cfg.Password)
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

func main() {
	rootCmd := &cobra.Command{
		Use:   "pgdump-each",
		Short: "PostgreSQL backup and restore utility",
	}

	rootCmd.PersistentFlags().StringVar(&connStr, "connstr", "", "PostgreSQL connection string (required)")
	rootCmd.MarkPersistentFlagRequired("connstr")

	backupCmd := &cobra.Command{
		Use:   "backup",
		Short: "Backup all databases",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()

			if err := setEnvFromConnStr(connStr); err != nil {
				return err
			}
			if err := validateConnStr(ctx, connStr); err != nil {
				return err
			}
			if err := checkRequired(); err != nil {
				return err
			}
			return runDumps(ctx)
		},
	}
	backupCmd.Flags().StringVar(&outputDir, "output", "./backups", "Directory to store backups")
	backupCmd.MarkPersistentFlagRequired("output")

	restoreCmd := &cobra.Command{
		Use:   "restore",
		Short: "Restore all databases from input",
		RunE: func(cmd *cobra.Command, args []string) error {
			fmt.Println("Restore not yet implemented")
			return nil
		},
	}
	restoreCmd.Flags().StringVar(&inputPath, "input", "", "Path to backup directory (required)")
	restoreCmd.MarkFlagRequired("input")

	rootCmd.AddCommand(backupCmd, restoreCmd)

	if err := rootCmd.Execute(); err != nil {
		log.Fatal(err)
	}
}
