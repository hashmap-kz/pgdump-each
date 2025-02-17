package backup

import (
	"bytes"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"sync"

	"gopgdump/internal/util"

	"gopgdump/config"
	"gopgdump/internal/ts"
)

func RunPgBasebackups() {
	cfg := config.Cfg()
	databases := cfg.Base.DBS

	// Number of concurrent workers
	workerCount := 3
	dbChan := make(chan config.PgBaseBackupDatabaseConfig, len(databases))
	var wg sync.WaitGroup

	// Start worker goroutines
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go pgBasebackupWorker(dbChan, &wg)
	}

	// Send databases to the pgDumpWorker channel
	for _, db := range databases {
		dbChan <- db
	}

	// Close the channel and wait for workers to finish
	close(dbChan)
	wg.Wait()
}

// pgDumpWorker handles executing pg_dump tasks.
func pgBasebackupWorker(databases <-chan config.PgBaseBackupDatabaseConfig, wg *sync.WaitGroup) {
	defer wg.Done()

	for db := range databases {
		if err := dumpCluster(db); err != nil {
			// log errors, and continue, don't care about,
			// the dump is performed in a tmp (*.dirty) directory
			slog.Error("backup",
				slog.String("status", "error"),
				slog.String("err", err.Error()),
				slog.String("server", fmt.Sprintf("%s:%d", db.Host, db.Port)),
			)
		}
	}
}

// dumpDatabase executes pg_dump for a given database.
func dumpCluster(db config.PgBaseBackupDatabaseConfig) error {
	var err error
	cfg := config.Cfg()

	slog.Info("backup",
		slog.String("status", "run"),
		slog.String("mode", "pg_basebackup"),
		slog.String("server", fmt.Sprintf("%s:%d", db.Host, db.Port)),
	)

	connStrBasebackup, err := util.CreateConnStrBasebackup(db)
	if err != nil {
		return err
	}

	// layout: datetime--host-port--dbname.dmp
	dumpName := fmt.Sprintf("%s--%s-%d--pg_basebackup", ts.WorkingTimestamp, db.Host, db.Port)
	// need in case backup is failed
	tmpDest := filepath.Join(cfg.Dest, dumpName+".dirty")
	// rename to target, if everything is success
	okDest := filepath.Join(cfg.Dest, dumpName+".dmp")
	// prepare directory
	err = os.MkdirAll(tmpDest, 0o755)
	if err != nil {
		return fmt.Errorf("cannot create target dir %s, cause: %w", tmpDest, err)
	}

	// prepare args with optional filters

	args := []string{
		"--dbname=" + connStrBasebackup,
		"--pgdata=" + tmpDest,
		"--checkpoint=fast",
		"--progress",
		"--no-password",
		"--format=tar",
		"--gzip",
		"--verbose",
		"--verbose", // yes, twice
	}

	// execute dump CMD
	var stdoutBuf, stderrBuf bytes.Buffer
	cmd := exec.Command("pg_basebackup", args...)
	if cfg.PrintDumpLogs {
		cmd.Stdout = io.MultiWriter(os.Stdout, &stdoutBuf)
		cmd.Stderr = io.MultiWriter(os.Stderr, &stderrBuf)
	} else {
		cmd.Stderr = &stderrBuf
	}
	// Set environment variables for authentication
	cmd.Env = append(cmd.Env, fmt.Sprintf("PGPASSWORD=%s", db.Password))
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("failed to dump %s: %v - %s",
			fmt.Sprintf("%s:%d", db.Host, db.Port),
			err,
			stderrBuf.String(),
		)
	}

	// if everything is ok, just rename a temporary dir into the target one
	err = os.Rename(tmpDest, okDest)
	if err != nil {
		return fmt.Errorf("cannot rename %s to %s, cause: %w\n", tmpDest, okDest, err)
	}

	slog.Info("backup",
		slog.String("status", "ok"),
		slog.String("mode", "pg_basebackup"),
		slog.String("server", fmt.Sprintf("%s:%d", db.Host, db.Port)),
		slog.String("path", filepath.ToSlash(okDest)),
	)
	return nil
}
