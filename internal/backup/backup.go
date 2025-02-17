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
	"time"

	"gopgdump/internal/naming"

	"gopgdump/config"
	"gopgdump/internal/util"
)

// remember timestamp for all backups
// it is easy to sort/retain when all backups in one iteration use one timestamp
var backupTimestamp = time.Now().Format(naming.TimestampLayout)

func RunBackup() {
	cfg := config.Cfg()
	databases := cfg.Dump.DBS

	// Number of concurrent workers
	workerCount := 3
	dbChan := make(chan config.PgDumpDatabaseConfig, len(databases))
	var wg sync.WaitGroup

	// Start worker goroutines
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go worker(dbChan, &wg)
	}

	// Send databases to the worker channel
	for _, db := range databases {
		dbChan <- db
	}

	// Close the channel and wait for workers to finish
	close(dbChan)
	wg.Wait()
}

// worker handles executing pg_dump tasks.
func worker(databases <-chan config.PgDumpDatabaseConfig, wg *sync.WaitGroup) {
	defer wg.Done()

	for db := range databases {
		if err := dumpDatabase(db); err != nil {
			// log errors, and continue, don't care about,
			// the dump is performed in a tmp (*.dirty) directory
			slog.Error("backup",
				slog.String("status", "error"),
				slog.String("err", err.Error()),
				slog.String("server", fmt.Sprintf("%s:%s", db.Host, db.Port)),
				slog.String("dbname", db.Dbname),
			)
		}
	}
}

// dumpDatabase executes pg_dump for a given database.
func dumpDatabase(db config.PgDumpDatabaseConfig) error {
	var err error
	cfg := config.Cfg()

	slog.Info("backup",
		slog.String("status", "run"),
		slog.String("server", fmt.Sprintf("%s:%s", db.Host, db.Port)),
		slog.String("dbname", db.Dbname),
	)

	connStr, err := util.CreateConnStr(db)
	if err != nil {
		return err
	}

	// layout: host-port/datetime-dbname
	hostPortPath := filepath.Join(cfg.Dest, naming.PgDumpPath, fmt.Sprintf("%s-%s.srv", db.Host, db.Port))
	// need in case backup is failed
	tmpDest := filepath.Join(hostPortPath, fmt.Sprintf("%s-%s.dirty", backupTimestamp, db.Dbname))
	// rename to target, if everything is success
	okDest := filepath.Join(hostPortPath, fmt.Sprintf("%s-%s.dmp", backupTimestamp, db.Dbname))
	// prepare directory
	err = os.MkdirAll(tmpDest, 0o755)
	if err != nil {
		return fmt.Errorf("cannot create target dir %s, cause: %w", tmpDest, err)
	}

	// prepare args with optional filters

	args := []string{
		"--dbname=" + connStr,
		"--file=" + tmpDest,
		"--format=directory",
		"--jobs=2",
		"--compress=1",
		"--no-password",
		"--verbose",
		"--verbose", // yes, twice
	}
	if len(db.Schemas) > 0 {
		for _, arg := range db.Schemas {
			args = append(args, "--schema="+arg)
		}
	}
	if len(db.ExcludeSchemas) > 0 {
		for _, arg := range db.ExcludeSchemas {
			args = append(args, "--exclude-schema="+arg)
		}
	}
	if len(db.Tables) > 0 {
		for _, arg := range db.Tables {
			args = append(args, "--table="+arg)
		}
	}
	if len(db.ExcludeTables) > 0 {
		for _, arg := range db.ExcludeTables {
			args = append(args, "--exclude-table="+arg)
		}
	}

	// execute dump CMD
	var stdoutBuf, stderrBuf bytes.Buffer
	cmd := exec.Command("pg_dump", args...)
	if cfg.PrintLogs {
		cmd.Stdout = io.MultiWriter(os.Stdout, &stdoutBuf)
		cmd.Stderr = io.MultiWriter(os.Stderr, &stderrBuf)
	} else {
		cmd.Stderr = &stderrBuf
	}
	// Set environment variables for authentication
	cmd.Env = append(cmd.Env, fmt.Sprintf("PGPASSWORD=%s", db.Password))
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("failed to dump %s: %v - %s", db.Dbname, err, stderrBuf.String())
	}

	// if everything is ok, just rename a temporary dir into the target one
	err = os.Rename(tmpDest, okDest)
	if err != nil {
		return fmt.Errorf("cannot rename %s to %s, cause: %w\n", tmpDest, okDest, err)
	}

	slog.Info("backup",
		slog.String("status", "ok"),
		slog.String("server", fmt.Sprintf("%s:%s", db.Host, db.Port)),
		slog.String("dbname", db.Dbname),
		slog.String("path", filepath.ToSlash(okDest)),
	)
	return nil
}
