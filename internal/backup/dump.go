package backup

import (
	"bytes"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"

	"gopgdump/internal/common"

	"gopgdump/internal/connstr"

	"gopgdump/internal/timestamp"

	"gopgdump/config"
)

func RunPgDumps() []*ResultInfo {
	cfg := config.Cfg()
	if !cfg.Dump.Enable {
		return nil
	}

	databases := cfg.Dump.Databases

	// Number of concurrent workers
	workerCount := common.GetMaxConcurrency(cfg.Dump.MaxConcurrency)
	dbChan := make(chan *config.PgDumpDatabase, len(databases))
	resultChan := make(chan *ResultInfo, len(databases))
	var wg sync.WaitGroup

	// Start worker goroutines
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for db := range dbChan {
				result := &ResultInfo{
					Host:   db.Host,
					Port:   db.Port,
					Dbname: db.Dbname,
					Mode:   "pg_dump",
				}
				if err := dumpDatabase(db); err != nil {
					// log errors, and continue, don't care about,
					// the dump is performed in a tmp (*.dirty) directory
					slog.Error("backup",
						slog.String("status", "error"),
						slog.String("err", err.Error()),
						slog.String("server", fmt.Sprintf("%s:%d/%s", db.Host, db.Port, db.Dbname)),
					)
					result.Err = err
				}
				resultChan <- result
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
		close(resultChan)
	}()

	result := make([]*ResultInfo, 0, len(resultChan))
	for r := range resultChan {
		result = append(result, r)
	}
	return result
}

// dumpDatabase executes pg_dump for a given database.
func dumpDatabase(db *config.PgDumpDatabase) error {
	var err error
	cfg := config.Cfg()
	if !cfg.Dump.Enable {
		return nil
	}

	pgDump, err := common.GetExec(db.PGBinPath, "pg_dump")
	if err != nil {
		return err
	}

	// set jobs
	jobs := db.Jobs
	if jobs <= 0 || jobs >= 32 {
		jobs = config.PgDumpJobsDefault
	}

	slog.Info("backup",
		slog.String("status", "run"),
		slog.String("mode", "pg_dump"),
		slog.String("server", fmt.Sprintf("%s:%d/%s", db.Host, db.Port, db.Dbname)),
		slog.Int("jobs", jobs),
	)

	connStr, err := connstr.CreateConnStr(&connstr.ConnStr{
		Host:     db.Host,
		Port:     db.Port,
		Username: db.Username,
		Password: db.Password,
		Dbname:   db.Dbname,
		Opts:     db.Opts,
	})
	if err != nil {
		return err
	}

	// layout: datetime--host-port--dbname.dmp
	dumpName := fmt.Sprintf("%s--%s-%d--%s", timestamp.WorkingTimestamp, db.Host, db.Port, db.Dbname)
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
		"--dbname=" + connStr,
		"--file=" + tmpDest + "/data",
		"--format=directory",
		"--jobs=" + fmt.Sprintf("%d", jobs),
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
	cmd := exec.Command(pgDump, args...)
	if cfg.PrintDumpLogs {
		cmd.Stdout = io.MultiWriter(os.Stdout, &stdoutBuf)
		cmd.Stderr = io.MultiWriter(os.Stderr, &stderrBuf)
	} else {
		cmd.Stderr = &stderrBuf
	}
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("failed to dump %s: %v - %s", db.Dbname, err, stderrBuf.String())
	}

	// if everything is ok, just rename a temporary dir into the target one
	err = os.Rename(tmpDest, okDest)
	if err != nil {
		return fmt.Errorf("cannot rename %s to %s, cause: %w", tmpDest, okDest, err)
	}

	logFileContent := stderrBuf.Bytes()

	// dump globals
	if cfg.Dump.DumpGlobals {
		pgDumpAllSQL, pgDumpAllLogs, err := dumpGlobals(db.PGBinPath, connStr)
		if err != nil {
			slog.Warn("globals", slog.String("err-dump-globals", err.Error()))
		}
		err = os.WriteFile(filepath.Join(okDest, "globals.sql"), pgDumpAllSQL, 0o600)
		if err != nil {
			slog.Warn("globals", slog.String("err-save-globals", err.Error()))
		}
		if len(pgDumpAllLogs) > 0 {
			logFileContent = append(logFileContent, []byte("\n\n")...)
			logFileContent = append(logFileContent, pgDumpAllLogs...)
		}
	}

	// save restore script
	if cfg.Dump.CreateRestoreScript {
		restoreScript, err := createRestoreScript(db)
		if err != nil {
			slog.Warn("restore-script", slog.String("err-create-script", err.Error()))
		}
		err = os.WriteFile(filepath.Join(okDest, "restore.sh"), []byte(restoreScript+"\n"), 0o600)
		if err != nil {
			slog.Warn("restore-script", slog.String("err-save-script", err.Error()))
		}
	}

	// save dump logs
	err = os.WriteFile(filepath.Join(okDest, "dump.log"), logFileContent, 0o600)
	if err != nil {
		slog.Warn("logs", slog.String("err-save-logs", err.Error()))
	}

	slog.Info("backup",
		slog.String("status", "ok"),
		slog.String("mode", "pg_dump"),
		slog.String("server", fmt.Sprintf("%s:%d/%s", db.Host, db.Port, db.Dbname)),
		slog.String("path", filepath.ToSlash(okDest)),
	)
	return nil
}

func dumpGlobals(binPath, connStr string) (sql, logs []byte, err error) {
	cfg := config.Cfg()

	pgDumpall, err := common.GetExec(binPath, "pg_dumpall")
	if err != nil {
		return nil, nil, err
	}

	args := []string{
		"--dbname=" + connStr,
		"--globals-only",
		"--verbose",
		"--verbose", // yes, twice
	}

	var stdoutBuf, stderrBuf bytes.Buffer
	cmd := exec.Command(pgDumpall, args...)
	if cfg.PrintDumpLogs {
		cmd.Stdout = io.MultiWriter(os.Stdout, &stdoutBuf)
		cmd.Stderr = io.MultiWriter(os.Stderr, &stderrBuf)
	} else {
		cmd.Stdout = &stdoutBuf
		cmd.Stderr = &stderrBuf
	}

	if err := cmd.Run(); err != nil {
		return nil, stderrBuf.Bytes(), err
	}
	return stdoutBuf.Bytes(), stderrBuf.Bytes(), nil
}

func createRestoreScript(db *config.PgDumpDatabase) (string, error) {
	template := strings.TrimSpace(`
#!/bin/bash
set -euo pipefail

export PGHOST='{{.Host}}'
export PGPORT='{{.Port}}'

# change these placeholders with real superuser name/pass
export PGUSER=postgres
export PGPASSWORD=postgres

# database to restore, the target
export RESTORE_TARGET_DB='{{.TargetDB}}'
export RESTORE_GLOBALS=true

psql -v ON_ERROR_STOP=1 --username "${PGUSER}" <<-EOSQL
  CREATE DATABASE ${RESTORE_TARGET_DB} encoding 'UTF8';
EOSQL

# It's okay to ignore errors while restoring global objects, as per the documentation.
# Some roles (e.g., 'postgres') may already exist, so it's better to skip them gracefully.
if [[ "${RESTORE_GLOBALS:-false}" = 'true' ]]; then
  psql --username "${PGUSER}" <globals.sql
fi

# additionally, you may use '--exit-on-error' flag here
pg_restore \
  --dbname="${RESTORE_TARGET_DB}" \
  --format=directory \
  --jobs=2 \
  --no-password \
  --verbose data
`)
	data := map[string]any{
		"Host":     db.Host,
		"Port":     db.Port,
		"TargetDB": db.Dbname + "_restore_" + timestamp.WorkingTimestamp,
	}

	restoreScript, err := common.ExecTemplate("pg_restore_script", template, data, map[string]any{})
	if err != nil {
		return "", err
	}
	return restoreScript, nil
}
