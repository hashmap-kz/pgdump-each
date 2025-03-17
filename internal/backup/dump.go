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
	dbChan := make(chan config.PgDumpDatabase, len(databases))
	resultChan := make(chan *ResultInfo, len(databases))
	var wg sync.WaitGroup

	// Start worker goroutines
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for db := range dbChan {
				if err := dumpDatabase(db); err != nil {
					// log errors, and continue, don't care about,
					// the dump is performed in a tmp (*.dirty) directory
					slog.Error("backup",
						slog.String("status", "error"),
						slog.String("err", err.Error()),
						slog.String("server", fmt.Sprintf("%s:%d/%s", db.Host, db.Port, db.Dbname)),
					)
					resultChan <- &ResultInfo{
						Host:   db.Host,
						Port:   db.Port,
						Dbname: db.Dbname,
						Err:    err,
					}
				} else {
					resultChan <- &ResultInfo{
						Host:   db.Host,
						Port:   db.Port,
						Dbname: db.Dbname,
						Err:    nil,
					}
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
		close(resultChan)
	}()

	var result []*ResultInfo
	for r := range resultChan {
		result = append(result, r)
	}
	return result
}

// dumpDatabase executes pg_dump for a given database.
func dumpDatabase(db config.PgDumpDatabase) error {
	var err error
	cfg := config.Cfg()
	if !cfg.Dump.Enable {
		return nil
	}

	pgDump, err := exec.LookPath("pg_dump")
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

	connStr, err := connstr.CreateConnStr(connstr.ConnStr{
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
		"--file=" + tmpDest,
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
		slog.String("mode", "pg_dump"),
		slog.String("server", fmt.Sprintf("%s:%d/%s", db.Host, db.Port, db.Dbname)),
		slog.String("path", filepath.ToSlash(okDest)),
	)
	return nil
}
