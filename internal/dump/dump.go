package dump

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"time"

	"github.com/hashmap-kz/pgdump-each/internal/common"
)

const GlobalsFileName = "globals.sql"

var WorkingTimestamp = time.Now().Truncate(time.Second).Format("20060102150405")

type ClusterDumpContext struct {
	ConnStr     string
	OutputDir   string
	PgBinPath   string
	Compress    string
	ParallelDBS int
}

func RunDumpJobs(ctx context.Context, dumpContext *ClusterDumpContext) error {
	if err := common.SetupEnv(ctx, dumpContext.ConnStr); err != nil {
		return err
	}

	stageDir := filepath.Join(dumpContext.OutputDir, fmt.Sprintf("%s.dirty", WorkingTimestamp))
	finalDir := filepath.Join(dumpContext.OutputDir, fmt.Sprintf("%s.dmp", WorkingTimestamp))

	if err := os.MkdirAll(stageDir, 0o755); err != nil {
		return err
	}
	// in case job failed, cleanup the stage
	defer os.RemoveAll(stageDir)

	// run jobs
	if err := dumpCluster(ctx, dumpContext, stageDir); err != nil {
		return err
	}

	// save globals
	if err := writeGlobalsFile(dumpContext, stageDir); err != nil {
		return err
	}

	// save checksums
	if err := common.WriteChecksumsFile(stageDir); err != nil {
		return err
	}

	// ONLY if ALL backups were successfully finished, rename staging to final
	if err := os.Rename(stageDir, finalDir); err != nil {
		return err
	}

	slog.Info("result",
		slog.String("status", "ok"),
		slog.String("path", filepath.ToSlash(finalDir)),
	)
	return nil
}

func dumpCluster(ctx context.Context, dumpContext *ClusterDumpContext, stageDir string) error {
	databases, err := common.GetDatabases(ctx, dumpContext.ConnStr)
	if err != nil {
		return err
	}

	jobsWeights, err := common.GetJobsWeights(ctx, databases, dumpContext.ConnStr)
	if err != nil {
		return err
	}

	slog.Info("dump",
		slog.Int("workers", dumpContext.ParallelDBS),
	)

	workerCount := dumpContext.ParallelDBS
	dbChan := make(chan *common.DBInfo, len(databases))
	erChan := make(chan error, len(databases))
	var wg sync.WaitGroup

	// Start worker goroutines
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for db := range dbChan {
				dumpErr := dumpDatabase(dumpContext, db, stageDir, jobsWeights)
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
func dumpDatabase(dumpContext *ClusterDumpContext, dbInfo *common.DBInfo, stageDir string, jobsWeights map[string]int) error {
	var err error

	db := dbInfo.DatName

	pgDump, err := common.GetExec(dumpContext.PgBinPath, "pg_dump")
	if err != nil {
		return err
	}

	pgDumpJobs, ok := jobsWeights[db]
	if !ok {
		return fmt.Errorf("cannot find database name in jobs-weights table: %s", db)
	}

	slog.Info("dump",
		slog.String("status", "run"),
		slog.String("dbname", dbInfo.DatName),
		slog.String("dbsize", common.ByteCountSI(dbInfo.SizeBytes)),
		slog.Int("jobs", pgDumpJobs),
	)

	// need in case backup is failed
	tmpDest := filepath.Join(stageDir, db+".dirty")
	// rename to target, if everything is success
	okDest := filepath.Join(stageDir, db+".dmp")
	// prepare directory
	if err := os.MkdirAll(tmpDest, 0o755); err != nil {
		return fmt.Errorf("cannot create target dir %s, cause: %w", tmpDest, err)
	}

	// prepare args with optional filters

	args := []string{
		"--dbname=" + db,
		"--file=" + tmpDest + "/data",
		"--format=directory",
		"--jobs=" + fmt.Sprintf("%d", pgDumpJobs),
		"--no-password",
		"--verbose",
		"--verbose", // yes, twice
	}
	if dumpContext.Compress != "" {
		args = append(args, fmt.Sprintf("--compress=%s", dumpContext.Compress))
	}

	// execute dump CMD
	var stderrBuf bytes.Buffer
	cmd := exec.Command(pgDump, args...)
	cmd.Stderr = &stderrBuf
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("failed to dump %s: %v - %s", db, err, stderrBuf.String())
	}

	// if everything is ok, just rename a temporary dir into the target one
	err = os.Rename(tmpDest, okDest)
	if err != nil {
		return fmt.Errorf("cannot rename %s to %s, cause: %w", tmpDest, okDest, err)
	}

	logFileContent := stderrBuf.Bytes()

	// save dump logs
	err = os.WriteFile(filepath.Join(okDest, "dump.log"), logFileContent, 0o600)
	if err != nil {
		slog.Warn("logs", slog.String("err-save-logs", err.Error()))
	}

	slog.Info("dump",
		slog.String("status", "ok"),
		slog.String("path", filepath.ToSlash(okDest)),
	)
	return nil
}

func writeGlobalsFile(dumpContext *ClusterDumpContext, path string) error {
	pgDumpAllSQL, _, err := dumpGlobals(dumpContext)
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(path, GlobalsFileName), pgDumpAllSQL, 0o600); err != nil {
		return err
	}
	return nil
}

func dumpGlobals(dumpContext *ClusterDumpContext) (sql, logs []byte, err error) {
	pgDumpall, err := common.GetExec(dumpContext.PgBinPath, "pg_dumpall")
	if err != nil {
		return nil, nil, err
	}

	args := []string{
		"--dbname=" + dumpContext.ConnStr,
		"--globals-only",
		"--clean",
		"--if-exists",
		"--verbose",
		"--verbose", // yes, twice
	}

	var stdoutBuf, stderrBuf bytes.Buffer
	cmd := exec.Command(pgDumpall, args...)
	cmd.Stdout = &stdoutBuf
	cmd.Stderr = &stderrBuf

	if err := cmd.Run(); err != nil {
		return nil, stderrBuf.Bytes(), err
	}
	return stdoutBuf.Bytes(), stderrBuf.Bytes(), nil
}
