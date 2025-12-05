package restore

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"sync"

	"github.com/hashmap-kz/pgdump-each/internal/xutil"
)

type ClusterRestoreContext struct {
	ConnStr     string
	InputDir    string
	PgBinPath   string
	ExitOnError bool
	ParallelDBS int
	LogDir      string
}

func RunRestoreJobs(ctx context.Context, restoreContext *ClusterRestoreContext) error {
	if err := xutil.SetupEnv(ctx, restoreContext.ConnStr); err != nil {
		return err
	}

	databases, err := xutil.GetDatabases(ctx, restoreContext.ConnStr)
	if err != nil {
		return err
	}
	if len(databases) > 0 {
		return fmt.Errorf("cannot restore on non-empty cluster")
	}

	inputPath := restoreContext.InputDir

	dirs, err := xutil.GetDumpsInDir(inputPath)
	if err != nil {
		return err
	}
	if len(dirs) == 0 {
		return fmt.Errorf("no dumps were found")
	}

	if err := xutil.CompareChecksums(inputPath); err != nil {
		return err
	}

	if err := restoreGlobals(restoreContext, inputPath); err != nil {
		return err
	}

	if err := restoreCluster(ctx, restoreContext, dirs); err != nil {
		return err
	}

	slog.Info("result", slog.String("status", "ok"))
	return nil
}

func restoreGlobals(restoreContext *ClusterRestoreContext, inputPath string) error {
	psql, err := xutil.GetExec(restoreContext.PgBinPath, "psql")
	if err != nil {
		return err
	}

	globalsScript := filepath.Join(inputPath, "globals.sql")

	args := []string{
		"--dbname=" + restoreContext.ConnStr,
		"--file=" + globalsScript,
	}

	// It's completely fine to have errors when restoring globals.
	// For instance: in 99.9% cases you already have role 'postgres' in your newly created cluster.
	// And in 99.9% cases this role is also presented in globals objects for restore.
	// According to documentation, we may freely ignore these errors.
	//
	// "--variable=ON_ERROR_STOP=1",
	// "--single-transaction",

	// execute psql
	var stderrBuf bytes.Buffer
	cmd := exec.Command(psql, args...)
	cmd.Stderr = &stderrBuf
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("failed to restore globals %s: %v - %s", inputPath, err, stderrBuf.String())
	}

	slog.Info("restore",
		slog.String("status", "ok"),
		slog.String("globals", filepath.ToSlash(globalsScript)),
	)
	return nil
}

func restoreCluster(ctx context.Context, restoreContext *ClusterRestoreContext, dirs []*xutil.DBInfo) error {
	jobsWeights, err := xutil.GetJobsWeights(ctx, dirs, restoreContext.ConnStr)
	if err != nil {
		return err
	}

	slog.Info("restore",
		slog.Int("workers", restoreContext.ParallelDBS),
	)

	workerCount := restoreContext.ParallelDBS
	dbChan := make(chan *xutil.DBInfo, len(dirs))
	erChan := make(chan error, len(dirs))
	var wg sync.WaitGroup

	// Start worker goroutines
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for dumpDir := range dbChan {
				restoreErr := restoreDump(restoreContext, dumpDir, jobsWeights)
				if restoreErr != nil {
					erChan <- restoreErr
				}
			}
		}()
	}

	// Send databases to the pgDumpWorker channel
	for _, db := range dirs {
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
		slog.Error("restore-error", slog.Any("err", e))
		lastErr = e
	}
	return lastErr
}

func restoreDump(restoreContext *ClusterRestoreContext, dumpDirInfo *xutil.DBInfo, jobsWeights map[string]int) error {
	pgRestore, err := xutil.GetExec(restoreContext.PgBinPath, "pg_restore")
	if err != nil {
		return err
	}

	dumpDir := dumpDirInfo.DatName

	pgDumpJobs, ok := jobsWeights[dumpDir]
	if !ok {
		return fmt.Errorf("cannot find dump dir name in jobs-weights table: %s", dumpDir)
	}

	slog.Info("restore",
		slog.String("status", "run"),
		slog.String("dumpname", filepath.Base(dumpDir)),
		slog.String("dumpsize", xutil.ByteCountSI(dumpDirInfo.SizeBytes)),
		slog.Int("jobs", pgDumpJobs),
	)

	args := []string{
		"--dbname=" + restoreContext.ConnStr,
		"--create",
		"--format=directory",
		"--jobs=" + fmt.Sprintf("%d", pgDumpJobs),
		"--no-password",
		"--verbose",
		dumpDir + "/data",
	}
	if restoreContext.ExitOnError {
		args = append(args, "--exit-on-error")
	}

	// preserve logs for debug
	logFileName := fmt.Sprintf("restore-%s.log", filepath.Base(dumpDir))
	logFile, err := os.Create(filepath.Join(restoreContext.LogDir, logFileName))
	if err != nil {
		return fmt.Errorf("failed to create log file: %w", err)
	}
	defer logFile.Close()

	// execute CMD
	cmd := exec.Command(pgRestore, args...)
	cmd.Stderr = logFile // write directly to file
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("failed to restore %s: %v", dumpDir, err)
	}

	slog.Info("restore",
		slog.String("status", "ok"),
		slog.String("dump", filepath.ToSlash(dumpDir)),
	)
	return nil
}
