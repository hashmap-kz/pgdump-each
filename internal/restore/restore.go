package restore

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"

	"gopgdump/internal/common"
)

type ClusterRestoreContext struct {
	ConnStr   string
	InputDir  string
	PgBinPath string
}

func RunRestoreJobs(ctx context.Context, restoreContext *ClusterRestoreContext) error {
	databases, err := common.GetDatabases(ctx, restoreContext.ConnStr)
	if err != nil {
		return err
	}
	if len(databases) > 0 {
		return fmt.Errorf("cannot restore on non-empty cluster")
	}

	inputPath := restoreContext.InputDir

	dirs, err := listTopLevelDirs(inputPath)
	if err != nil {
		return err
	}
	if len(dirs) == 0 {
		return fmt.Errorf("no dumps were found")
	}

	if err := common.CompareChecksums(inputPath); err != nil {
		return err
	}

	if err := restoreGlobals(restoreContext, inputPath); err != nil {
		return err
	}

	return restoreCluster(restoreContext, dirs)
}

func restoreGlobals(restoreContext *ClusterRestoreContext, inputPath string) error {
	psql, err := common.GetExec(restoreContext.PgBinPath, "psql")
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
		slog.String("globals", globalsScript),
	)
	return nil
}

func restoreCluster(restoreContext *ClusterRestoreContext, dirs []string) error {
	// TODO: adjust with CLI parameters (if any)
	parallelSettings, err := common.CalculateParallelSettings(len(dirs), runtime.NumCPU())
	if err != nil {
		return err
	}
	slog.Info("restore-cluster",
		slog.Int("db-workers", parallelSettings.DBWorkers),
		slog.Int("pgdump-jobs", parallelSettings.PGDumpJobs),
	)

	workerCount := parallelSettings.DBWorkers
	dbChan := make(chan string, len(dirs))
	erChan := make(chan error, len(dirs))
	var wg sync.WaitGroup

	// Start worker goroutines
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for dumpDir := range dbChan {
				restoreErr := restoreDump(restoreContext, dumpDir, parallelSettings.PGDumpJobs)
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

func restoreDump(restoreContext *ClusterRestoreContext, dumpDir string, pgDumpJobs int) error {
	pgRestore, err := common.GetExec(restoreContext.PgBinPath, "pg_restore")
	if err != nil {
		return err
	}

	args := []string{
		"--dbname=" + restoreContext.ConnStr,
		"--create",
		"--exit-on-error",
		"--format=directory",
		"--jobs=" + fmt.Sprintf("%d", pgDumpJobs),
		"--no-password",
		"--verbose",
		dumpDir + "/data",
	}

	// execute dump CMD
	var stderrBuf bytes.Buffer
	cmd := exec.Command(pgRestore, args...)
	cmd.Stderr = &stderrBuf
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("failed to restore %s: %v - %s", dumpDir, err, stderrBuf.String())
	}

	logFileContent := stderrBuf.Bytes()

	// save dump logs
	err = os.WriteFile(fmt.Sprintf("restore-%s.log", filepath.Base(dumpDir)), logFileContent, 0o600)
	if err != nil {
		slog.Warn("logs", slog.String("err-save-logs", err.Error()))
	}

	slog.Info("restore",
		slog.String("status", "ok"),
		slog.String("dump", dumpDir),
	)
	return nil
}

func listTopLevelDirs(path string) ([]string, error) {
	entries, err := os.ReadDir(path)
	if err != nil {
		return nil, err
	}
	var dirs []string
	for _, entry := range entries {
		if entry.IsDir() && strings.HasSuffix(entry.Name(), ".dmp") {
			dirs = append(dirs, filepath.Join(path, entry.Name()))
		}
	}
	return dirs, nil
}
