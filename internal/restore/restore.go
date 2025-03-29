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

	"gopgdump/internal/common"
)

func RunRestoreJobs(ctx context.Context, connStr, inputPath string) error {
	databases, err := common.GetDatabases(ctx, connStr)
	if err != nil {
		return err
	}
	if len(databases) > 0 {
		return fmt.Errorf("cannot restore on non-empty cluster")
	}

	dirs, err := listTopLevelDirs(inputPath)
	if err != nil {
		return err
	}
	if len(dirs) == 0 {
		return fmt.Errorf("no dumps were found")
	}

	// TODO: restore globals file

	return runRestoreJobsForDumps(connStr, dirs)
}

func runRestoreJobsForDumps(connStr string, dirs []string) error {
	workerCount := 3
	dbChan := make(chan string, len(dirs))
	erChan := make(chan error, len(dirs))
	var wg sync.WaitGroup

	// Start worker goroutines
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for dumpDir := range dbChan {
				restoreErr := restoreDump(connStr, dumpDir)
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

func restoreDump(connStr, dumpDir string) error {
	args := []string{
		"--dbname=" + connStr,
		"--create",
		"--exit-on-error",
		"--format=directory",
		"--jobs=3",
		"--no-password",
		"--verbose",
		dumpDir + "/data",
	}

	// execute dump CMD
	var stderrBuf bytes.Buffer
	cmd := exec.Command("pg_restore", args...)
	cmd.Stderr = &stderrBuf
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("failed to restore %s: %v - %s", dumpDir, err, stderrBuf.String())
	}

	logFileContent := stderrBuf.Bytes()

	// save dump logs
	err := os.WriteFile(filepath.Base(dumpDir)+"-restore.log", logFileContent, 0o600)
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
		if entry.IsDir() {
			dirs = append(dirs, filepath.Join(path, entry.Name()))
		}
	}
	return dirs, nil
}
