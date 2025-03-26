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

	"gopgdump/config"
	"gopgdump/internal/timestamp"
)

func RunPgBasebackups() []*ResultInfo {
	cfg := config.Cfg()
	if !cfg.Base.Enable {
		return nil
	}

	clusters := cfg.Base.Clusters

	// Number of concurrent workers
	workerCount := common.GetMaxConcurrency(cfg.Base.MaxConcurrency)
	clusterChan := make(chan *config.PgBaseBackupCluster, len(clusters))
	resultChan := make(chan *ResultInfo, len(clusters))
	var wg sync.WaitGroup

	// Start worker goroutines
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for db := range clusterChan {
				if err := dumpCluster(db); err != nil {
					// log errors, and continue, don't care about,
					// the dump is performed in a tmp (*.dirty) directory
					slog.Error("backup",
						slog.String("status", "error"),
						slog.String("err", err.Error()),
						slog.String("server", fmt.Sprintf("%s:%d", db.Host, db.Port)),
					)
					resultChan <- &ResultInfo{
						Host: db.Host,
						Port: db.Port,
						Mode: "pg_basebackup",
						Err:  err,
					}
				} else {
					resultChan <- &ResultInfo{
						Host: db.Host,
						Port: db.Port,
						Mode: "pg_basebackup",
					}
				}
			}
		}()
	}

	// Send clusters to the pgDumpWorker channel
	for _, db := range clusters {
		clusterChan <- db
	}
	close(clusterChan) // Close the task channel once all tasks are submitted

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
func dumpCluster(cluster *config.PgBaseBackupCluster) error {
	var err error
	cfg := config.Cfg()

	pgBasebackup, err := common.GetExec(cluster.PGBinPath, "pg_basebackup")
	if err != nil {
		return err
	}

	slog.Info("backup",
		slog.String("status", "run"),
		slog.String("mode", "pg_basebackup"),
		slog.String("cluster", fmt.Sprintf("%s:%d", cluster.Host, cluster.Port)),
	)

	connStrBasebackup, err := connstr.CreateConnStr(&connstr.ConnStr{
		Host:     cluster.Host,
		Port:     cluster.Port,
		Username: cluster.Username,
		Password: cluster.Password,
		Opts:     cluster.Opts,
	})
	if err != nil {
		return err
	}

	// layout: datetime--host-port--dbname.dmp
	dumpName := fmt.Sprintf("%s--%s-%d--__pg_basebackup__", timestamp.WorkingTimestamp, cluster.Host, cluster.Port)
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
		"--pgdata=" + tmpDest + "/data",
		"--checkpoint=fast",
		"--progress",
		"--no-password",
		"--format=tar",
		"--gzip",
		"--verbose",
	}

	// execute dump CMD
	var stdoutBuf, stderrBuf bytes.Buffer
	cmd := exec.Command(pgBasebackup, args...)
	if cfg.PrintDumpLogs {
		cmd.Stdout = io.MultiWriter(os.Stdout, &stdoutBuf)
		cmd.Stderr = io.MultiWriter(os.Stderr, &stderrBuf)
	} else {
		cmd.Stderr = &stderrBuf
	}
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("failed to dump %s: %v - %s",
			fmt.Sprintf("%s:%d", cluster.Host, cluster.Port),
			err,
			stderrBuf.String(),
		)
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
		slog.String("mode", "pg_basebackup"),
		slog.String("cluster", fmt.Sprintf("%s:%d", cluster.Host, cluster.Port)),
		slog.String("path", filepath.ToSlash(okDest)),
	)
	return nil
}
