package common

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"sync"
	"time"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v5"
)

const (
	DefaultConnWaitTimeout       = 30 * time.Second
	DefaultConnSleepWhileWaiting = 5 * time.Second

	IntegrationTestEnv  = "PGDUMP_EACH_INTEGRATION_TESTS_AVAILABLE"
	IntegrationTestFlag = "0xcafebabe"
)

var (
	setupOnce sync.Once
	setupErr  error
)

func SetupEnv(_ context.Context, connStr string) error {
	setupOnce.Do(func() {
		if err := validateConnStr(connStr); err != nil {
			setupErr = err
			return
		}
		if err := setEnvFromConnStr(connStr); err != nil {
			setupErr = err
			return
		}
		if err := checkRequired(); err != nil {
			setupErr = err
			return
		}
	})
	return setupErr
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

	os.Setenv("PGHOST", cfg.Host)
	os.Setenv("PGPORT", fmt.Sprintf("%d", cfg.Port))

	if cfg.User != "" {
		os.Setenv("PGUSER", cfg.User)
	}
	if cfg.Password != "" {
		os.Setenv("PGPASSWORD", cfg.Password)
	}

	return nil
}

func validateConnStr(connStr string) error {
	deadline := time.Now().Add(DefaultConnWaitTimeout)

	for {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		conn, err := pgx.Connect(ctx, connStr)
		if err == nil {
			if err := conn.Ping(ctx); err != nil {
				conn.Close(ctx)
				return fmt.Errorf("ping failed: %w", err)
			}
			slog.Info("pg_isready", slog.String("status", "ok"))
			conn.Close(ctx)
			return nil // Ready
		}

		if time.Now().After(deadline) {
			return fmt.Errorf("PostgreSQL not ready after %s: %w", DefaultConnWaitTimeout, err)
		}

		slog.Info("pg_isready", slog.String("status", "waiting"))
		time.Sleep(DefaultConnSleepWhileWaiting)
	}
}
