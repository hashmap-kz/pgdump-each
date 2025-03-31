package common

import (
	"context"
	"fmt"
	"os"
	"os/exec"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v5"
)

func SetupEnv(ctx context.Context, connStr string) error {
	if err := validateConnStr(ctx, connStr); err != nil {
		return err
	}
	if err := setEnvFromConnStr(connStr); err != nil {
		return err
	}
	if err := checkRequired(); err != nil {
		return err
	}
	return nil
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

func validateConnStr(ctx context.Context, connStr string) error {
	conn, err := pgx.Connect(ctx, connStr)
	if err != nil {
		return fmt.Errorf("failed to connect to PostgreSQL: %w", err)
	}
	defer conn.Close(ctx)
	if err := conn.Ping(ctx); err != nil {
		return fmt.Errorf("ping failed: %w", err)
	}
	return nil
}
