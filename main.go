package main

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const targetDir = "./backups"

// remember timestamp for all backups
// it is easy to sort/retain when all backups in one iteration use one timestamp
var backupTimestamp = time.Now().Format("20060102150405")

type BackupConfig struct {
	// postgres://username:password@host:port/dbname
	ConnStr string

	// optional filters
	Schemas        []string
	ExcludeSchemas []string
	Tables         []string
	ExcludeTables  []string
}

type HostConfig struct {
	Host     string
	Port     string
	Username string
	Password string
	Dbname   string
	Params   string
}

func parseConnStr(connString string) (*HostConfig, error) {
	pref := strings.HasPrefix(connString, "postgres://") || strings.HasPrefix(connString, "postgresql://")
	if !pref {
		return nil, fmt.Errorf("not a postgresql conn-string: %s", connString)
	}

	parsedURL, err := url.Parse(connString)
	if err != nil {
		if urlErr := new(url.Error); errors.As(err, &urlErr) {
			return nil, urlErr.Err
		}
		return nil, err
	}

	var usr, pass string
	if parsedURL.User != nil {
		usr = parsedURL.User.Username()
		if password, present := parsedURL.User.Password(); present {
			pass = password
		}
	}

	host, port, err := net.SplitHostPort(parsedURL.Host)
	if err != nil {
		return nil, fmt.Errorf("failed to split host:port in '%s', err: %w", parsedURL.Host, err)
	}

	return &HostConfig{
		Host:     host,
		Port:     port,
		Username: usr,
		Password: pass,
		Dbname:   strings.TrimLeft(parsedURL.Path, "/"),
		Params:   parsedURL.RawQuery,
	}, nil
}

// dumpDatabase executes pg_dump for a given database.
func dumpDatabase(backupConfig BackupConfig) error {
	var err error

	db, err := parseConnStr(backupConfig.ConnStr)
	if err != nil {
		return err
	}

	// layout: host-port/datetime-dbname
	hostPortPath := filepath.Join(targetDir, fmt.Sprintf("%s-%s", db.Host, db.Port))
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
		"--dbname=" + backupConfig.ConnStr,
		"--file=" + tmpDest,
		"--format=directory",
		"--jobs=2",
		"--compress=1",
		"--no-password",
		"--verbose",
		"--verbose", // yes, twice
	}
	if len(backupConfig.Schemas) > 0 {
		for _, arg := range backupConfig.Schemas {
			args = append(args, "--schema="+arg)
		}
	}
	if len(backupConfig.ExcludeSchemas) > 0 {
		for _, arg := range backupConfig.ExcludeSchemas {
			args = append(args, "--exclude-schema="+arg)
		}
	}
	if len(backupConfig.Tables) > 0 {
		for _, arg := range backupConfig.Tables {
			args = append(args, "--table="+arg)
		}
	}
	if len(backupConfig.ExcludeTables) > 0 {
		for _, arg := range backupConfig.ExcludeTables {
			args = append(args, "--exclude-table="+arg)
		}
	}

	// execute dump CMD
	var stdoutBuf, stderrBuf bytes.Buffer
	cmd := exec.Command("pg_dump", args...)
	cmd.Stdout = io.MultiWriter(os.Stdout, &stdoutBuf)
	cmd.Stderr = io.MultiWriter(os.Stderr, &stderrBuf)
	// Set environment variables for authentication
	cmd.Env = append(cmd.Env, "PGPASSWORD=postgres") // Replace with a secure method
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("failed to dump %s: %v - %s", db.Dbname, err, stderrBuf.String())
	}

	// if everything is ok, just rename a temporary dir into the target one
	err = os.Rename(tmpDest, okDest)
	if err != nil {
		return fmt.Errorf("cannot rename %s to %s, cause: %w\n", tmpDest, okDest, err)
	}

	fmt.Printf("Backup completed: %s -> %s\n", db.Dbname, filepath.ToSlash(okDest))
	return nil
}

// worker handles executing pg_dump tasks.
func worker(databases <-chan BackupConfig, wg *sync.WaitGroup) {
	defer wg.Done()

	for db := range databases {
		if err := dumpDatabase(db); err != nil {
			fmt.Println(err)
		}
	}
}

func main() {
	// Define your databases here
	databases := []BackupConfig{
		{
			ConnStr: "postgres://postgres:postgres@localhost:5432/bookstore?connect_timeout=5&sslmode=disable",
		},
		{
			ConnStr: "postgres://postgres:postgres@10.40.240.189:5432/keycloak_base",
		},
		{
			ConnStr: "postgres://postgres:postgres@10.40.240.165:30201/vault",
		},
	}

	// TODO: before concurrent tasks
	// 1) remove all '*.dirty' dirs, if any
	// 2) process purge jobs
	//

	// Number of concurrent workers
	workerCount := 3
	dbChan := make(chan BackupConfig, len(databases))
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

	fmt.Println("All backups completed.")
}
