package main

import (
	"bytes"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"time"
)

// TODO: these values are configurable
const (
	targetDir     = "./backups"
	printDumpLogs = false
)

// remember timestamp for all backups
// it is easy to sort/retain when all backups in one iteration use one timestamp
var backupTimestamp = time.Now().Format("20060102150405")

type BackupConfig struct {
	// postgres://username:password@host:port/dbname?connect_timeout=5&sslmode=disable
	Host     string
	Port     string
	Username string
	Password string
	Dbname   string
	Opts     map[string]string

	// optional filters
	Schemas        []string
	ExcludeSchemas []string
	Tables         []string
	ExcludeTables  []string
}

func createConnStr(db BackupConfig) (string, error) {
	connStr := fmt.Sprintf("postgres://%s:%s@%s:%s/%s", db.Username, db.Password, db.Host, db.Port, db.Dbname)
	if len(db.Opts) > 0 {
		query := url.Values{}
		for key, value := range db.Opts {
			query.Set(key, value)
		}
		connStr = connStr + "?" + query.Encode()
	}
	parse, err := url.Parse(connStr)
	if err != nil {
		return "", err
	}
	return parse.String(), nil
}

// dumpDatabase executes pg_dump for a given database.
func dumpDatabase(db BackupConfig) error {
	var err error

	connStr, err := createConnStr(db)
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
		"--dbname=" + connStr,
		"--file=" + tmpDest,
		"--format=directory",
		"--jobs=2",
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
	cmd := exec.Command("pg_dump", args...)
	if printDumpLogs {
		cmd.Stdout = io.MultiWriter(os.Stdout, &stdoutBuf)
		cmd.Stderr = io.MultiWriter(os.Stderr, &stderrBuf)
	} else {
		cmd.Stderr = &stderrBuf
	}
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
			Host:     "localhost",
			Port:     "5432",
			Username: "postgres",
			Password: "postgres",
			Dbname:   "bookstore",
			Opts: map[string]string{
				"connect_timeout": "5",
				"sslmode":         "disable",
			},
			Schemas: []string{
				"public",
			},
		},
		{
			Host:     "10.40.240.189",
			Port:     "5432",
			Username: "postgres",
			Password: "postgres",
			Dbname:   "keycloak_base",
		},
		{
			Host:     "10.40.240.165",
			Port:     "30201",
			Username: "postgres",
			Password: "postgres",
			Dbname:   "vault",
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
