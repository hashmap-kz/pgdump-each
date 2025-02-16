package main

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"time"
)

const targetDir = "./backups"

type Database struct {
	DBName string
	User   string
	Host   string
	Port   string

	// optional filters
	Schemas        []string
	ExcludeSchemas []string
	Tables         []string
	ExcludeTables  []string
}

// dumpDatabase executes pg_dump for a given database.
func dumpDatabase(db Database) error {
	var err error
	// remember timestamp
	ts := time.Now().Format("2006_01_02_150405")
	// layout: host_port/datetime_dbname
	hostPortPath := filepath.Join(targetDir, fmt.Sprintf("%s_%s", db.Host, db.Port))
	// need in case backup is failed
	tmpDest := filepath.Join(hostPortPath, fmt.Sprintf("%s_%s.dirty", ts, db.DBName))
	// rename to target, if everything is success
	okDest := filepath.Join(hostPortPath, fmt.Sprintf("%s_%s.dmp", ts, db.DBName))
	// prepare directory
	err = os.MkdirAll(tmpDest, 0o755)
	if err != nil {
		return fmt.Errorf("cannot create target dir %s, cause: %w", tmpDest, err)
	}

	// prepare args with optional filters

	args := []string{
		"--host=" + db.Host,
		"--port=" + db.Port,
		"--username=" + db.User,
		"--dbname=" + db.DBName,
		"--format=directory",
		"--file=" + tmpDest,
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
	var stderr bytes.Buffer
	cmd := exec.Command("pg_dump", args...)
	cmd.Stderr = &stderr
	// Set environment variables for authentication
	cmd.Env = append(cmd.Env, "PGPASSWORD=postgres") // Replace with a secure method
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("failed to dump %s: %v - %s", db.DBName, err, stderr.String())
	}

	// if everything is ok, just rename a temporary dir into the target one
	err = os.Rename(tmpDest, okDest)
	if err != nil {
		return fmt.Errorf("cannot rename %s to %s, cause: %w\n", tmpDest, okDest, err)
	}

	fmt.Printf("Backup completed: %s -> %s\n", db.DBName, filepath.ToSlash(okDest))
	return nil
}

// worker handles executing pg_dump tasks.
func worker(databases <-chan Database, wg *sync.WaitGroup) {
	defer wg.Done()

	for db := range databases {
		if err := dumpDatabase(db); err != nil {
			fmt.Println(err)
		}
	}
}

func main() {
	// Define your databases here
	databases := []Database{
		{
			DBName: "keycloak_base",
			User:   "postgres",
			Host:   "10.40.240.189",
			Port:   "5432",
		},
		{
			DBName: "bookstore",
			User:   "postgres",
			Host:   "localhost",
			Port:   "5432",
		},
		{
			DBName: "vault",
			User:   "postgres",
			Host:   "10.40.240.165",
			Port:   "30201",
		},
	}

	// Number of concurrent workers
	workerCount := 3
	dbChan := make(chan Database, len(databases))
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
