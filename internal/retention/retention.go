package retention

import (
	"fmt"
	"gopgdump/config"
	"gopgdump/internal/naming"
	"os"
	"path/filepath"
	"time"
)

func PurgeOldDirs() error {
	cfg := config.Cfg()
	if !cfg.Retention.Enable {
		return nil
	}
	_, err := time.ParseDuration(cfg.Retention.Period)
	if err != nil {
		return err
	}

	dirs, err := findDumpsDirs()
	if err != nil {
		return err
	}

	// host=dump
	rmap := map[string]string{}
	for _, d := range dirs {
		dir := filepath.Dir(d)
		rmap[dir] = d
	}

	return nil
}

func findDumpsDirs() ([]string, error) {
	var dirs []string
	cfg := config.Cfg()

	// Use filepath.WalkDir to traverse the directory
	err := filepath.WalkDir(cfg.Dest, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return fmt.Errorf("error accessing path %s: %w", path, err)
		}

		base := filepath.Base(path)
		if d.IsDir() {
			fmt.Println(path)
		}
		if d.IsDir() && naming.BackupDmpRegex.MatchString(base) {
			dirs = append(dirs, path)
		}
		return nil
	})

	return dirs, err
}
