package retention

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"gopgdump/config"
	"gopgdump/internal/naming"
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

	for _, d := range dirs {
		_, err := getBackupTimestampFromDirName(d)
		if err != nil {
			return err
		}
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
		if d.IsDir() && naming.BackupDmpRegex.MatchString(base) {
			dirs = append(dirs, path)
		}
		return nil
	})

	return dirs, err
}

func getBackupTimestampFromDirName(path string) (time.Time, error) {
	baseName := filepath.Base(path)

	if !naming.BackupDmpRegex.MatchString(baseName) {
		return time.Time{}, fmt.Errorf("not a backup dir: %s", baseName)
	}

	// 20250217135009--localhost-5432--demo.dmp

	submatch := naming.BackupDmpRegex.FindStringSubmatch(baseName)
	if len(submatch) != 5 {
		return time.Time{}, fmt.Errorf("not a backup dir: %s", baseName)
	}

	date, err := time.Parse(naming.TimestampLayout, submatch[1])
	if err != nil {
		return time.Time{}, fmt.Errorf("not a backup dir: %s", baseName)
	}

	return date, nil
}
