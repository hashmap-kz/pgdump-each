package local

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"

	"gopgdump/config"
	"gopgdump/internal/naming"
)

type BackupEntry struct {
	Path       string
	AbsPath    string
	BackupInfo naming.BackupInfo
}

// BackupIndex host+port+dbname=[]backups
type BackupIndex map[string][]BackupEntry

func FindAllBackups() (BackupIndex, error) {
	result := make(map[string][]BackupEntry)

	backups, err := findDumpsDirs(naming.BackupDmpRegex)
	if err != nil {
		return nil, err
	}
	for _, b := range backups {
		key := fmt.Sprintf("%s-%s-%s", b.BackupInfo.Host, b.BackupInfo.Port, b.BackupInfo.Dbname)
		result[key] = append(result[key], b)
	}

	return result, nil
}

func ListTopLevelDirs(reg *regexp.Regexp) ([]string, error) {
	var dirs []string
	cfg := config.Cfg()

	entries, err := os.ReadDir(cfg.Dest)
	if err != nil {
		return nil, err
	}
	for _, entry := range entries {
		if entry.IsDir() && reg.MatchString(entry.Name()) {
			dirs = append(dirs, entry.Name())
		}
	}
	return dirs, nil
}

func findDumpsDirs(reg *regexp.Regexp) ([]BackupEntry, error) {
	var dirs []BackupEntry
	cfg := config.Cfg()

	err := filepath.WalkDir(cfg.Dest, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return fmt.Errorf("error accessing path %s: %w", path, err)
		}
		basename := filepath.Base(path)
		if d.IsDir() && path != cfg.Dest && reg.MatchString(basename) {
			ri, err := parseBackupInfo(path)
			if err != nil {
				return err
			}
			dirs = append(dirs, ri)
		}
		return nil
	})

	return dirs, err
}

func parseBackupInfo(path string) (BackupEntry, error) {
	// 20250217135009--localhost-5432--demo.dmp

	absPath, err := filepath.Abs(path)
	if err != nil {
		return BackupEntry{}, err
	}

	backupInfo, err := naming.ParseDmpRegex(path)
	if err != nil {
		return BackupEntry{}, err
	}

	return BackupEntry{
		Path:       path,
		AbsPath:    absPath,
		BackupInfo: backupInfo,
	}, nil
}
