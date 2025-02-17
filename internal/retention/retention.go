package retention

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"time"

	"gopgdump/config"
	"gopgdump/internal/naming"
)

type retainInfo struct {
	// path info
	absPath  string
	path     string
	basename string
	modTime  time.Time

	// parsed path meta-info
	backupInfo naming.BackupInfo
}

// host+port+dbname=[]backups
type retainList map[string][]retainInfo

func PurgeOldDirs() error {
	cfg := config.Cfg()
	if !cfg.Retention.Enable {
		return nil
	}

	retentionPeriod, err := time.ParseDuration(cfg.Retention.Period)
	if err != nil {
		return err
	}

	keepCnt := cfg.Retention.KeepLast
	if keepCnt <= 0 {
		keepCnt = 0
	}

	allBackups, err := findAllBackups()
	if err != nil {
		return err
	}

	backupsToRetain, err := findBackupsToRetain(allBackups, retentionPeriod, keepCnt)
	if err != nil {
		return err
	}

	err = dropBackups(backupsToRetain)
	if err != nil {
		return err
	}

	return nil
}

func findAllBackups() (retainList, error) {
	result := make(map[string][]retainInfo)

	backups, err := findDumpsDirs(naming.BackupDmpRegex)
	if err != nil {
		return nil, err
	}
	for _, b := range backups {
		key := fmt.Sprintf("%s-%s-%s", b.backupInfo.Host, b.backupInfo.Port, b.backupInfo.Dbname)
		result[key] = append(result[key], b)
	}

	return result, nil
}

func findBackupsToRetain(retainList retainList, retentionPeriod time.Duration, keepCnt int) ([]retainInfo, error) {
	var result []retainInfo
	currentTime := time.Now()

	for k, v := range retainList {

		// (oldest to newest)
		sort.SliceStable(v, func(i, j int) bool {
			dateI := v[i].backupInfo.Datetime
			dateJ := v[j].backupInfo.Datetime
			return dateI.Before(dateJ)
		})

		toDelete := len(v) - keepCnt
		if toDelete <= 0 {
			slog.Info("purge",
				slog.String("key", k),
				slog.String("msg", "nothing to purge"),
			)
		} else {
			for i := 0; i < toDelete; i++ {
				elem := v[i]
				if currentTime.Sub(elem.modTime) > retentionPeriod {
					result = append(result, elem)
				}
			}
		}

	}

	return result, nil
}

func findDumpsDirs(reg *regexp.Regexp) ([]retainInfo, error) {
	var dirs []retainInfo
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

func parseBackupInfo(path string) (retainInfo, error) {
	// 20250217135009--localhost-5432--demo.dmp

	absPath, err := filepath.Abs(path)
	if err != nil {
		return retainInfo{}, err
	}

	fileInfo, err := os.Stat(path)
	if err != nil {
		return retainInfo{}, fmt.Errorf("error accessing folder %s: %v", path, err)
	}

	backupInfo, err := naming.ParseDmpRegex(path)
	if err != nil {
		return retainInfo{}, err
	}

	return retainInfo{
		absPath:    absPath,
		path:       path,
		basename:   filepath.Base(path),
		modTime:    fileInfo.ModTime(),
		backupInfo: backupInfo,
	}, nil
}

func dropBackups(ri []retainInfo) error {
	for _, elem := range ri {
		slog.Info("purge",
			slog.String("msg", "rm -rf"),
			slog.String("path", filepath.ToSlash(elem.path)),
		)
		err := os.RemoveAll(elem.absPath)
		if err != nil {
			return err
		}
	}
	return nil
}
