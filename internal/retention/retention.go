package retention

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
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

	// parsed path meta-info
	datetime time.Time
	host     string
	port     string
	dbname   string
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

func findBackupsToRetain(retainList retainList, retentionPeriod time.Duration, keepCnt int) ([]retainInfo, error) {
	var result []retainInfo
	currentTime := time.Now()

	for k, v := range retainList {

		// (oldest to newest)
		sort.SliceStable(v, func(i, j int) bool {
			dateI := v[i].datetime
			dateJ := v[j].datetime
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
				info, err := os.Stat(elem.path)
				if err != nil {
					return nil, fmt.Errorf("error accessing folder %s: %v", elem.path, err)
				}
				if currentTime.Sub(info.ModTime()) > retentionPeriod {
					result = append(result, elem)
				}
			}
		}

	}

	return result, nil
}

func findAllBackups() (retainList, error) {
	result := make(map[string][]retainInfo)

	backups, err := findDumpsDirs()
	if err != nil {
		return nil, err
	}
	for _, b := range backups {
		key := fmt.Sprintf("%s-%s-%s", b.host, b.port, b.dbname)
		result[key] = append(result[key], b)
	}

	return result, nil
}

func findDumpsDirs() ([]retainInfo, error) {
	var dirs []retainInfo
	cfg := config.Cfg()

	err := filepath.WalkDir(cfg.Dest, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return fmt.Errorf("error accessing path %s: %w", path, err)
		}
		basename := filepath.Base(path)
		if d.IsDir() && path != cfg.Dest && naming.BackupDmpRegex.MatchString(basename) {
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

	basename := filepath.Base(path)
	notBackupDir := fmt.Errorf("not a backup dir: %s", filepath.ToSlash(path))
	submatch := naming.BackupDmpRegex.FindStringSubmatch(basename)

	if len(submatch) != 5 {
		return retainInfo{}, notBackupDir
	}

	date, err := time.Parse(naming.TimestampLayout, submatch[1])
	if err != nil {
		return retainInfo{}, notBackupDir
	}

	return retainInfo{
		absPath:  absPath,
		path:     path,
		basename: basename,

		datetime: date,
		host:     submatch[2],
		port:     submatch[3],
		dbname:   submatch[4],
	}, nil
}
