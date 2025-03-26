package retention

import (
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"time"

	"gopgdump/internal/common"

	"gopgdump/internal/local"

	"gopgdump/config"
)

func PurgeOldDirs() error {
	cfg := config.Cfg()
	if !cfg.Retention.Enable {
		return nil
	}

	// check if a destination dir exist
	destinationDirExists, err := common.DirExists(cfg.Dest)
	if err != nil {
		return err
	}
	// destination not exists yet, nothing to do
	if !destinationDirExists {
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

	allBackups, err := local.FindAllBackups()
	if err != nil {
		return err
	}

	backupsToRetain, err := filterBackupsToRetain(allBackups, retentionPeriod, keepCnt)
	if err != nil {
		return err
	}

	err = dropBackups(backupsToRetain)
	if err != nil {
		return err
	}

	return nil
}

//nolint:unparam
func filterBackupsToRetain(retainList local.BackupIndex, retentionPeriod time.Duration, keepCnt int) ([]local.BackupEntry, error) {
	var result []local.BackupEntry

	for k, v := range retainList {
		// (oldest to newest)
		sort.SliceStable(v, func(i, j int) bool {
			dateI := v[i].BackupInfo.DatetimeUTC
			dateJ := v[j].BackupInfo.DatetimeUTC
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
				elapsed := time.Since(elem.BackupInfo.DatetimeUTC).Truncate(time.Second)
				if elapsed > retentionPeriod {
					result = append(result, elem)
				}
			}
		}
	}
	return result, nil
}

func dropBackups(ri []local.BackupEntry) error {
	for i := 0; i < len(ri); i++ { // rangeValCopy
		elem := ri[i]
		slog.Info("purge",
			slog.String("action", "rm -rf"),
			slog.String("storage", "local"),
			slog.String("path", filepath.ToSlash(elem.AbsPath)),
		)
		err := os.RemoveAll(elem.AbsPath)
		if err != nil {
			return err
		}
	}
	return nil
}
