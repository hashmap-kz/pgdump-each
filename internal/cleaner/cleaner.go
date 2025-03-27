package cleaner

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"

	"gopgdump/internal/common"

	"gopgdump/config"
	"gopgdump/internal/naming"
)

func DropDirtyDirs() error {
	cfg := config.Cfg()

	// check if a destination dir exist
	destinationDirExists, err := common.DirExists(cfg.Dest)
	if err != nil {
		return err
	}
	// destination not exists yet, nothing to do
	if !destinationDirExists {
		return nil
	}

	dirtyDirs, err := findDirtyDirs(naming.BackupDirtyRegex)
	if err != nil {
		return err
	}
	for _, d := range dirtyDirs {
		slog.Info("clean", slog.String("drop-dirty", filepath.ToSlash(d)))
		err := os.RemoveAll(d)
		if err != nil {
			// print warning and continue, don't care about
			slog.Warn("clean",
				slog.String("drop-dirty", filepath.ToSlash(d)),
				slog.String("err", err.Error()),
			)
		}
	}
	return nil
}

func findDirtyDirs(reg *regexp.Regexp) ([]string, error) {
	var dirs []string
	cfg := config.Cfg()

	err := filepath.WalkDir(cfg.Dest, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return fmt.Errorf("error accessing path %s: %w", path, err)
		}
		basename := filepath.Base(path)
		if d.IsDir() && path != cfg.Dest && reg.MatchString(basename) {
			abs, err := filepath.Abs(path)
			if err != nil {
				return err
			}
			dirs = append(dirs, abs)
		}
		return nil
	})

	return dirs, err
}
