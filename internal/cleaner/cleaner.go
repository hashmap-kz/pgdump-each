package cleaner

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"

	"gopgdump/config"
	"gopgdump/internal/naming"
)

func DropDirtyDirs() error {
	dirtyDirs, err := findDirtyDirs(naming.BackupDirtyRegex)
	if err != nil {
		return err
	}
	for _, d := range dirtyDirs {
		slog.Info("clean", slog.String("drop-dirty", filepath.ToSlash(d)))
		err := os.RemoveAll(d)
		if err != nil {
			return err
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
