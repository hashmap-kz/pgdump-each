package restore

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"gopgdump/internal/common"
)

func RunRestoreJobs(ctx context.Context, connStr, inputPath string) error {
	databases, err := common.GetDatabases(ctx, connStr)
	if err != nil {
		return err
	}
	if len(databases) > 0 {
		return fmt.Errorf("cannot restore on non-empty cluster")
	}
	dirs, err := listTopLevelDirs(inputPath)
	if err != nil {
		return err
	}
	if len(dirs) == 0 {
		return fmt.Errorf("no dumps were found")
	}

	return nil
}

func listTopLevelDirs(path string) ([]string, error) {
	entries, err := os.ReadDir(path)
	if err != nil {
		return nil, err
	}

	var dirs []string
	for _, entry := range entries {
		if entry.IsDir() {
			dirs = append(dirs, filepath.Join(path, entry.Name()))
		}
	}
	return dirs, nil
}
