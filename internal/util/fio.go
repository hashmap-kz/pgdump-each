package util

import (
	"os"
	"path/filepath"
	"sort"
)

func GetAllFilesInDir(localDir string) ([]string, error) {
	var err error
	result := []string{}

	err = filepath.Walk(localDir, func(localPath string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		result = append(result, filepath.ToSlash(localPath))
		return nil
	})
	if err != nil {
		return nil, err
	}

	sort.Strings(result)
	return result, nil
}
