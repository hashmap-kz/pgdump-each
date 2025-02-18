package fio

import (
	"os"
	"path/filepath"
	"regexp"
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

func ListTopLevelDirs(path string, reg *regexp.Regexp) ([]string, error) {
	var dirs []string

	// Read the directory contents
	entries, err := os.ReadDir(path)
	if err != nil {
		return nil, err
	}

	// Filter and collect only directories
	for _, entry := range entries {
		if entry.IsDir() && reg.MatchString(entry.Name()) {
			dirs = append(dirs, entry.Name())
		}
	}

	return dirs, nil
}
