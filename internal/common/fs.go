package common

import (
	"os"
	"path/filepath"
	"strings"
)

func GetDumpsInDir(path string) ([]*DBInfo, error) {
	entries, err := os.ReadDir(path)
	if err != nil {
		return nil, err
	}

	var results []*DBInfo
	for _, entry := range entries {
		if entry.IsDir() && strings.HasSuffix(entry.Name(), ".dmp") {
			dirPath := filepath.Join(path, entry.Name())
			size, err := dirSize(dirPath)
			if err != nil {
				return nil, err
			}
			results = append(results, &DBInfo{
				DatName:   dirPath,
				SizeBytes: size,
			})
		}
	}
	return results, nil
}

// dirSize walks a directory and returns the total size of all files
func dirSize(path string) (int64, error) {
	var total int64
	err := filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return err // Can't access file
		}
		if !info.IsDir() {
			total += info.Size()
		}
		return nil
	})
	return total, err
}
