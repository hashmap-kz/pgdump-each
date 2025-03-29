package common

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"
	"strings"
)

const ChecksumsFileName = "checksums.txt"

func WriteChecksumsFile(stageDir string) error {
	f, err := os.Create(filepath.Join(stageDir, ChecksumsFileName))
	if err != nil {
		return err
	}
	defer f.Close()

	checksums, err := getChecksums(stageDir)
	if err != nil {
		return err
	}

	for k, v := range checksums {
		_, err := f.WriteString(fmt.Sprintf("%s  %s\n", v, k))
		if err != nil {
			return err
		}
	}
	return nil
}

func CompareChecksums(root string) error {
	expected, err := scanChecksumsFromFile(filepath.Join(root, ChecksumsFileName))
	if err != nil {
		return err
	}
	current, err := getChecksums(root)
	if err != nil {
		return err
	}
	if len(current) != len(expected) {
		return fmt.Errorf("checksums directory content mismatch")
	}
	for k, v := range expected {
		curVal, ok := current[k]
		if !ok {
			return fmt.Errorf("checksums mismatch, stray file: %s", k)
		}
		if v != curVal {
			return fmt.Errorf("checksums value mismatch for file: %s", k)
		}
	}
	return nil
}

func computeChecksum(filePath string, hasher hash.Hash) (string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return "", err
	}
	defer file.Close()

	hasher.Reset()
	if _, err := io.Copy(hasher, file); err != nil {
		return "", err
	}

	return hex.EncodeToString(hasher.Sum(nil)), nil
}

func getChecksums(root string) (map[string]string, error) {
	checksums := make(map[string]string)
	err := filepath.Walk(root, func(path string, info os.FileInfo, walkErr error) error {
		if walkErr != nil {
			return fmt.Errorf("error encountered during dir traverse %v: %w", path, walkErr)
		}
		if !info.IsDir() && (filepath.Base(path) != ChecksumsFileName) {
			checksum, err := computeChecksum(path, sha256.New())
			if err != nil {
				return err
			}
			relPath, err := filepath.Rel(root, path)
			if err != nil {
				return err
			}
			checksums[filepath.ToSlash(relPath)] = checksum
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return checksums, nil
}

func scanChecksumsFromFile(checksumsFilePath string) (map[string]string, error) {
	file, err := os.Open(checksumsFilePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	checksums := make(map[string]string)
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.SplitN(line, "  ", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid line: %s", line)
		}
		expectedHash := parts[0]
		relPath := parts[1]
		checksums[filepath.ToSlash(relPath)] = expectedHash
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return checksums, nil
}
