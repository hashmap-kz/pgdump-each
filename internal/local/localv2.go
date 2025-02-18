package local

import (
	"os"
	"path/filepath"

	"gopgdump/config"
	"gopgdump/internal/fio"
	"gopgdump/internal/naming"
)

type BackupFileV2 struct {
	RelPath string
	AbsPath string
}

type BackupEntryV2 struct {
	RelPath    string
	AbsPath    string
	BackupInfo naming.BackupInfo
	Files      []BackupFileV2
}

type BackupIndexV2 map[string]BackupEntryV2

func FindAllBackupsV2() (BackupIndexV2, error) {
	cfg := config.Cfg()
	result := make(map[string]BackupEntryV2)

	entries, err := os.ReadDir(cfg.Dest)
	if err != nil {
		return nil, err
	}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		if !naming.BackupDmpRegex.MatchString(e.Name()) {
			continue
		}
		backupInfoV2, err := parseBackupInfoV2(filepath.Join(cfg.Dest, e.Name()))
		if err != nil {
			return nil, err
		}
		result[e.Name()] = backupInfoV2
	}

	return result, nil
}

func parseBackupInfoV2(path string) (BackupEntryV2, error) {
	// 20250217135009--localhost-5432--demo.dmp

	absPath, err := filepath.Abs(path)
	if err != nil {
		return BackupEntryV2{}, err
	}

	backupInfo, err := naming.ParseDmpRegex(path)
	if err != nil {
		return BackupEntryV2{}, err
	}

	fileNames, err := fio.GetAllFilesInDir(path)
	if err != nil {
		return BackupEntryV2{}, err
	}

	files := []BackupFileV2{}
	for _, f := range fileNames {
		fileAbsPath, err := filepath.Abs(f)
		if err != nil {
			return BackupEntryV2{}, err
		}
		files = append(files, BackupFileV2{
			RelPath: f,
			AbsPath: fileAbsPath,
		})
	}

	return BackupEntryV2{
		RelPath:    path,
		AbsPath:    absPath,
		BackupInfo: backupInfo,
		Files:      files,
	}, nil
}
