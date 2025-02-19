package local

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"

	"gopgdump/internal/fio"

	"gopgdump/config"
	"gopgdump/internal/naming"
)

type BackupEntry struct {
	Path       string
	RelPath    string
	AbsPath    string
	BackupInfo naming.BackupInfo
	Files      []BackupFileEntry
}

type BackupFileEntry struct {
	Path     string
	RelPath  string
	AbsPath  string
	Basename string
}

// BackupIndex host+port+dbname=[]backups
type BackupIndex map[string][]BackupEntry

func FindAllBackups() (BackupIndex, error) {
	result := make(map[string][]BackupEntry)

	backups, err := findDumpsDirs(naming.BackupDmpRegex)
	if err != nil {
		return nil, err
	}
	for _, b := range backups {
		// NOTE: key matters in exactly that form
		// by that key retention is performed
		key := fmt.Sprintf("%s-%s-%s", b.BackupInfo.Host, b.BackupInfo.Port, b.BackupInfo.Dbname)
		result[key] = append(result[key], b)
	}

	return result, nil
}

func ListObjects() ([]string, error) {
	index, err := FindAllBackups()
	if err != nil {
		return nil, err
	}
	paths := []string{}
	for _, v := range index {
		for _, be := range v {
			for _, fe := range be.Files {
				paths = append(paths, fe.Path)
			}
		}
	}
	return paths, nil
}

func ListTopLevelDirs(reg *regexp.Regexp) ([]string, error) {
	var dirs []string
	cfg := config.Cfg()

	entries, err := os.ReadDir(cfg.Dest)
	if err != nil {
		return nil, err
	}
	for _, entry := range entries {
		if entry.IsDir() && reg.MatchString(entry.Name()) {
			dirs = append(dirs, entry.Name())
		}
	}
	return dirs, nil
}

func findDumpsDirs(reg *regexp.Regexp) ([]BackupEntry, error) {
	var dirs []BackupEntry
	cfg := config.Cfg()

	err := filepath.WalkDir(cfg.Dest, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return fmt.Errorf("error accessing path %s: %w", path, err)
		}
		basename := filepath.Base(path)
		if d.IsDir() && path != cfg.Dest && reg.MatchString(basename) {
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

func parseBackupInfo(path string) (BackupEntry, error) {
	// 20250217135009--localhost-5432--demo.dmp

	cfg := config.Cfg()

	absPath, err := filepath.Abs(path)
	if err != nil {
		return BackupEntry{}, err
	}

	relPath, err := filepath.Rel(cfg.Dest, path)
	if err != nil {
		return BackupEntry{}, err
	}

	backupInfo, err := naming.ParseDmpRegex(path)
	if err != nil {
		return BackupEntry{}, err
	}

	filesForBackup, err := getFilesForBackup(path)
	if err != nil {
		return BackupEntry{}, err
	}

	return BackupEntry{
		Path:       filepath.ToSlash(path),
		AbsPath:    filepath.ToSlash(absPath),
		RelPath:    filepath.ToSlash(relPath),
		BackupInfo: backupInfo,
		Files:      filesForBackup,
	}, nil
}

func getFilesForBackup(path string) ([]BackupFileEntry, error) {
	cfg := config.Cfg()

	filesInDir, err := fio.GetAllFilesInDir(path)
	if err != nil {
		return nil, err
	}

	files := []BackupFileEntry{}
	for _, f := range filesInDir {
		absPathFile, err := filepath.Abs(f)
		if err != nil {
			return nil, err
		}
		relPathFile, err := filepath.Rel(cfg.Dest, f)
		if err != nil {
			return nil, err
		}
		files = append(files, BackupFileEntry{
			Path:     filepath.ToSlash(f),
			RelPath:  filepath.ToSlash(relPathFile),
			AbsPath:  filepath.ToSlash(absPathFile),
			Basename: filepath.Base(f),
		})
	}
	return files, nil
}
