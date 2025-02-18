package remote

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"

	"gopgdump/config"
)

func SyncLocalWithRemote() error {
	return uploadSftp()
}

func uploadSftp() error {
	cfg := config.Cfg()
	uploadIsEnabled := cfg.Upload.Sftp.Enable
	if !uploadIsEnabled {
		return nil
	}

	sftpUploader, err := NewUploader(SftpUploaderType, cfg.Upload)
	if err != nil {
		return err
	}

	// remote backups
	remoteFiles, err := sftpUploader.ListObjects(cfg.Upload.Sftp.Dest)
	if err != nil {
		return err
	}

	// local backups
	localFiles, err := getAllFilesInDir(cfg.Dest)
	if err != nil {
		return err
	}

	// search index
	relativeMapLocal, err := makeRelativeMap(cfg.Dest, localFiles)
	if err != nil {
		return err
	}
	relativeMapRemote, err := makeRelativeMap(cfg.Upload.Sftp.Dest, remoteFiles)
	if err != nil {
		return err
	}

	// upload on remote
	for localFile := range relativeMapLocal {
		if !relativeMapRemote[localFile] {
			remotePathToUpload := filepath.ToSlash(fmt.Sprintf("%s/%s", cfg.Upload.Sftp.Dest, localFile))
			err := sftpUploader.Upload(filepath.Join(cfg.Dest, localFile), remotePathToUpload)
			if err != nil {
				slog.Error("remote",
					slog.String("action", "upload"),
					slog.String("status", "err"),
					slog.String("err", err.Error()),
				)
			} else {
				slog.Debug("remote",
					slog.String("action", "upload"),
					slog.String("status", "ok"),
					slog.String("path", remotePathToUpload),
				)
			}
		}
	}
	// remove on remote
	for remoteFile := range relativeMapRemote {
		if !relativeMapLocal[remoteFile] {
			remotePathToRm := filepath.ToSlash(fmt.Sprintf("%s/%s", cfg.Upload.Sftp.Dest, remoteFile))
			err := sftpUploader.Delete(remotePathToRm)
			if err != nil {
				slog.Error("remote",
					slog.String("action", "rm"),
					slog.String("status", "err"),
					slog.String("err", err.Error()),
				)
			} else {
				slog.Debug("remote",
					slog.String("action", "rm"),
					slog.String("status", "ok"),
					slog.String("path", remotePathToRm),
				)
			}
		}
	}

	return nil
}

func makeRelativeMap(basepath string, input []string) (map[string]bool, error) {
	basepath = filepath.ToSlash(basepath)
	result := make(map[string]bool)
	for _, f := range input {
		f = filepath.ToSlash(f)
		rel, err := filepath.Rel(basepath, f)
		if err != nil {
			return nil, err
		}
		result[rel] = true
	}
	return result, nil
}

func getAllFilesInDir(localDir string) ([]string, error) {
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
