package remote

import (
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

	// sync
	for localFile := range relativeMapLocal {
		if !relativeMapRemote[localFile] {
			// TODO: upload to remote
		}
	}
	for remoteFile := range relativeMapRemote {
		if !relativeMapLocal[remoteFile] {
			// TODO: delete from remote
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
