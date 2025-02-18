package remote

import (
	"fmt"
	"log/slog"
	"path/filepath"
	"sync"

	"gopgdump/internal/naming"

	"gopgdump/internal/fio"
	"gopgdump/internal/local"

	"gopgdump/config"
)

type uploadTask struct {
	localPath  string
	remotePath string
}

func SyncLocalWithRemote() error {
	return uploadSftp()
}

func uploadSftp() error {
	cfg := config.Cfg()
	sftpConfig := cfg.Upload.Sftp
	if !sftpConfig.Enable {
		return nil
	}

	// init sftp client
	sftpUploader, err := NewUploader(SftpUploaderType, cfg.Upload)
	if err != nil {
		return err
	}

	_ = uploadOnRemote(sftpUploader)
	_ = deleteOnRemote(sftpUploader)

	return nil
}

func deleteOnRemote(sftpUploader Uploader) error {
	cfg := config.Cfg()
	sftpConfig := cfg.Upload.Sftp

	// get remote dirs
	topLevelRemoteDirs, err := sftpUploader.ListTopLevelDirs(sftpConfig.Dest, naming.BackupDmpRegex)
	if err != nil {
		return err
	}

	// get local dirs
	topLevelLocalDirs, err := fio.ListTopLevelDirs(cfg.Dest, naming.BackupDmpRegex)
	if err != nil {
		return err
	}
	localIndex := map[string]bool{}
	for _, f := range topLevelLocalDirs {
		localIndex[f] = true
	}

	// remove dirs on remote, that are not present locally
	for _, remoteDirName := range topLevelRemoteDirs {
		if !localIndex[remoteDirName] {
			err := sftpUploader.DeleteAll(filepath.ToSlash(filepath.Join(sftpConfig.Dest, remoteDirName)))
			if err != nil {
				slog.Error("remote",
					slog.String("action", "rm -rf"),
					slog.String("remote-path", remoteDirName),
					slog.String("err", err.Error()),
				)
			} else {
				slog.Debug("remote",
					slog.String("action", "rm -rf"),
					slog.String("remote-path", remoteDirName),
					slog.String("status", "ok"),
				)
			}
		}
	}

	return nil
}

func uploadOnRemote(sftpUploader Uploader) error {
	cfg := config.Cfg()
	sftpConfig := cfg.Upload.Sftp

	// local and remote backups
	remoteFiles, err := sftpUploader.ListObjects(sftpConfig.Dest)
	if err != nil {
		return err
	}
	// here should be ONLY files from *.dmp dirs, NOT *.dirty ones
	localFiles, err := getLocalFiles()
	if err != nil {
		return err
	}

	// search index
	relativeMapLocal, err := makeRelativeMap(cfg.Dest, localFiles)
	if err != nil {
		return err
	}
	relativeMapRemote, err := makeRelativeMap(sftpConfig.Dest, remoteFiles)
	if err != nil {
		return err
	}

	filesToUploadOnRemote := []uploadTask{}
	for localFile := range relativeMapLocal {
		if !relativeMapRemote[localFile] {
			// make actual paths from relatives (we compare relatives, but working with actual)
			localFilePath := filepath.Join(cfg.Dest, localFile)
			remoteFilePath := filepath.ToSlash(fmt.Sprintf("%s/%s", sftpConfig.Dest, localFile))

			filesToUploadOnRemote = append(filesToUploadOnRemote, uploadTask{
				localPath:  localFilePath,
				remotePath: remoteFilePath,
			})
		}
	}

	// upload concurrently
	workerCount := 8
	uploadTasksCh := make(chan uploadTask, len(filesToUploadOnRemote))
	var wg sync.WaitGroup
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go uploadWorker(i, sftpUploader, uploadTasksCh, &wg)
	}
	for _, db := range filesToUploadOnRemote {
		uploadTasksCh <- db
	}
	close(uploadTasksCh)
	wg.Wait()

	return nil
}

func uploadWorker(worker int, uploader Uploader, tasks <-chan uploadTask, wg *sync.WaitGroup) {
	defer wg.Done()

	for t := range tasks {
		err := uploader.Upload(t.localPath, t.remotePath)
		if err != nil {
			slog.Error("remote",
				slog.String("action", "upload"),
				slog.Int("worker", worker),
				slog.String("local-path", filepath.ToSlash(t.localPath)),
				slog.String("remote-path", filepath.ToSlash(t.remotePath)),
				slog.String("err", err.Error()),
			)
		} else {
			slog.Debug("remote",
				slog.String("action", "upload"),
				slog.Int("worker", worker),
				slog.String("local-path", filepath.ToSlash(t.localPath)),
				slog.String("remote-path", filepath.ToSlash(t.remotePath)),
				slog.String("status", "ok"),
			)
		}
	}
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

func getLocalFiles() ([]string, error) {
	backups, err := local.FindAllBackups()
	if err != nil {
		return nil, err
	}
	localFiles := []string{}
	for _, v := range backups {
		for _, b := range v {
			allFilesInDir, err := fio.GetAllFilesInDir(b.Path)
			if err != nil {
				return nil, err
			}
			localFiles = append(localFiles, allFilesInDir...)
		}
	}
	return localFiles, nil
}
