package remote

import (
	"fmt"
	"log/slog"
	"path/filepath"
	"sync"

	"gopgdump/config"
	"gopgdump/internal/fio"
	"gopgdump/internal/local"
	"gopgdump/internal/naming"
	"gopgdump/internal/remote/uploader"
)

type uploadTask struct {
	localPath  string
	remotePath string
}

func SyncLocalWithRemote() error {
	var err error

	cfg := config.Cfg()
	if !cfg.Upload.Enable {
		return nil
	}

	err = uploadSftp()
	if err != nil {
		slog.Error("remote", slog.String("sync-error", err.Error()))
	}

	err = uploadS3()
	if err != nil {
		slog.Error("remote", slog.String("sync-error", err.Error()))
	}

	return err
}

// common routine for all remotes

func deleteOnRemote(u uploader.Uploader) error {
	cfg := config.Cfg()
	dest := getDest(u)

	// get remote dirs
	topLevelRemoteDirs, err := u.ListTopLevelDirs(dest, naming.BackupDmpRegex)
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
			err := u.DeleteAll(filepath.ToSlash(filepath.Join(dest, remoteDirName)))
			if err != nil {
				slog.Error("remote",
					slog.String("action", "rm -rf"),
					slog.String("storage", string(u.GetType())),
					slog.String("remote-path", remoteDirName),
					slog.String("err", err.Error()),
				)
			} else {
				slog.Debug("remote",
					slog.String("action", "rm -rf"),
					slog.String("storage", string(u.GetType())),
					slog.String("remote-path", remoteDirName),
					slog.String("status", "ok"),
				)
			}
		}
	}

	return nil
}

func getDest(u uploader.Uploader) string {
	cfg := config.Cfg()

	if u.GetType() == uploader.S3UploaderType {
		// bucket-level
		return ""
	}

	if u.GetType() == uploader.SftpUploaderType {
		return cfg.Upload.Sftp.Dest
	}

	return ""
}

func getFilesToUpload(u uploader.Uploader) ([]uploadTask, error) {
	cfg := config.Cfg()
	dest := getDest(u)

	// local and remote backups
	remoteFiles, err := u.ListObjects(dest)
	if err != nil {
		return nil, err
	}
	// here should be ONLY files from *.dmp dirs, NOT *.dirty ones
	localFiles, err := getLocalFiles()
	if err != nil {
		return nil, err
	}

	// search index
	relativeMapLocal, err := makeRelativeMap(cfg.Dest, localFiles)
	if err != nil {
		return nil, err
	}
	relativeMapRemote, err := makeRelativeMap(dest, remoteFiles)
	if err != nil {
		return nil, err
	}

	filesToUploadOnRemote := []uploadTask{}
	for localFile := range relativeMapLocal {
		if !relativeMapRemote[localFile] {
			// make actual paths from relatives (we compare relatives, but working with actual)
			localFilePath := filepath.Join(cfg.Dest, localFile)

			// dest may be empty for s3, when the prefix is not set, and all objects are stored at the bucket level
			remoteFilePath := filepath.ToSlash(fmt.Sprintf("%s/%s", dest, localFile))
			if dest == "" {
				remoteFilePath = filepath.ToSlash(localFile)
			}

			filesToUploadOnRemote = append(filesToUploadOnRemote, uploadTask{
				localPath:  localFilePath,
				remotePath: remoteFilePath,
			})
		}
	}

	return filesToUploadOnRemote, nil
}

func uploadOnRemote(u uploader.Uploader) error {
	// prepare tasks
	filesToUploadOnRemote, err := getFilesToUpload(u)
	if err != nil {
		return err
	}

	// upload concurrently
	workerCount := 8
	uploadTasksCh := make(chan uploadTask, len(filesToUploadOnRemote))
	var wg sync.WaitGroup
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go uploadWorker(i, u, uploadTasksCh, &wg)
	}
	for _, db := range filesToUploadOnRemote {
		uploadTasksCh <- db
	}
	close(uploadTasksCh)
	wg.Wait()

	return nil
}

func uploadWorker(worker int, u uploader.Uploader, tasks <-chan uploadTask, wg *sync.WaitGroup) {
	defer wg.Done()

	for t := range tasks {
		err := u.Upload(t.localPath, t.remotePath)
		if err != nil {
			slog.Error("remote",
				slog.String("action", "upload"),
				slog.String("storage", string(u.GetType())),
				slog.Int("worker", worker),
				slog.String("local-path", filepath.ToSlash(t.localPath)),
				slog.String("remote-path", filepath.ToSlash(t.remotePath)),
				slog.String("err", err.Error()),
			)
		} else {
			slog.Debug("remote",
				slog.String("action", "upload"),
				slog.String("storage", string(u.GetType())),
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
