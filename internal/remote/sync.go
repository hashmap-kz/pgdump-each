package remote

import (
	"log/slog"
	"path/filepath"
	"sync"

	"gopgdump/config"
	"gopgdump/internal/local"
	"gopgdump/internal/naming"
	"gopgdump/internal/remote/uploader"
)

type uploadTask struct {
	localPath  string
	remotePath string
}

func SyncLocalWithRemote() error {
	cfg := config.Cfg()
	if !cfg.Upload.Enable {
		return nil
	}

	// NOTE: !!! adding new remote-storage increase wg cnt !!!
	const numRemotes = 2
	var wg sync.WaitGroup
	errCh := make(chan error, numRemotes) // Buffered channel to avoid blocking
	wg.Add(numRemotes)
	go uploadRoutine(&wg, errCh, uploadSftp)
	go uploadRoutine(&wg, errCh, uploadS3)
	wg.Wait()
	close(errCh)

	for err := range errCh {
		slog.Error("remote", slog.String("sync-error", err.Error()))
	}
	return nil
}

func uploadRoutine(wg *sync.WaitGroup, errCh chan<- error, uploadFunc func() error) {
	defer wg.Done()
	if err := uploadFunc(); err != nil {
		errCh <- err
	}
}

// remotes

func uploadSftp() error {
	var err error

	cfg := config.Cfg()
	sftpConfig := cfg.Upload.Sftp
	if !sftpConfig.Enable {
		return nil
	}

	u, err := uploader.NewUploader(uploader.SftpUploaderType, cfg.Upload)
	if err != nil {
		return err
	}

	err = uploadOnRemote(u)
	err = deleteOnRemote(u)

	return err
}

func uploadS3() error {
	var err error

	cfg := config.Cfg()
	s3Config := cfg.Upload.S3
	if !s3Config.Enable {
		return nil
	}

	u, err := uploader.NewUploader(uploader.S3UploaderType, cfg.Upload)
	if err != nil {
		return nil
	}

	err = uploadOnRemote(u)
	err = deleteOnRemote(u)

	return err
}

// common routine for all remotes

func deleteOnRemote(u uploader.Uploader) error {
	// get remote dirs
	topLevelRemoteDirs, err := u.ListTopLevelDirs(naming.BackupDmpRegex)
	if err != nil {
		return err
	}

	// get local dirs
	topLevelLocalDirs, err := local.ListTopLevelDirs(naming.BackupDmpRegex)
	if err != nil {
		return err
	}

	// remove dirs on remote, that are not present locally
	for remoteDirName := range topLevelRemoteDirs {
		if !topLevelLocalDirs[remoteDirName] {
			err := u.DeleteAll(filepath.ToSlash(remoteDirName))
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
	remoteFiles, err := u.ListObjects()
	if err != nil {
		return nil, err
	}
	localFiles, err := local.ListObjects()
	if err != nil {
		return nil, err
	}

	// relative search index
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
			filesToUploadOnRemote = append(filesToUploadOnRemote, uploadTask{
				localPath:  filepath.Join(cfg.Dest, localFile),
				remotePath: filepath.ToSlash(filepath.Join(dest, localFile)),
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
		result[filepath.ToSlash(rel)] = true
	}
	return result, nil
}
