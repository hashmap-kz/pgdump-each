package remote

import (
	"context"
	"fmt"
	"log/slog"
	"path/filepath"

	"gopgdump/internal/common"

	"gopgdump/internal/fio"

	"github.com/hashmap-kz/workerfn/pkg/concur"

	"gopgdump/config"
	"gopgdump/internal/local"
	"gopgdump/internal/naming"
	"gopgdump/internal/remote/uploader"
)

type uploadTask struct {
	localPath      string
	remotePath     string
	remoteUploader uploader.Uploader
}

type remoteStorageTask struct {
	fn func() error
}

func SyncLocalWithRemote() error {
	cfg := config.Cfg()
	if !cfg.Upload.Enable {
		return nil
	}

	// concurrently run tasks for all remotes at once

	uploaderFn := func(_ context.Context, r remoteStorageTask) error {
		return r.fn()
	}
	tasks := []remoteStorageTask{
		{fn: uploadS3},
		{fn: uploadSftp},
	}
	errors := concur.ProcessConcurrentlyWithLimit(
		context.Background(),
		len(tasks),
		tasks,
		uploaderFn,
	)
	if len(errors) != 0 {
		return fmt.Errorf("%s", errors[0].Error())
	}
	return nil
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
	defer u.Close()

	err = uploadOnRemote(u)
	err = deleteOnRemote(u)

	if cfg.Upload.CheckTotalCntAndSizeAfterUpload {
		err = calcTotals(u)
	}

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
		return err
	}
	defer u.Close()

	err = uploadOnRemote(u)
	err = deleteOnRemote(u)

	if cfg.Upload.CheckTotalCntAndSizeAfterUpload {
		err = calcTotals(u)
	}

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

func getFilesToUpload(u uploader.Uploader) ([]uploadTask, error) {
	cfg := config.Cfg()
	dest := u.GetDest()

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
				localPath:      filepath.Join(cfg.Dest, localFile),
				remotePath:     filepath.ToSlash(filepath.Join(dest, localFile)),
				remoteUploader: u,
			})
		}
	}

	return filesToUploadOnRemote, nil
}

func uploadOnRemote(u uploader.Uploader) error {
	cfg := config.Cfg()
	filesToUploadOnRemote, err := getFilesToUpload(u)
	if err != nil {
		return err
	}

	errors := concur.ProcessConcurrentlyWithLimit(
		context.Background(),
		common.GetMaxConcurrency(cfg.Upload.MaxConcurrency),
		filesToUploadOnRemote,
		uploadWorker,
	)

	if len(errors) != 0 {
		return fmt.Errorf("upload failed for: %s", u.GetType())
	}
	return nil
}

func uploadWorker(_ context.Context, t uploadTask) error {
	err := t.remoteUploader.Upload(t.localPath, t.remotePath)
	if err != nil {
		slog.Error("remote",
			slog.String("action", "upload"),
			slog.String("storage", string(t.remoteUploader.GetType())),
			slog.String("local-path", filepath.ToSlash(t.localPath)),
			slog.String("remote-path", filepath.ToSlash(t.remotePath)),
			slog.String("err", err.Error()),
		)
		return fmt.Errorf("upload failed. remote: %s, path: %s",
			string(t.remoteUploader.GetType()),
			filepath.ToSlash(t.remotePath),
		)
	}

	slog.Debug("remote",
		slog.String("action", "upload"),
		slog.String("storage", string(t.remoteUploader.GetType())),
		slog.String("local-path", filepath.ToSlash(t.localPath)),
		slog.String("remote-path", filepath.ToSlash(t.remotePath)),
		slog.String("status", "ok"),
	)
	return nil
}

func makeRelativeMap(basepath string, input []fio.FileRepr) (map[string]bool, error) {
	basepath = filepath.ToSlash(basepath)
	result := make(map[string]bool)
	for _, f := range input {
		rel, err := filepath.Rel(basepath, filepath.ToSlash(f.Path))
		if err != nil {
			return nil, err
		}
		result[filepath.ToSlash(rel)] = true
	}
	return result, nil
}

func calcTotals(u uploader.Uploader) error {
	remoteFiles, err := u.ListObjects()
	if err != nil {
		return err
	}
	localFiles, err := local.ListObjects()
	if err != nil {
		return err
	}

	if len(remoteFiles) != len(localFiles) {
		return fmt.Errorf("remote/local count mismatch")
	}

	calcTotalSize := func(files []fio.FileRepr) int64 {
		var s int64
		for _, f := range files {
			s += f.Size
		}
		return s
	}
	if calcTotalSize(remoteFiles) != calcTotalSize(localFiles) {
		return fmt.Errorf("remote/local size mismatch")
	}

	return nil
}
