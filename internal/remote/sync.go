package remote

import (
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

	_, err = sftpUploader.ListObjects(cfg.Upload.Sftp.Dest)
	if err != nil {
		return err
	}
	return nil
}
