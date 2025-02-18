package remote

import (
	"gopgdump/internal/remote/uploader"

	"gopgdump/config"
)

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
