package remote

import (
	"gopgdump/internal/remote/uploader"

	"gopgdump/config"
)

func uploadSftp() error {
	cfg := config.Cfg()
	sftpConfig := cfg.Upload.Sftp
	if !sftpConfig.Enable {
		return nil
	}

	u, err := uploader.NewUploader(uploader.SftpUploaderType, cfg.Upload)
	if err != nil {
		return err
	}

	_ = uploadOnRemote(u)
	_ = deleteOnRemote(u)

	return nil
}
