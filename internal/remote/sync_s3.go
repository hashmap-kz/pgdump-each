package remote

import (
	"gopgdump/internal/remote/uploader"

	"gopgdump/config"
)

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
