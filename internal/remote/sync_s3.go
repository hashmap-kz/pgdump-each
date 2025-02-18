package remote

import (
	"gopgdump/internal/remote/uploader"

	"gopgdump/config"
)

func uploadS3() error {
	cfg := config.Cfg()
	s3Config := cfg.Upload.S3
	if !s3Config.Enable {
		return nil
	}

	u, err := uploader.NewUploader(uploader.S3UploaderType, cfg.Upload)
	if err != nil {
		return nil
	}

	_ = uploadOnRemote(u)
	_ = deleteOnRemote(u)

	return nil
}
