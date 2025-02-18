package remote

import (
	"fmt"

	"gopgdump/internal/remote/uploader"

	"gopgdump/internal/naming"

	"gopgdump/config"
)

func uploadS3() error {
	cfg := config.Cfg()
	s3Config := cfg.Upload.S3
	if !s3Config.Enable {
		return nil
	}

	s3Uploader, err := uploader.NewUploader(uploader.S3UploaderType, cfg.Upload)
	if err != nil {
		return nil
	}

	path := s3Config.Bucket
	if s3Config.Prefix != "" {
		path = fmt.Sprintf("%s/%s", s3Config.Bucket, s3Config.Prefix)
	}

	dirs, err := s3Uploader.ListTopLevelDirs(path, naming.BackupDmpRegex)
	if err != nil {
		return err
	}
	fmt.Println(dirs)

	return nil
}
