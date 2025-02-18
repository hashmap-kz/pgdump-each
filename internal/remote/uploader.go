package uploader

import (
	"fmt"

	"gopgdump/config"
)

type UploaderType string

var (
	S3UploaderType   UploaderType = "s3"
	SftpUploaderType UploaderType = "sftp"
)

type Uploader interface {
	Upload(localFilePath, remoteFilePath string) error
	ListObjects(path string) ([]string, error)
	Close() error
	GetType() UploaderType
	Delete(path string) error
	DeleteAll(prefix string) error
}

// NewUploader factory method to init uploader, based on its kind and config
func NewUploader(kind UploaderType, config config.UploadConfig) (Uploader, error) {
	var uploader Uploader
	var err error

	switch kind {
	// case S3UploaderType:
	//	uploader, err = NewS3Storage(config)
	//	if err != nil {
	//		return nil, fmt.Errorf("failed to init s3 storage: %w", err)
	//	}
	case SftpUploaderType:
		uploader, err = NewSFTPStorage(config)
		if err != nil {
			return nil, fmt.Errorf("failed to init sftp storage: %w", err)
		}
	}

	return uploader, nil
}
