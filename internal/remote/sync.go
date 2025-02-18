package remote

type uploadTask struct {
	localPath  string
	remotePath string
}

func SyncLocalWithRemote() error {
	// skip errors
	_ = uploadSftp()
	_ = uploadS3()
	return nil
}
