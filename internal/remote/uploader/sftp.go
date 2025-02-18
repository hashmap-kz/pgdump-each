package uploader

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"time"

	"gopgdump/config"

	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
)

type SFTPStorage struct {
	sshClient  *ssh.Client
	sftpClient *sftp.Client
	config     config.UploadSftpConfig
}

var _ Uploader = &SFTPStorage{}

func (s *SFTPStorage) GetType() UploaderType {
	return SftpUploaderType
}

// NewSFTPStorage creates an SFTP client using passphrase-protected private key authentication
func NewSFTPStorage(c config.UploadConfig) (*SFTPStorage, error) {
	var err error
	sftpConfig := c.Sftp

	// Load the private key from file, or read from the property as a string
	key, err := os.ReadFile(sftpConfig.PkeyPath)
	if err != nil {
		return nil, fmt.Errorf("unable to read private key: %w", err)
	}

	// Parse the private key with passphrase
	var signer ssh.Signer
	if sftpConfig.Passphrase != "" {
		signer, err = ssh.ParsePrivateKeyWithPassphrase(key, []byte(sftpConfig.Passphrase))
		if err != nil {
			return nil, fmt.Errorf("unable to parse private key with passphrase: %w", err)
		}
	} else {
		signer, err = ssh.ParsePrivateKey(key)
		if err != nil {
			return nil, fmt.Errorf("unable to parse private key: %w", err)
		}
	}

	// Setup SSH configuration
	sshConfig := &ssh.ClientConfig{
		User: sftpConfig.User,
		Auth: []ssh.AuthMethod{
			ssh.PublicKeys(signer),
		},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		Timeout:         5 * time.Second,
	}

	// Establish the SSH connection
	addr := fmt.Sprintf("%s:%s", sftpConfig.Host, sftpConfig.Port)
	conn, err := ssh.Dial("tcp", addr, sshConfig)
	if err != nil {
		return nil, fmt.Errorf("unable to connect to SFTP server: %w", err)
	}

	// Create an SFTP sftpClient over the SSH connection
	client, err := sftp.NewClient(conn)
	if err != nil {
		return nil, fmt.Errorf("unable to create SFTP sftpClient: %w", err)
	}

	return &SFTPStorage{
		sshClient:  conn,
		sftpClient: client,
		config:     sftpConfig,
	}, nil
}

func (s *SFTPStorage) Client() *sftp.Client {
	return s.sftpClient
}

// Upload uploads a file to the SFTP server
func (s *SFTPStorage) Upload(localPath, remotePath string) error {
	localFile, err := os.Open(localPath)
	if err != nil {
		return fmt.Errorf("failed to open local file: %w", err)
	}
	defer localFile.Close()

	// Create remote directories if they don't exist
	remoteDirPath := filepath.ToSlash(filepath.Dir(remotePath))
	if err := s.Client().MkdirAll(remoteDirPath); err != nil {
		return fmt.Errorf("failed to create remote directory %s: %v", remoteDirPath, err)
	}

	remoteFile, err := s.sftpClient.Create(remotePath)
	if err != nil {
		return fmt.Errorf("failed to create remote file: %w", err)
	}
	defer remoteFile.Close()

	_, err = io.Copy(remoteFile, localFile)
	if err != nil {
		return fmt.Errorf("failed to copy file content: %w", err)
	}

	return nil
}

func sftpDirExists(client *sftp.Client, path string) (bool, error) {
	info, err := client.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil // Directory does not exist
		}
		return false, err // Other error
	}
	return info.IsDir(), nil // Return true if it's a directory
}

// ListObjects recursively lists all files and directories under the specified remote directory
func (s *SFTPStorage) ListObjects(path string) ([]string, error) {
	objects := []string{}

	exists, err := sftpDirExists(s.sftpClient, path)
	if err != nil {
		return nil, err
	}
	if !exists {
		return objects, nil
	}

	walker := s.sftpClient.Walk(path)
	for walker.Step() {
		if err := walker.Err(); err != nil {
			return nil, fmt.Errorf("error walking directory: %w", err)
		}
		if walker.Stat().IsDir() {
			continue
		}
		// Collect the full path of the current file/directory
		if walker.Path() != path {
			objects = append(objects, walker.Path())
		}
	}

	return objects, nil
}

func (s *SFTPStorage) ListTopLevelDirs(path string, reg *regexp.Regexp) ([]string, error) {
	var dirs []string

	// Read the directory contents
	entries, err := s.sftpClient.ReadDir(path)
	if err != nil {
		return nil, err
	}

	// Filter and collect only directories
	for _, entry := range entries {
		if entry.IsDir() && reg.MatchString(entry.Name()) {
			dirs = append(dirs, entry.Name())
		}
	}

	return dirs, nil
}

func (s *SFTPStorage) Delete(path string) error {
	err := s.sftpClient.Remove(path)
	if err != nil {
		return fmt.Errorf("failed to remove file: %s, %w", path, err)
	}
	return nil
}

func (s *SFTPStorage) DeleteAll(path string) error {
	err := s.sftpClient.RemoveAll(path)
	if err != nil {
		return fmt.Errorf("failed to remove file: %s, %w", path, err)
	}
	return nil
}

// Close closes the SFTP connection
func (s *SFTPStorage) Close() error {
	var err error = nil
	if s.sftpClient != nil {
		err = s.sftpClient.Close()
	}
	if s.sshClient != nil {
		err = s.sshClient.Close()
	}
	return err
}
