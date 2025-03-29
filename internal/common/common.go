package common

import (
	"os/exec"
	"path/filepath"
)

func GetExec(binPath, bin string) (string, error) {
	if binPath != "" {
		return exec.LookPath(filepath.Join(binPath, bin))
	}
	return exec.LookPath(bin)
}
