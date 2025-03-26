package common

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"text/template"

	"gopgdump/config"
)

func GetMaxConcurrency(from int) int {
	if from <= 0 || from > runtime.NumCPU() {
		return config.MaxConcurrencyDefault
	}
	return from
}

func GetExec(binPath, bin string) (string, error) {
	if binPath != "" {
		return exec.LookPath(filepath.Join(binPath, bin))
	}
	return exec.LookPath(bin)
}

func DirExists(path string) (bool, error) {
	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	return info.IsDir(), nil
}

func ExecTemplate(name, t string, data, funcMap map[string]any) (string, error) {
	var result bytes.Buffer
	tmpl, err := template.New(name).Funcs(funcMap).Parse(t)
	if err != nil {
		return "", err
	}
	err = tmpl.Execute(&result, data)
	if err != nil {
		return "", err
	}
	return result.String(), nil
}
