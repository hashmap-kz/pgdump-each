package common

import (
	"os/exec"
	"runtime"

	"gopgdump/config"
)

func GetMaxConcurrency(from int) int {
	if from <= 0 || from > runtime.NumCPU() {
		return config.MaxConcurrencyDefault
	}
	return from
}

func GetExec(configValue, defaultValue string) (string, error) {
	if configValue != "" {
		return exec.LookPath(configValue)
	}
	return exec.LookPath(defaultValue)
}
