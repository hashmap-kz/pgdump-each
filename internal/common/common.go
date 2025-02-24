package common

import (
	"runtime"

	"gopgdump/config"
)

func GetMaxConcurrency(from int) int {
	if from <= 0 || from > runtime.NumCPU() {
		return config.MaxConcurrencyDefault
	}
	return from
}
