package retention

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetBackupTimestampFromDirName(t *testing.T) {
	validCases := map[string]string{
		"/backup/20250217135009--localhost-5432--demo.dmp": "2025-02-17 13:50:09",
		"20240101010101--db-server-6000--backup.dmp":       "2024-01-01 01:01:01",
		"/tmp/19991231235959--example.com-5432--data.dmp":  "1999-12-31 23:59:59",
	}

	for path, expectedTime := range validCases {
		t.Run("Valid:"+path, func(t *testing.T) {
			result, err := getBackupTimestampFromDirName(path)
			assert.NoError(t, err, "Expected no error for valid filename")
			assert.Equal(t, expectedTime, result.Format("2006-01-02 15:04:05"))
		})
	}

	invalidCases := []string{
		"invalid_file.dmp",                           // Incorrect format
		"202502171350--localhost-5432--demo.dmp",     // Incorrect timestamp
		"20250217135009--localhost--demo.dmp",        // Missing port
		"20250217135009--localhost-5432--.dmp",       // Missing db name
		"20250217135009--localhost-999999--demo.dmp", // Port too long
	}

	for _, path := range invalidCases {
		t.Run("Invalid:"+path, func(t *testing.T) {
			_, err := getBackupTimestampFromDirName(path)
			assert.Error(t, err, "Expected error for invalid filename")
		})
	}
}
