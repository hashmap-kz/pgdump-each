package naming

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBackupDmpRegex(t *testing.T) {
	validCases := []string{
		"20250217132356--localhost-5432--demo.dmp",      // Valid
		"20240101010101--dbserver-123-5433--backup.dmp", // Valid
		"19991231235959--example.com-6000--testdb.dmp",  // Valid (hostname with dots)
		"20250217132356--localhost-54321--demo.dmp",     // Valid (port max 5 digits)
		"20250217132356--hostname-99999--demo.dmp",      // Valid (port max 5 digits)
		"20250217132356--hostname-5432--DBNAME.dmp",     // Valid (case insensitive)
	}

	invalidCases := []string{
		"2025021713235--localhost-5432--demo.dmp",   // Invalid (timestamp length issue)
		"20250217132356--invalid/host-5432--db.dmp", // Invalid (hostname with `/`)
		"20250217132356--server--demo.dmp",          // Invalid (missing port)
	}

	for _, tc := range validCases {
		assert.Equal(t, 5, len(BackupDmpRegex.FindStringSubmatch(tc)))
		assert.True(t, BackupDmpRegex.MatchString(tc), "Expected valid match for: "+tc)
	}

	for _, tc := range invalidCases {
		assert.False(t, BackupDmpRegex.MatchString(tc), "Expected invalid match for: "+tc)
	}
}
