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

func TestDatabaseNameRegex(t *testing.T) {
	validCases := []string{
		"mydatabase",
		"test_db123",
		"_db_backup",
		"a123456789_abcdefghij", // Shorter than 63 chars
		"UPPERCASE",             // Uppercase are allowed
	}

	invalidCases := []string{
		"123database", // Starts with a digit
		"db-name",     // Hyphen not allowed
		"waytoolongname_waytoolongname_waytoolongname_waytoolongname_waytoolongname", // Exceeds 63 chars
	}

	for _, tc := range validCases {
		assert.True(t, DatabaseNameRegex.MatchString(tc), "Expected valid match for: "+tc)
	}

	for _, tc := range invalidCases {
		assert.False(t, DatabaseNameRegex.MatchString(tc), "Expected invalid match for: "+tc)
	}
}

func TestBackupRegex(t *testing.T) {
	// Valid test cases
	validCases := map[string][]string{
		"20250217132356--localhost-5432--pg_basebackup":      {"20250217132356", "localhost", "5432", "pg_basebackup"},
		"20240101010101--dbserver-6000--pg_basebackup":       {"20240101010101", "dbserver", "6000", "pg_basebackup"},
		"19991231235959--example.com-1234--pg_basebackup":    {"19991231235959", "example.com", "1234", "pg_basebackup"},
		"20250317124500--backup-server-99999--pg_basebackup": {"20250317124500", "backup-server", "99999", "pg_basebackup"},
	}

	for filename, expectedGroups := range validCases {
		t.Run("Valid:"+filename, func(t *testing.T) {
			matches := BackupClusterRegex.FindStringSubmatch(filename)
			assert.NotNil(t, matches, "Expected a match")
			assert.Len(t, matches, 5, "Expected 5 capture groups")

			// Check each capture group
			for i, expected := range expectedGroups {
				assert.Equal(t, expected, matches[i+1], "Mismatch in capture group")
			}
		})
	}

	// Invalid test cases
	invalidCases := []string{
		"2025021713235--localhost-5432--pg_basebackup",     // Timestamp too short
		"20250217132356--localhost--pg_basebackup",         // Missing port
		"20250217132356--localhost-5432--backup",           // Wrong suffix (should be `pg_basebackup`)
		"20250217132356--localhost-543210--pg_basebackup",  // Port > 5 digits
		"20250217132356--invalid/host-5432--pg_basebackup", // Invalid character in hostname
	}

	for _, filename := range invalidCases {
		t.Run("Invalid:"+filename, func(t *testing.T) {
			matches := BackupClusterRegex.FindStringSubmatch(filename)
			assert.Nil(t, matches, "Expected no match")
		})
	}
}
