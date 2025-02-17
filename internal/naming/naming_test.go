package naming

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestHostFolderRegex(t *testing.T) {
	testCasesCorrect := []string{
		"example.com-80",      // Valid
		"sub.example.com-443", // Valid
		"localhost-3000",      // Valid
		"127.0.0.1-22",        // Valid
	}
	testCasesIncorrect := []string{
		"example.com",        // Invalid (missing port)
		"example.com-",       // Invalid (missing port number)
		"example.com-655361", // Invalid (port out of range)
		"-example.com-22",    // Invalid (invalid DNS name)
	}

	for _, test := range testCasesCorrect {
		if !DnsWithPortRegex.MatchString(test) {
			fmt.Println("fail: ", test)
		}
		assert.True(t, DnsWithPortRegex.MatchString(test))
	}
	for _, test := range testCasesIncorrect {
		if DnsWithPortRegex.MatchString(test) {
			fmt.Println("fail: ", test)
		}
		assert.False(t, DnsWithPortRegex.MatchString(test))
	}
}

func TestBackupDmpRegex(t *testing.T) {
	validCases := []string{
		"20250217110721-dbname.dmp",
		"20240101010101-test_db.dmp",
		"19991231235959-my_backup-123.dmp",
	}

	invalidCases := []string{
		"202502171107-dbname.dmp",    // Missing digits
		"abc20250217110721-db.dmp",   // Extra characters before timestamp
		"20250217110721-.dmp",        // Missing dbname
		"20250217110721-db.name.dmp", // Invalid character (dot)
		"20250217110721-db/dump.dmp", // Invalid character (slash)
	}

	for _, tc := range validCases {
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
	}

	invalidCases := []string{
		"123database", // Starts with a digit
		"UPPERCASE",   // Uppercase not allowed
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
