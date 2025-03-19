package connstr

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCreateConnStr(t *testing.T) {
	tests := []struct {
		name     string
		input    *ConnStr
		expected string
		hasError bool
	}{
		{
			name: "Standard connection string",
			input: &ConnStr{
				Host:     "localhost",
				Port:     5432,
				Username: "user",
				Password: "pass",
				Dbname:   "testdb",
			},
			expected: "postgres://user:pass@localhost:5432/testdb",
			hasError: false,
		},
		{
			name: "Connection string with options",
			input: &ConnStr{
				Host:     "localhost",
				Port:     5432,
				Username: "user",
				Password: "pass",
				Dbname:   "testdb",
				Opts: map[string]string{
					"pool":    "5",
					"sslmode": "disable",
				},
			},
			expected: "postgres://user:pass@localhost:5432/testdb?pool=5&sslmode=disable",
			hasError: false,
		},
		{
			name: "Basebackup mode (no dbname)",
			input: &ConnStr{
				Host:     "127.0.0.1",
				Port:     5432,
				Username: "backup",
				Password: "secure",
			},
			expected: "postgres://backup:secure@127.0.0.1:5432",
			hasError: false,
		},
		{
			name: "Options encoding test",
			input: &ConnStr{
				Host:     "example.com",
				Port:     5432,
				Username: "user",
				Password: "pass",
				Dbname:   "testdb",
				Opts: map[string]string{
					"search_path": "public,schema1",
					"sslmode":     "require",
				},
			},
			expected: "postgres://user:pass@example.com:5432/testdb?search_path=public,schema1&sslmode=require",
			hasError: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result, err := CreateConnStr(tc.input)
			if tc.hasError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.expected, result)
			}
		})
	}
}
