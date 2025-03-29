package common_test

import (
	"testing"

	"gopgdump/internal/common"

	"github.com/stretchr/testify/assert"
)

func TestCalculateParallelSettings(t *testing.T) {
	tests := []struct {
		name         string
		numDatabases int
		mockCPUs     int
		wantWorkers  int
		wantJobs     int
		expectError  bool
	}{
		{"1 DB, 8 CPUs", 1, 8, 1, 7, false},
		{"2 DBs, 8 CPUs", 2, 8, 2, 3, false},
		{"4 DBs, 8 CPUs", 4, 8, 4, 1, false},
		{"8 DBs, 8 CPUs", 8, 8, 7, 1, false},
		{"16 DBs, 16 CPUs", 16, 16, 15, 1, false},
		{"Low CPU (1), 1 DB", 1, 1, 1, 1, false},
		{"Low CPU (1), 5 DBs", 5, 1, 1, 1, false},
		{"Invalid: 0 DBs", 0, 8, 0, 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := common.CalculateParallelSettings(tt.numDatabases, tt.mockCPUs)
			if tt.expectError {
				assert.Error(t, err)
				assert.Nil(t, result)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.wantWorkers, result.DBWorkers)
				assert.Equal(t, tt.wantJobs, result.PGDumpJobs)
			}
		})
	}
}
