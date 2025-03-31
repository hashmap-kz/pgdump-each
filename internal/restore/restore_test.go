package restore

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/hashmap-kz/pgdump-each/internal/common"
	"github.com/hashmap-kz/pgdump-each/internal/dump"
	"github.com/stretchr/testify/assert"
)

func TestRestoreStage(t *testing.T) {
	if os.Getenv(common.IntegrationTestEnv) != common.IntegrationTestFlag {
		t.Log("integration test was skipped due to configuration")
		return
	}

	outputDir := t.TempDir()

	// perform dump

	err := dump.RunDumpJobs(context.Background(), &dump.ClusterDumpContext{
		ConnStr:     "postgres://postgres:postgres@localhost:15432/postgres",
		OutputDir:   outputDir,
		ParallelDBS: 3,
	})
	assert.NoError(t, err)

	// check expected output content

	expectedPath := filepath.Join(outputDir, fmt.Sprintf("%s.dmp", dump.WorkingTimestamp))
	dumps, err := common.GetDumpsInDir(expectedPath)
	assert.NoError(t, err)
	assert.Equal(t, 7, len(dumps))

	// trying to restore

	err = RunRestoreJobs(context.Background(), &ClusterRestoreContext{
		ConnStr:     "postgres://postgres:postgres@localhost:15433/postgres",
		InputDir:    expectedPath,
		ParallelDBS: 5,
	})

	assert.NoError(t, err)
}
