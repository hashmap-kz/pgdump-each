package dump

import (
	"context"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

const (
	integrationTestEnv  = "PGDUMP_EACH_INTEGRATION_TESTS_AVAILABLE"
	integrationTestFlag = "0xcafebabe"
)

func TestRunner(t *testing.T) {
	if os.Getenv(integrationTestEnv) != integrationTestFlag {
		t.Log("integration test was skipped due to configuration")
		return
	}

	outputDir := t.TempDir()

	err := RunDumpJobs(context.Background(), &ClusterDumpContext{
		ConnStr:     "postgres://postgres:postgres@localhost:15432/postgres",
		OutputDir:   outputDir,
		ParallelDBS: 3,
	})

	assert.NoError(t, err)
}
