package dump

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/hashmap-kz/pgdump-each/internal/xutil"
	"github.com/stretchr/testify/assert"
)

func TestDumpStage(t *testing.T) {
	if os.Getenv(xutil.IntegrationTestEnv) != xutil.IntegrationTestFlag {
		t.Log("integration test was skipped due to configuration")
		return
	}

	outputDir := t.TempDir()

	// perform dump

	err := RunDumpJobs(context.Background(), &ClusterDumpContext{
		ConnStr:     "postgres://postgres:postgres@localhost:15432/postgres",
		OutputDir:   outputDir,
		ParallelDBS: 3,
	})
	assert.NoError(t, err)

	// check expected output content

	expectedPath := filepath.Join(outputDir, fmt.Sprintf("%s.dmp", WorkingTimestamp))
	dumps, err := xutil.GetDumpsInDir(expectedPath)
	assert.NoError(t, err)
	assert.Equal(t, 7, len(dumps))

	// expecting checksums and globals

	expectedFiles := []string{
		xutil.ChecksumsFileName,
		GlobalsFileName,
	}
	for _, expFile := range expectedFiles {
		path := filepath.Join(expectedPath, expFile)
		_, err = os.Stat(path)
		assert.NoError(t, err, "expected file to exist: %s", path)
	}
}
