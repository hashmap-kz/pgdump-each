package common

import (
	"fmt"
)

type ParallelSettings struct {
	DBWorkers  int
	PGDumpJobs int
}

// CalculateParallelSettings - adjusts number of CPUs available per-database dump, and per each pg_dump --jobs param
// totalCPUs added as a parameter, for unit-testing
// totalCPUs := runtime.NumCPU()
func CalculateParallelSettings(numDatabases, totalCPUs int) (*ParallelSettings, error) {
	if numDatabases <= 0 {
		return nil, fmt.Errorf("zero or negative size of DBS")
	}

	if totalCPUs < 2 {
		// fallback if system is very constrained
		return &ParallelSettings{
			DBWorkers:  1,
			PGDumpJobs: 1,
		}, nil
	}

	usableCPUs := totalCPUs - 1 // leave 1 for system
	if usableCPUs < 1 {
		usableCPUs = 1
	}

	// Let’s say we want at least 1 job per dump
	maxWorkers := xMin(numDatabases, usableCPUs)

	// Distribute CPUs across workers, leave 1 job per dump minimum
	pgDumpJobs := usableCPUs / maxWorkers
	if pgDumpJobs < 1 {
		pgDumpJobs = 1
	}

	return &ParallelSettings{
		DBWorkers:  maxWorkers,
		PGDumpJobs: pgDumpJobs,
	}, nil
}

func xMin(a, b int) int {
	if a < b {
		return a
	}
	return b
}
