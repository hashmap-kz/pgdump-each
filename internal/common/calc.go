package common

type ParallelSettings struct {
	DBWorkers  int
	PGDumpJobs int
}

// TODO: it should be smart enough to decide resources.
// For instance: you have 10 databases, 9 of them with size 100Mi, and the 10th one with the size 100Gi
// In that case it's better to use as more `--jobs` (pg_dump opt) as possible, and use only one or two workers per-database.
//
// Another example: you have 10 databases, 10 of them with size 100-500Mi.
// In that case it's better to utilize as more db-workers as possible, and using only 1 or 2 `--jobs`
//

// CalculateParallelSettings - adjusts number of CPUs available per-database dump, and per each pg_dump --jobs param
// totalCPUs added as a parameter, for unit-testing
// totalCPUs := runtime.NumCPU()
func CalculateParallelSettings(_, _ int) (*ParallelSettings, error) {
	return &ParallelSettings{
		DBWorkers:  3,
		PGDumpJobs: 4,
	}, nil
}
