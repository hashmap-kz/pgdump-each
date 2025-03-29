package common

import (
	"context"
	"runtime"

	"github.com/jackc/pgx/v5"
)

func GetDatabases(ctx context.Context, connStr string) ([]string, error) {
	conn, err := pgx.Connect(ctx, connStr)
	if err != nil {
		return nil, err
	}
	defer conn.Close(ctx)

	rows, err := conn.Query(ctx, `
	SELECT 	datname FROM pg_database 
	WHERE 	datistemplate = false
	AND 	datname <> 'postgres'
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var databases []string
	for rows.Next() {
		var db string
		if err := rows.Scan(&db); err != nil {
			return nil, err
		}
		databases = append(databases, db)
	}
	if rows.Err() != nil {
		return nil, err
	}
	return databases, nil
}

func GetJobsWeights(ctx context.Context, connStr string) (map[string]int, error) {
	conn, err := pgx.Connect(ctx, connStr)
	if err != nil {
		return nil, err
	}
	defer conn.Close(ctx)

	totalJobs := runtime.NumCPU() - 1
	if totalJobs < 2 {
		totalJobs = 1
	}

	q := `
	with db_sizes as (select datname,
							 pg_database_size(datname) as size_bytes
					  from pg_database
					  where datistemplate = false
						and datname <> 'postgres'),
		 totals as (select sum(size_bytes) as total_size
					from db_sizes)
	select d.datname as datname,
		   greatest(1,
					round(d.size_bytes::numeric / t.total_size * $1)
		   )::int as jobs
	from db_sizes d,
		 totals t
	order by d.size_bytes desc
`
	rows, err := conn.Query(ctx, q, totalJobs)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	scannedEntities := make(map[string]int)
	for rows.Next() {
		var dbname string
		var jobs int
		err := rows.Scan(&dbname, &jobs)
		if err != nil {
			return nil, err
		}
		scannedEntities[dbname] = jobs
	}
	if rows.Err() != nil {
		return nil, rows.Err()
	}
	return scannedEntities, nil
}
