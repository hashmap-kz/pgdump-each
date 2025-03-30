package common

import (
	"context"
	"encoding/json"
	"runtime"

	"github.com/jackc/pgx/v5"
)

type DBInfo struct {
	DatName   string `json:"datname,omitempty"`
	SizeBytes int64  `json:"size_bytes,omitempty"`
}

func GetDatabases(ctx context.Context, connStr string) ([]DBInfo, error) {
	conn, err := pgx.Connect(ctx, connStr)
	if err != nil {
		return nil, err
	}
	defer conn.Close(ctx)

	rows, err := conn.Query(ctx, `
	select d.datname                   as datname,
		   pg_database_size(d.datname) as size_bytes
	from pg_database d
	where d.datistemplate = false
	  and d.datallowconn
	  and d.datname <> 'postgres';
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var scannedEntities []DBInfo
	for rows.Next() {
		var scannedEntity DBInfo
		err := rows.Scan(
			&scannedEntity.DatName,
			&scannedEntity.SizeBytes,
		)
		if err != nil {
			return nil, err
		}
		scannedEntities = append(scannedEntities, scannedEntity)
	}
	if rows.Err() != nil {
		return nil, rows.Err()
	}
	return scannedEntities, nil
}

func GetJobsWeights(ctx context.Context, dpmInfos []DBInfo, connStr string) (map[string]int, error) {
	conn, err := pgx.Connect(ctx, connStr)
	if err != nil {
		return nil, err
	}
	defer conn.Close(ctx)

	totalJobs := runtime.NumCPU() - 1
	if totalJobs < 2 {
		totalJobs = 1
	}

	jsonData, err := json.Marshal(dpmInfos)
	if err != nil {
		return nil, err
	}

	q := `
	with db_sizes as (select datname, size_bytes
					  from jsonb_to_recordset($1::jsonb) AS t(datname text, size_bytes int)),
		 totals as (select sum(size_bytes) as total_size
					from db_sizes)
	select d.datname as datname,
		   greatest(1,
					round(d.size_bytes::numeric / t.total_size * $2)
		   )::int as jobs
	from db_sizes d,
		 totals t
	order by d.size_bytes desc
`
	rows, err := conn.Query(ctx, q, jsonData, totalJobs)
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
