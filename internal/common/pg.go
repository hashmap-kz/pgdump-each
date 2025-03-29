package common

import (
	"context"

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
