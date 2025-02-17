package util

import (
	"fmt"
	"net/url"

	"gopgdump/config"
)

func CreateConnStr(db config.PgDumpDatabaseConfig) (string, error) {
	connStr := fmt.Sprintf("postgres://%s:%s@%s:%d/%s", db.Username, db.Password, db.Host, db.Port, db.Dbname)
	if len(db.Opts) > 0 {
		query := url.Values{}
		for key, value := range db.Opts {
			query.Set(key, value)
		}
		connStr = connStr + "?" + query.Encode()
	}
	parse, err := url.Parse(connStr)
	if err != nil {
		return "", err
	}
	return parse.String(), nil
}

func CreateConnStrBasebackup(db config.PgBaseBackupDatabaseConfig) (string, error) {
	connStr := fmt.Sprintf("postgres://%s:%s@%s:%d", db.Username, db.Password, db.Host, db.Port)
	if len(db.Opts) > 0 {
		query := url.Values{}
		for key, value := range db.Opts {
			query.Set(key, value)
		}
		connStr = connStr + "?" + query.Encode()
	}
	parse, err := url.Parse(connStr)
	if err != nil {
		return "", err
	}
	return parse.String(), nil
}
