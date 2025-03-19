package connstr

import (
	"fmt"
	"strings"
)

type ConnStr struct {
	Host     string
	Port     int
	Username string
	Password string
	Dbname   string
	Opts     map[string]string
}

func CreateConnStr(db *ConnStr) (string, error) {
	connStr := fmt.Sprintf("postgres://%s:%s@%s:%d/%s", db.Username, db.Password, db.Host, db.Port, db.Dbname)
	if db.Dbname == "" {
		// basebackup
		connStr = fmt.Sprintf("postgres://%s:%s@%s:%d", db.Username, db.Password, db.Host, db.Port)
	}
	if len(db.Opts) > 0 {
		query := []string{}
		for key, value := range db.Opts {
			query = append(query, fmt.Sprintf("%s=%s", key, value))
		}
		connStr = connStr + "?" + strings.Join(query, "&")
	}
	return connStr, nil
}
