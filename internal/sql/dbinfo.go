package sql

import (
	"context"
	"fmt"

	"gopgdump/internal/connstr"

	"github.com/jackc/pgx/v5"
	_ "github.com/jackc/pgx/v5"
)

type DbInfo struct {
	ServerVersionNum int `json:"server_version_num"`

	// configs
	PostgresqlConf string `json:"postgresql_conf"`
	PgHbaConf      string `json:"pg_hba_conf"`

	// internal
	connectedAsSuperUser bool
}

func SelectHostInfo(connInfo connstr.ConnStr) (*DbInfo, error) {
	ctx := context.Background()

	connStr, err := connstr.CreateConnStr(connInfo)
	if err != nil {
		return nil, err
	}

	conn, err := newPgConn(ctx, connStr)
	if err != nil {
		return nil, err
	}
	defer conn.Close(ctx)

	query := `
		select (select rolsuper
				from pg_roles
				where rolname = current_user)      as connected_as_superuser,
		
			   (select setting::int
				from pg_settings
				where name = 'server_version_num') as server_version_num,
		
			   (select pg_read_file(setting, 0, (pg_stat_file(setting)).size)
				from pg_settings
				where name = 'config_file')        as postgresql_conf,
		
			   (select pg_read_file(setting, 0, (pg_stat_file(setting)).size)
				from pg_settings
				where name = 'hba_file')           as pg_hba_conf
	`

	var info DbInfo
	err = conn.QueryRow(ctx, query).Scan(
		&info.connectedAsSuperUser,
		&info.ServerVersionNum,
		&info.PostgresqlConf,
		&info.PgHbaConf,
	)
	if err != nil {
		return nil, err
	}

	if !info.connectedAsSuperUser {
		return nil, fmt.Errorf("required superuser privileges")
	}

	return &info, nil
}

func newPgConn(ctx context.Context, connStr string) (*pgx.Conn, error) {
	return pgx.Connect(ctx, connStr)
}
