package backup

type ResultInfo struct {
	Host   string
	Port   int
	Dbname string
	Mode   string // pg_dump, pg_basebackup
	Err    error
}
