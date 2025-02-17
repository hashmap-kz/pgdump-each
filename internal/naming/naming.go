package naming

import "regexp"

const (
	TimestampLayout  = "20060102150405"
	PgDumpPath       = "dump"
	PgBasebackupPath = "base"
	PgConfPath       = "conf"
)

var (
	// BackupDmpRegex defines a regex for filter dumps in  target dir
	// layout: datetime--host-port--dbname.dmp
	// example: 20250217134506--10.40.240.165-30201--vault.dmp
	BackupDmpRegex = regexp.MustCompile(`^(\d{14})--([a-zA-Z0-9.-]+)-(\d{1,5})--([a-zA-Z_][a-zA-Z0-9_]{0,62})\.dmp$`)

	DatabaseNameRegex = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]{0,62}$`)
)
