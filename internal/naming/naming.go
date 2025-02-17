package naming

import (
	"fmt"
	"path/filepath"
	"regexp"
	"time"
)

const (
	TimestampLayout  = "20060102150405"
	PgDumpPath       = "dump"
	PgBasebackupPath = "base"
	PgConfPath       = "conf"

	baseDmpPattern = `^(\d{14})--([a-zA-Z0-9.-]+)-(\d{1,5})--([a-zA-Z_][a-zA-Z0-9_]{0,62})`
)

var (
	// BackupDmpRegex defines a regex for filter dumps in  target dir
	// layout: datetime--host-port--dbname.dmp
	// example: 20250217134506--10.40.240.165-30201--vault.dmp
	BackupDmpRegex   = regexp.MustCompile(baseDmpPattern + `\.dmp$`)
	BackupDirtyRegex = regexp.MustCompile(baseDmpPattern + `\.dirty$`)

	DatabaseNameRegex = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]{0,62}$`)
)

type BackupInfo struct {
	Datetime time.Time
	Host     string
	Port     string
	Dbname   string
}

func ParseDmpRegex(path string) (BackupInfo, error) {
	notBackupDir := fmt.Errorf("not a backup dir: %s", filepath.ToSlash(path))

	basename := filepath.Base(path)
	regMatch := BackupDmpRegex.FindStringSubmatch(basename)

	if len(regMatch) != 5 {
		return BackupInfo{}, notBackupDir
	}

	dateTimeFromDirNamePattern, err := time.Parse(TimestampLayout, regMatch[1])
	if err != nil {
		return BackupInfo{}, notBackupDir
	}

	return BackupInfo{
		Datetime: dateTimeFromDirNamePattern,
		Host:     regMatch[2],
		Port:     regMatch[3],
		Dbname:   regMatch[4],
	}, nil
}
