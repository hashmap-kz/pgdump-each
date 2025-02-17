package ts

import (
	"time"

	"gopgdump/internal/naming"
)

// remember timestamp for all backups
// it is easy to sort/retain when all backups in one iteration use one timestamp
var BackupTimestamp = time.Now().Format(naming.TimestampLayout)
