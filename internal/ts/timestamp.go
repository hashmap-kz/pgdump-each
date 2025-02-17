package ts

import (
	"time"

	"gopgdump/internal/naming"
)

// WorkingTimestamp holds 'base working' timestamp for backup/retain tasks
// remember timestamp for all backups
// it is easy to sort/retain when all backups in one iteration use one timestamp
var WorkingTimestamp = time.Now().UTC().Truncate(time.Second).Format(naming.TimestampLayout)
