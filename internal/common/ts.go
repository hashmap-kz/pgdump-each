package common

import "time"

const (
	TimestampLayout = "20060102150405"
)

var WorkingTimestamp = time.Now().Truncate(time.Second).Format(TimestampLayout)
