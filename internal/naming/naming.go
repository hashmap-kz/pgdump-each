package naming

import "regexp"

var (
	BackupDmpRegex = regexp.MustCompile(`^(\d{14})-([a-zA-Z0-9_-]+)\.dmp$`)

	DatabaseNameRegex = regexp.MustCompile(`^[a-z_][a-z0-9_]{0,62}$`)

	// Define the regex for DNS name with required port
	DnsWithPortRegex = regexp.MustCompile(`^(?:(?:[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)\.)*[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?-([0-9]{1,5})$`)
	// ..............................................................................................................................................^
	// we use this pattern for folder names, so the colon was replaced by dash
	// example: 10.40.240.63-30231
)
