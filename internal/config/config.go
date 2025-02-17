package config

type BackupConfig struct {
	// postgres://username:password@host:port/dbname?connect_timeout=5&sslmode=disable
	Host     string
	Port     string
	Username string
	Password string
	Dbname   string
	Opts     map[string]string

	// optional filters
	Schemas        []string
	ExcludeSchemas []string
	Tables         []string
	ExcludeTables  []string
}
