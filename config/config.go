package config

import (
	"log"
	"os"
	"sync"

	"sigs.k8s.io/yaml"
)

var (
	once   sync.Once
	config *Config
)

type Config struct {
	Dest      string
	Dump      PgDumpsConfig
	Base      PgBaseBackupsConfig
	Retention RetentionConfig

	Logger    LoggerConfig
	PrintLogs bool
}

type PgDumpsConfig struct {
	Jobs int
	DBS  []PgDumpDatabaseConfig
}

type PgBaseBackupsConfig struct {
	Compress bool
	DBS      []PgBaseBackupDatabaseConfig
}

type PgBaseBackupDatabaseConfig struct {
	Host     string
	Port     int
	Username string
	Password string
	Opts     map[string]string
}

type PgDumpDatabaseConfig struct {
	// postgres://username:password@host:port/dbname?connect_timeout=5&sslmode=disable
	Host     string
	Port     int
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

type RetentionConfig struct {
	Enable   bool
	Period   string // duration
	KeepLast int
}

type LoggerConfig struct {
	Format string
	Level  string
}

// LoadConfigFromFile unmarshal file into config struct
func LoadConfigFromFile(filename string) *Config {
	once.Do(func() {
		content, err := os.ReadFile(filename)
		if err != nil {
			log.Fatal(err)
		}

		content = expandEnvVars(content)

		var cfg Config
		err = yaml.Unmarshal(content, &cfg)
		if err != nil {
			log.Fatal(err)
		}
		config = &cfg

		// TODO: later
		//errors := validateStruct(config)
		//if len(errors) > 0 {
		//	for _, e := range errors {
		//		log.Printf("%v", e)
		//	}
		//	log.Fatalln("config-file is not completely set")
		//}
	})

	return config
}

// LoadConfig unmarshal raw data into config struct
func LoadConfig(content []byte) *Config {
	once.Do(func() {
		content = expandEnvVars(content)

		var cfg Config
		err := yaml.Unmarshal(content, &cfg)
		if err != nil {
			log.Fatal(err)
		}
		config = &cfg
	})

	return config
}

func expandEnvVars(buf []byte) []byte {
	s := string(buf)
	e := os.ExpandEnv(s)
	return []byte(e)
}

func Cfg() *Config {
	if config == nil {
		log.Fatal("config was not loaded in main")
	}
	return config
}
