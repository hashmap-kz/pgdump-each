package config

import (
	"fmt"
	"log"
	"os"
	"strings"
	"sync"

	"gopgdump/pkg/xnet"

	"sigs.k8s.io/yaml"
)

var (
	once   sync.Once
	config *Config
)

const (
	PgDumpJobsDefault     = 4
	MaxConcurrencyDefault = 2
)

type Config struct {
	Dest          string
	Dump          PgDumpsConfig
	Base          PgBaseBackupsConfig
	Retention     RetentionConfig
	Logger        LoggerConfig
	Notify        NotifyConfig
	PrintDumpLogs bool
}

type PgDumpsConfig struct {
	Enable         bool
	MaxConcurrency int
	Databases      []*PgDumpDatabase
}

type PgBaseBackupsConfig struct {
	Enable         bool
	Compress       bool
	MaxConcurrency int
	Clusters       []*PgBaseBackupCluster
}

type PgBaseBackupCluster struct {
	Host     string
	Port     int
	Username string
	Password string
	Opts     map[string]string
}

type PgDumpDatabase struct {
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

	Jobs int
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

// notifier

type NotifyConfig struct {
	Enable   bool
	Telegram NotifyTelegramConfig
}

type NotifyTelegramConfig struct {
	Enable bool
	Token  string
	ChatID string
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
		checkConfigHard()
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
		checkConfigHard()
	})

	return config
}

// check everything that needs to be set, etc...
func checkConfigHard() {
	checkNoDuplicateAmongHosts()
}

func checkNoDuplicateAmongHosts() {
	// must not be duplicates: host+port+dbname
	m := map[string]string{}
	for i := 0; i < len(config.Dump.Databases); i++ { // rangeValCopy
		db := config.Dump.Databases[i]
		ips, err := xnet.LookupIP4Addresses(db.Host)
		if err != nil {
			log.Fatal(err)
		}
		key := fmt.Sprintf("%s;%d;%s", strings.Join(ips, ";"), db.Port, db.Dbname)
		if _, ok := m[key]; ok {
			log.Fatalf("found duplicate: host=%s port=%d dbname=%s", db.Host, db.Port, db.Dbname)
		}
		m[key] = key
	}

	// must not be duplicates: host+port
	m = map[string]string{}
	for _, db := range config.Base.Clusters {
		ips, err := xnet.LookupIP4Addresses(db.Host)
		if err != nil {
			log.Fatal(err)
		}
		key := fmt.Sprintf("%s;%d", strings.Join(ips, ";"), db.Port)
		if _, ok := m[key]; ok {
			log.Fatalf("found duplicate: host=%s port=%d", db.Host, db.Port)
		}
		m[key] = key
	}
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
