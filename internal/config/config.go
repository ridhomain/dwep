package config

import (
	"fmt"
	"os"
	"reflect"
	"strings"
	"time"

	"github.com/spf13/viper"
)

// Config holds all configuration for the service
type Config struct {
	Environment string `mapstructure:"environment"`
	LogLevel    string `mapstructure:"logLevel"`
	Server      struct {
		Port int `mapstructure:"port"`
	} `mapstructure:"server"`
	NATS struct {
		URL                 string             `mapstructure:"url"`
		Realtime            ConsumerNatsConfig `mapstructure:"realtime"`
		Historical          ConsumerNatsConfig `mapstructure:"historical"`
		DLQStream           string             `mapstructure:"dlqStream"`           // Name of the Dead Letter Queue stream
		DLQSubject          string             `mapstructure:"dlqSubject"`          // Base subject for DLQ messages (e.g., v1.dlq)
		DLQWorkers          int                `mapstructure:"dlqWorkers"`          // Number of concurrent DLQ processing workers
		DLQBaseDelayMinutes int                `mapstructure:"dlqBaseDelayMinutes"` // Base delay in minutes for exponential backoff
		DLQMaxDelayMinutes  int                `mapstructure:"dlqMaxDelayMinutes"`  // Max delay in minutes for exponential backoff
		DLQMaxAgeDays       int                `mapstructure:"dlqMaxAgeDays"`       // Retention period for DLQ messages (days)
		DLQMaxDeliver       int                `mapstructure:"dlqMaxDeliver"`       // Max redelivery attempts for DLQ consumer
		DLQAckWait          time.Duration      `mapstructure:"dlqAckWait"`          // Ack wait timeout for DLQ consumer
		DLQMaxAckPending    int                `mapstructure:"dlqMaxAckPending"`    // Max pending ACKs for DLQ consumer
	} `mapstructure:"nats"`
	Database struct {
		PostgresDSN         string `mapstructure:"postgresDSN"`
		PostgresAutoMigrate bool   `mapstructure:"postgresAutoMigrate"`
	} `mapstructure:"database"`
	Company struct {
		Default string `mapstructure:"default"`
		ID      string `mapstructure:"id"`
	} `mapstructure:"company"`
	Metrics struct {
		Enabled bool `mapstructure:"enabled"`
		Port    int  `mapstructure:"port"`
	} `mapstructure:"metrics"`
	WorkerPools struct {
		Onboarding OnboardingWorkerPoolConfig `mapstructure:"onboarding"`
	} `mapstructure:"workerPools"`
}

// OnboardingWorkerPoolConfig holds configuration for the onboarding worker pool
type OnboardingWorkerPoolConfig struct {
	PoolSize   int           `mapstructure:"poolSize"`   // Number of workers
	QueueSize  int           `mapstructure:"queueSize"`  // Task queue buffer size
	MaxBlock   time.Duration `mapstructure:"maxBlock"`   // Max time to block when submitting if queue full
	ExpiryTime time.Duration `mapstructure:"expiryTime"` // Idle worker expiry time
}

// ConsumerNatsConfig holds configuration specific to a NATS consumer
type ConsumerNatsConfig struct {
	MaxAge       int64         `mapstructure:"maxAge"` // max age of messages in day
	Stream       string        `mapstructure:"stream"`
	Consumer     string        `mapstructure:"consumer"` // durable name
	QueueGroup   string        `mapstructure:"group"`
	SubjectList  []string      `mapstructure:"subjectList"`
	MaxDeliver   int           `mapstructure:"maxDeliver"`   // Max delivery attempts before DLQ
	NakBaseDelay time.Duration `mapstructure:"nakBaseDelay"` // Base delay for exponential backoff NAK
	NakMaxDelay  time.Duration `mapstructure:"nakMaxDelay"`  // Maximum delay for exponential backoff NAK
}

// LoadConfig reads configuration from file or environment variables
func LoadConfig(path string) (*Config, error) {
	// Create new viper instance
	v := viper.New()

	// Set defaults
	v.SetDefault("environment", "development")
	v.SetDefault("logLevel", "info")
	v.SetDefault("server.port", 8080)
	v.SetDefault("metrics.enabled", true)
	v.SetDefault("metrics.port", 2112)

	// DLQ Worker Defaults
	v.SetDefault("nats.dlqWorkers", 8)
	v.SetDefault("nats.dlqBaseDelayMinutes", 1)
	v.SetDefault("nats.dlqMaxDelayMinutes", 15)

	// WorkerPools Defaults
	v.SetDefault("workerPools.onboarding.poolSize", 10)
	v.SetDefault("workerPools.onboarding.queueSize", 10000)
	v.SetDefault("workerPools.onboarding.maxBlock", time.Second)   // Default to 1 second block
	v.SetDefault("workerPools.onboarding.expiryTime", time.Minute) // Default to 1 minute expiry

	// Config file settings
	v.SetConfigName("default") // name of config file (without extension)
	v.SetConfigType("yaml")    // REQUIRED if the config file does not have the extension in the name

	// Add lookup paths
	if path != "" {
		v.AddConfigPath(path)
	}
	v.AddConfigPath(".")
	v.AddConfigPath("./internal/config")
	v.AddConfigPath("$HOME/.daisi-wa-events-processor")
	v.AddConfigPath("/etc/daisi-wa-events-processor")

	// Try to read from config file
	if err := v.ReadInConfig(); err != nil {
		// It's ok if config file is not found, we'll use env vars
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			return nil, fmt.Errorf("error reading config file: %w", err)
		}
	}

	// Override with environment variables
	// v.SetEnvPrefix("MES") // will be uppercased automatically
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	// Map environment variables to config fields
	bindEnvs(v, Config{})

	// Read directly from ENV for critical values
	if dsn := os.Getenv("POSTGRES_DSN"); dsn != "" {
		v.Set("database.postgresDSN", dsn)
	}
	if lgLevel := os.Getenv("LOG_LEVEL"); lgLevel != "" {
		v.Set("logLevel", lgLevel)
	}
	if url := os.Getenv("NATS_URL"); url != "" {
		v.Set("nats.url", url)
	}
	if company := os.Getenv("COMPANY_ID"); company != "" {
		v.Set("company.id", company)
	}

	// Unmarshal config
	var config Config
	if err := v.Unmarshal(&config); err != nil {
		return nil, fmt.Errorf("unable to decode config into struct: %w", err)
	}

	return &config, nil
}

// bindEnvs recursively binds environment variables to config struct fields
func bindEnvs(v *viper.Viper, cfg interface{}, parts ...string) {
	ifv := reflect.ValueOf(cfg)
	ift := reflect.TypeOf(cfg)
	for i := 0; i < ift.NumField(); i++ {
		fieldVal := ifv.Field(i)
		fieldType := ift.Field(i)

		// Get the field tag value (mapstructure)
		tag := fieldType.Tag.Get("mapstructure")
		if tag == "" || tag == "-" {
			continue
		}

		// Build the env var path
		path := append(parts, tag)
		key := strings.Join(path, ".")

		// If it's a struct, recursively bind its fields
		if fieldType.Type.Kind() == reflect.Struct {
			bindEnvs(v, fieldVal.Interface(), path...)
			continue
		}

		// Bind the env var
		_ = v.BindEnv(key)
	}
}
