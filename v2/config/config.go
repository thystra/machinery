package config

import (
	"crypto/tls"
	"time"
)

const DefaultResultsExpireIn = 3600

var (
	defaultCnf = &Config{
		Broker:          "redis://localhost:6379",
		DefaultQueue:    "machinery_tasks",
		ResultBackend:   "redis://localhost:6379",
		ResultsExpireIn: DefaultResultsExpireIn,
		Redis: &RedisConfig{
			MaxIdle:                3,
			IdleTimeout:            240,
			ReadTimeout:            15,
			WriteTimeout:           15,
			ConnectTimeout:         15,
			NormalTasksPollPeriod:  1000,
			DelayedTasksPollPeriod: 500,
			ClaimLeaseDuration:     120000,
			ClaimRenewInterval:     30000,
			ClaimRecoveryPeriod:    1000,
			ClaimRecoveryBatchSize: 100,
			ClaimKeyPrefix:         "machinery:claims",
		},
	}
	reloadDelay = 10 * time.Second
)

// Config contains core and Redis-specific settings retained by this fork.
type Config struct {
	Broker                  string       `yaml:"broker" envconfig:"BROKER"`
	Lock                    string       `yaml:"lock" envconfig:"LOCK"`
	MultipleBrokerSeparator string       `yaml:"multiple_broker_separator" envconfig:"MULTIPLE_BROKEN_SEPARATOR"`
	DefaultQueue            string       `yaml:"default_queue" envconfig:"DEFAULT_QUEUE"`
	ResultBackend           string       `yaml:"result_backend" envconfig:"RESULT_BACKEND"`
	ResultsExpireIn         int          `yaml:"results_expire_in" envconfig:"RESULTS_EXPIRE_IN"`
	Redis                   *RedisConfig `yaml:"redis"`
	TLSConfig               *tls.Config  `yaml:"-" ignored:"true"`
	NoUnixSignals           bool         `yaml:"no_unix_signals" envconfig:"NO_UNIX_SIGNALS"`
}

// RedisConfig contains Redis broker, backend, and lock settings.
type RedisConfig struct {
	MaxIdle                int    `yaml:"max_idle" envconfig:"REDIS_MAX_IDLE"`
	MaxActive              int    `yaml:"max_active" envconfig:"REDIS_MAX_ACTIVE"`
	IdleTimeout            int    `yaml:"max_idle_timeout" envconfig:"REDIS_IDLE_TIMEOUT"`
	Wait                   bool   `yaml:"wait" envconfig:"REDIS_WAIT"`
	ReadTimeout            int    `yaml:"read_timeout" envconfig:"REDIS_READ_TIMEOUT"`
	WriteTimeout           int    `yaml:"write_timeout" envconfig:"REDIS_WRITE_TIMEOUT"`
	ConnectTimeout         int    `yaml:"connect_timeout" envconfig:"REDIS_CONNECT_TIMEOUT"`
	NormalTasksPollPeriod  int    `yaml:"normal_tasks_poll_period" envconfig:"REDIS_NORMAL_TASKS_POLL_PERIOD"`
	DelayedTasksPollPeriod int    `yaml:"delayed_tasks_poll_period" envconfig:"REDIS_DELAYED_TASKS_POLL_PERIOD"`
	DelayedTasksKey        string `yaml:"delayed_tasks_key" envconfig:"REDIS_DELAYED_TASKS_KEY"`
	ClientName             string `yaml:"client_name" envconfig:"REDIS_CLIENT_NAME"`
	MasterName             string `yaml:"master_name" envconfig:"REDIS_MASTER_NAME"`
	ClusterEnabled         bool   `yaml:"cluster_enabled" envconfig:"REDIS_CLUSTER_ENABLED"`
	SentinelPassword       string `yaml:"sentinel_password" envconfig:"REDIS_SENTINEL_PASSWORD"`

	// ClaimLeaseDuration is the visibility lease for a claimed task, in
	// milliseconds. The go-redis broker renews this lease while processing.
	ClaimLeaseDuration int `yaml:"claim_lease_duration_milliseconds" envconfig:"REDIS_CLAIM_LEASE_DURATION_MILLISECONDS"`
	// ClaimRenewInterval is the lease-renewal interval, in milliseconds.
	ClaimRenewInterval int `yaml:"claim_renew_interval_milliseconds" envconfig:"REDIS_CLAIM_RENEW_INTERVAL_MILLISECONDS"`
	// ClaimRecoveryPeriod is the expired-claim scan period, in milliseconds.
	ClaimRecoveryPeriod int `yaml:"claim_recovery_period_milliseconds" envconfig:"REDIS_CLAIM_RECOVERY_PERIOD_MILLISECONDS"`
	// ClaimRecoveryBatchSize limits claims recovered in one atomic scan.
	ClaimRecoveryBatchSize int `yaml:"claim_recovery_batch_size" envconfig:"REDIS_CLAIM_RECOVERY_BATCH_SIZE"`
	// ClaimKeyPrefix namespaces the reliable claim payload and lease keys.
	ClaimKeyPrefix string `yaml:"claim_key_prefix" envconfig:"REDIS_CLAIM_KEY_PREFIX"`
}
