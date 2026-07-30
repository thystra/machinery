package config_test

import (
	"github.com/RichardKnop/machinery/v2/config"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestReadFromFile(t *testing.T) {
	data, err := config.ReadFromFile("testconfig.yml")
	if err != nil {
		t.Fatal(err)
	}
	assert.Contains(t, string(data), "broker: redis://broker.example:6379/5")
}

func TestNewFromYaml(t *testing.T) {
	cnf, err := config.NewFromYaml("testconfig.yml", false)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, "redis://broker.example:6379/5", cnf.Broker)
	assert.Equal(t, "default_queue", cnf.DefaultQueue)
	assert.Equal(t, "redis://backend.example:6379/5", cnf.ResultBackend)
	assert.Equal(t, 123456, cnf.ResultsExpireIn)
	assert.Equal(t, 12, cnf.Redis.MaxIdle)
	assert.Equal(t, 123, cnf.Redis.MaxActive)
	assert.Equal(t, 456, cnf.Redis.IdleTimeout)
	assert.Equal(t, 1001, cnf.Redis.NormalTasksPollPeriod)
	assert.Equal(t, 23, cnf.Redis.DelayedTasksPollPeriod)
	assert.Equal(t, "delayed_tasks_key", cnf.Redis.DelayedTasksKey)
	assert.Equal(t, "relay-client", cnf.Redis.ClientName)
	assert.Equal(t, "master_name", cnf.Redis.MasterName)
	assert.True(t, cnf.Redis.ClusterEnabled)
	assert.Equal(t, "sentinel-secret", cnf.Redis.SentinelPassword)
	assert.True(t, cnf.NoUnixSignals)
}
