package config_test

import (
	"bufio"
	"github.com/RichardKnop/machinery/v2/config"
	"github.com/stretchr/testify/assert"
	"os"
	"strings"
	"testing"
)

func TestNewFromEnvironment(t *testing.T) {
	file, err := os.Open("test.env")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		parts := strings.SplitN(scanner.Text(), "=", 2)
		if len(parts) == 2 {
			t.Setenv(parts[0], parts[1])
		}
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	cnf, err := config.NewFromEnvironment()
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
