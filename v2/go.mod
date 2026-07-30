module github.com/RichardKnop/machinery/v2

go 1.22

toolchain go1.24.1

require (
	github.com/RichardKnop/logging v0.0.0-20190827224416-1a693bdd4fae
	github.com/go-redsync/redsync/v4 v4.8.1
	github.com/gomodule/redigo v1.9.2
	github.com/google/uuid v1.6.0
	github.com/kelseyhightower/envconfig v1.4.0
	github.com/opentracing/opentracing-go v1.2.0
	github.com/redis/go-redis/v9 v9.12.1
	github.com/robfig/cron/v3 v3.0.1
	github.com/stretchr/testify v1.10.0
	gopkg.in/yaml.v2 v2.4.0
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/hashicorp/errwrap v1.1.0 // indirect
	github.com/hashicorp/go-multierror v1.1.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace git.apache.org/thrift.git => github.com/apache/thrift v0.0.0-20180902110319-2566ecd5d999
