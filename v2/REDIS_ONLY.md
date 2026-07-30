# thystra Machinery Redis-only fork

This branch is based on upstream Machinery v2.0.16 and is maintained for
Activity-Relay. It retains Machinery core, task and workflow handling, workers,
retries, result state, Redis brokers/backends, Redis and eager locks, eager test
implementations, configuration loading, tracing, logging, and utilities.

AMQP, AWS SQS, AWS DynamoDB, Google Cloud Pub/Sub, MongoDB, Memcached, the
AMQP connector helper, AMQP integration test, AMQP-specific worker branches,
and the example commands are removed together with their SDK dependencies. The
shared integration-test helpers and Redis integration test remain.

The module path intentionally remains `github.com/RichardKnop/machinery/v2`.
Consumers select this fork with a Go `replace` directive. This keeps internal
imports and the task, queue, delayed-retry, group, and result wire formats
unchanged.
