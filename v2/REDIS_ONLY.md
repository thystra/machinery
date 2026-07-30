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

## Reliable go-redis claims

The v2 go-redis broker uses leased in-flight claims and acknowledgement after
`TaskProcessor.Process` returns successfully. Claim, acknowledgement, release,
expired-claim recovery, and delayed-task promotion are Redis-atomic operations.
This provides **at-least-once** processing for abrupt worker or host failure:
unacknowledged work is returned to its original ready queue after its lease
expires.

At-least-once processing can repeat a task. In particular, a worker can die
after the remote side has accepted an operation but before Machinery records
local completion and acknowledges the claim. Task handlers and remote endpoints
must therefore be idempotent or deduplicate using stable operation identifiers.
This fork does not claim exactly-once delivery.

The ready-list task JSON, delayed-task JSON, result state, retry, group, and
workflow formats remain unchanged. Existing producers, including v1 producers,
can publish tasks consumed by the reliable v2 go-redis worker. A v1 worker or
the legacy redigo broker still removes a ready task before processing and does
not provide the leased-claim guarantee. Before rolling back from a reliable v2
worker, operators must return any outstanding in-flight claims to their ready
queues.

Reliable claims are intentionally rejected when `redis.cluster_enabled` is
true. The existing ready-queue key format does not guarantee that ready,
in-flight payload, and lease keys share a Redis Cluster hash slot, so the
required atomic Lua operations cannot be guaranteed without a wire-format/key
migration. Redis standalone and Sentinel deployments are supported.

### Configuration

All duration values are milliseconds:

- `redis.claim_lease_duration_milliseconds` (default `120000`)
- `redis.claim_renew_interval_milliseconds` (default `30000`)
- `redis.claim_recovery_period_milliseconds` (default `1000`)
- `redis.claim_recovery_batch_size` (default `100`)
- `redis.claim_key_prefix` (default `machinery:claims`)

The claim lease must exceed the renewal interval. If the configured renewal
interval is greater than or equal to the lease, the broker reduces it to one
third of the lease.
