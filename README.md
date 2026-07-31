<!-- /home/alan/src/machinery/README.md -->

# Machinery v2 — Activity-Relay maintained fork

This repository is the maintained Machinery v2 fork used by
[Activity-Relay](https://github.com/thystra/Activity-Relay).

The maintained code is the Go module under [`v2/`](v2/). The obsolete
Machinery v1 implementation, root Go module, legacy multi-backend examples,
and legacy integration-test infrastructure have been removed from the active
tree. Their history remains available through Git.

## Supported scope

The maintained production scope is Redis-backed Machinery v2.

- Module path: `github.com/RichardKnop/machinery/v2`
- Fork source: `github.com/thystra/machinery/v2`
- Primary Redis implementation: `go-redis`
- Delivery contract: at-least-once
- Redis Cluster mode: unsupported
- Active in-flight work: protected by leased claims
- Expired claims: atomically recovered to the ready queue
- Delayed tasks: atomically promoted when due

The retained eager and null components are local/test helpers. The retained
Redigo implementation is Redis-specific legacy compatibility; it is not the
reliable-claims implementation used by Activity-Relay.

See [`v2/REDIS_ONLY.md`](v2/REDIS_ONLY.md) for the maintained-fork contract
and operational details.

## Consuming the fork

Existing applications may preserve the upstream import path and pin this fork
with a Go module replacement:

```go
replace github.com/RichardKnop/machinery/v2 => github.com/thystra/machinery/v2 v2.0.17-0.20260730204902-5efae3f700cd
```

Activity-Relay pins an exact validated fork revision rather than following an
unbounded branch.

## Development and validation

Run Go commands from the v2 module directory:

```bash
cd v2
go mod verify
go test -count=1 ./...
go test -race -count=1 ./...
go vet ./...
```

There is intentionally no Go module at the repository root.

## Reliability semantics

A task is moved atomically from the ready queue into an in-flight payload hash
and lease set. The worker acknowledges the claim only after task processing
returns successfully. If the worker terminates before acknowledgement, another
worker recovers the expired claim and retries it.

This prevents silent loss but intentionally permits duplicates when a remote
side effect succeeds before the local acknowledgement is persisted. Consumers
must therefore treat task execution as at-least-once and make externally
visible operations idempotent where practical.

## License

The original Machinery license remains in [`LICENSE`](LICENSE).

<!-- EOF: /home/alan/src/machinery/README.md -->
