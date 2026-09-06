# slogx-collector

`slogx-collector` reads newline-delimited JSON, syncs records to a disk journal,
and delivers batches with retries and restart replay. It supports OpenObserve
JSON logs and OTLP HTTP/JSON traces. Delivery is at least once.

Applications write JSON to the collector's stdin and do not import its packages.
Slogx applications can send ordinary logs and completed spans through the same pipe;
the collector owns conversion and delivery.

## Installation

Requires Go 1.27.1 or later. Durable journaling supports Linux, macOS, FreeBSD,
NetBSD, OpenBSD, and DragonFly BSD.

```sh
go install github.com/rah-0/slogx-collector@latest
```

To install this checkout, run `go install .`. For an optional local workspace
with a sibling slogx checkout, run `go work init . ./integration ../slogx` from
this directory if no workspace exists. Workspace files are ignored by Git.

## Quick start

Set `LOG_INGEST_PASSWORD` in the launching environment, then pipe JSON output:

```sh
./my-service | slogx-collector \
  -destination openobserve \
  -endpoint https://logs.example.com/api/default/application/_json \
  -journal-dir /var/lib/slogx-collector/application \
  -username ingest@example.com -password-env LOG_INGEST_PASSWORD
```

Each nonblank input line must be one JSON object. Use `slogx-collector -help`
for CLI options or the [collector guide](internal/collector/README.md) for contracts.

## Examples

The [examples directory](examples/README.md) contains runnable scripts and tests
for each case. The tracing application uses only slogx and the standard library.

| Example | What it demonstrates |
| --- | --- |
| [logs](examples/logs) | Collect generic JSON while preserving nested values and large integers. |
| [fields](examples/fields) | Add or override deployment metadata before journaling. |
| [check](examples/check) | Validate configuration and credential sources without collecting. |
| [traces](examples/traces) | Route logs and three nested spans to separate endpoints. |
| [otlp](examples/otlp) | Deliver spans to an OTLP endpoint and skip ordinary logs. |
| [replay](examples/replay) | Retain records through an outage and replay the journal after restart. |

## Traces

OpenObserve is a storage and query backend; OTLP is a telemetry transmission
protocol. `-destination` selects an output adapter: `openobserve` integrates
OpenObserve's JSON logs API with optional OTLP trace delivery through
`-traces-endpoint`; `otlp` sends only traces to any compatible OTLP HTTP/JSON
endpoint, including OpenObserve, and skips ordinary logs. Without
`-traces-endpoint`, the `openobserve` adapter sends every record as a log.
Keep slogx's reserved `span` group at the record root. See the
[application and pipeline example](examples/traces) and
[trace delivery contract](internal/collector/otlp/README.md).

## Journal and delivery

Use a dedicated persistent journal for each collector and destination. Durability
starts after a record is synced; application and pipe buffers are outside that
guarantee. EOF drains the journal. Pending records survive restarts, and uncertain
acknowledgments can cause duplicates. There is no configured disk quota.
See [delivery behavior](internal/collector/README.md#buffering-and-delivery) for
journal identity, retries, and shutdown details.

## Testing

Run unit and CLI example tests without Docker:

```sh
go test -count=1 -race -cover -covermode=atomic ./...
```

Example tests use local HTTP fixtures. The separate [integration suite](internal/collector/README.md#testing)
uses Docker and a real OpenObserve instance. See [benchmarks](BENCHMARKS.md) for
performance results and commands.

## Support

If `slogx-collector` keeps your logs flowing, consider buying me a coffee to support its development.

[![Buy Me A Coffee](https://cdn.buymeacoffee.com/buttons/default-orange.png)](https://www.buymeacoffee.com/rah.0)

Licensed under the [MIT License](LICENSE).
