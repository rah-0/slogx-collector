# slogx-collector

`slogx-collector` reads newline-delimited JSON from a pipe, saves records in a durable disk
journal, and delivers batches to a destination. It retries temporary failures and replays
pending records after a restart. Delivery is at least once.

OpenObserve is the first destination. The reusable collector accepts any JSON object and uses
a small destination interface, so other destinations and stream sources can be added without
changing journal storage. The executable and library use only the Go standard library and
have no dependency on `slogx`.

## Installation

Requires Go 1.27.1 or later. The durable journal supports Linux, macOS, FreeBSD, NetBSD,
OpenBSD, and DragonFly BSD.

Install a published version directly from the module root:

```sh
go install github.com/rah-0/slogx-collector@latest
```

Or install from a local checkout:

```sh
go install .
```

## Usage

Set `LOG_INGEST_PASSWORD` in the launching environment, then pipe JSON logs into the command:

```sh
./my-service | slogx-collector \
  -destination openobserve \
  -endpoint https://logs.example.com/api/default/application/_json \
  -journal-dir /var/lib/slogx-collector/application \
  -username ingest@example.com \
  -password-env LOG_INGEST_PASSWORD
```

Configuration uses CLI flags. Keep a dedicated journal directory for each collector and
destination, and preserve it across restarts. The journal has no configured disk quota;
intake continues during destination outages.

Add source metadata with repeatable `-field Name=Value` flags, such as
`-field project=application -field version=v1.2.3`. Fields override matching input keys
before journaling, so backlog replay retains the original values across deployments.

Add `-check` to validate the same configuration and credential sources without reading
stdin, opening the journal, or contacting the destination. It exits silently with status
`0` on success or reports a configuration error with status `2`. It does not verify remote
authentication or journal health.

See the [collector guide](collector/README.md) for all flags, input and delivery contracts,
OpenObserve timestamp settings, and implementing destinations. Run `slogx-collector -help`
for the CLI reference.

## Packages

- [`collector`](collector/README.md): collection loop, disk journal, and `Destination` interface;
  import `github.com/rah-0/slogx-collector/collector`.
- [`collector/openobserve`](collector/openobserve): OpenObserve JSON ingestion adapter;
  import `github.com/rah-0/slogx-collector/collector/openobserve`.

## Testing

Run unit tests from the repository root without Docker:

```sh
go test -race ./...
```

The separate integration module uses Testcontainers and requires a running Docker daemon:

```sh
cd integration
go test -count=1 -v -timeout=5m ./...
```

It tests ingestion, queries, authentication, partial rejection, and CLI journal replay against
a real OpenObserve container. See the [testing guide](collector/README.md#testing) for details.

Benchmarks cover parsing, durable journaling, replay, collection, and OpenObserve
request preparation. See [benchmark results and commands](BENCHMARKS.md).

## License

Available under the [MIT License](LICENSE).

## Support

If `slogx-collector` keeps your logs flowing, consider buying me a coffee to support its development.

[![Buy Me A Coffee](https://cdn.buymeacoffee.com/buttons/default-orange.png)](https://www.buymeacoffee.com/rah.0)
