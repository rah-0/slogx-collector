# JSON collector

`slogx-collector` reads newline-delimited JSON objects, persists them in a disk journal, and sends
batches to a destination. OpenObserve is the first destination. The collection loop uses the
public `collector.Destination` interface; input parsing and journal storage have no dependency
on OpenObserve or a logging library. Import this package as
`github.com/rah-0/slogx-collector/collector`; the OpenObserve adapter is
`github.com/rah-0/slogx-collector/collector/openobserve`.

## Install and run

Requires Go 1.27.1 or later. The collector's durable journal currently supports Linux, macOS,
FreeBSD, NetBSD, OpenBSD, and DragonFly BSD on a local filesystem with file locking and sync
support. Other platforms return an explicit unsupported error.

Install a published version that includes the command:

```sh
go install github.com/rah-0/slogx-collector@latest
```

To install from a local checkout, run this from the repository root:

```sh
go install .
```

Ensure Go's binary installation directory is on `PATH`. Configure the executable entirely
through command-line flags:

```sh
./my-service | slogx-collector \
  -input stdin \
  -destination openobserve \
  -endpoint https://logs.example.com/api/default/application/_json \
  -journal-dir /var/lib/slogx-collector/application \
  -username ingest@example.com \
  -password-env LOG_INGEST_PASSWORD
```

Set `LOG_INGEST_PASSWORD` in the launching environment. There are no collector configuration
files; password and header environment variables are selected explicitly by flags. Literal
passwords, password files, and arbitrary HTTP headers are also supported. Use `-password-env`, `-password-file`,
or `-header-env` when a secret should stay out of process arguments.

Use a dedicated journal directory for each collector and destination. It is created when
needed, locked while running, and bound to the destination URL. Reusing it for another URL
fails before sending its backlog. Preserve the directory across restarts.

## Input contract

Each nonblank physical line must contain one JSON object. A final object without a newline
is accepted. Arrays, scalar values, multiline objects, and malformed JSON are errors.
There is no configured record-size limit. Diagnostics identify the line without copying
its contents.

Objects can use any field names and types; slog's `time`, `level`, and `msg` fields are not
required. Raw JSON preserves integer precision, nested values, field order, and duplicate
keys through collection. A destination can transform fields or impose its own restrictions.

Only `-input stdin` is implemented. Shell redirection also reads a finite JSON file:

```sh
slogx-collector -destination openobserve \
  -endpoint http://localhost:5080/api/default/application/_json \
  -journal-dir ./collector-journal < events.jsonl
```

There is no file watcher yet. Programmatic callers pass an `io.ReadCloser` to `collector.Run`,
so additional stream sources do not require changing destination implementations. `Run` owns
the reader; its `Close` must unblock an active `Read`. Close failures are returned alongside
any collection error and remain identifiable with `errors.Is`.

All bytes sent to this input must satisfy the JSON contract. Merge stderr into the pipe only
if it also contains JSON objects; ordinary panic stacks and plain-text output are invalid input.

## Buffering and delivery

- Each accepted record is appended and synced to disk before becoming available for delivery.
  There is no configured journal quota. Intake continues during destination outages; actual
  disk write failures stop collection with a nonzero exit.
- Delivery uses one batch at a time, in journal order. `-batch-bytes` controls batch grouping;
  a record larger than that target is sent alone. Memory holds the current input record and
  delivery batch; the backlog stays on disk, with small metadata per journal segment.
  Record syncing limits intake throughput, so the pipe can still block when disk intake is
  slower than the producer. There is no destination-outage policy that deliberately pauses intake.
- Temporary errors retry with exponential backoff. A destination's requested minimum delay
  is respected. The default is unlimited retries; `-max-retries` can bound them.
- Successful delivery advances a synced checkpoint before acknowledged segments are removed.
  Segments rotate at approximately 16 MiB; a larger record occupies its own segment.
- Restart replays unacknowledged records in order before newly journaled records. Delivery is
  **at least once**: a lost response or a crash after remote acceptance can cause duplicates.
  No exactly-once guarantee is provided.
- Clean stdin EOF drains the journal before exiting. SIGINT, SIGTERM, or malformed input stops
  intake and allows `-shutdown-timeout` to drain saved records. Records still pending remain
  for the next run. Bytes still in the pipe or reader buffers have not been accepted; stop the
  producer and let stdin reach EOF when a complete drain is required.
- Permanent destination errors stop the process and preserve the pending batch and backlog.
  Correct the configuration or rejected data before restarting. There is no automatic dropping
  or dead-letter policy. Treat journal files as collector-owned state, not editable JSON logs.

The journal checks record checksums and recovers incomplete final writes. Durability assumes
the local filesystem and storage honor sync operations; copying a journal requires stopping
its collector first.

## OpenObserve

`-endpoint` is the complete JSON ingestion URL, including organization and stream:
`https://logs.example.com/api/<organization>/<stream>/_json`. The adapter posts JSON arrays
and checks the response's successful and failed record counts. An HTTP 200 alone does not
acknowledge a batch. See [OpenObserve's JSON ingestion API](https://openobserve.ai/docs/reference/api/ingestion/logs/json/).

HTTP 408, 429, server errors, transport failures, and ambiguous acknowledgments are retryable.
Other HTTP errors and explicit record rejections are terminal. For partial acceptance,
OpenObserve does not identify the failed records in this response format: the whole batch
stays journaled, and replay can duplicate the accepted subset. Redirects are rejected.

The CLI defaults to copying the first exact top-level `time` field into `_timestamp`, leaving
the original field intact. Existing `_timestamp` or `@timestamp` fields take precedence.
Strings accept RFC3339Nano or slogx's default `2006-01-02 15:04:05.000000` UTC layout; integer
values mean Unix epoch microseconds. A different `-timestamp-layout` replaces automatic layout
detection. A missing source field is left alone; an incompatible value is a terminal error.
OpenObserve uses ingestion time when no timestamp override is supplied.

OpenObserve also enforces an ingestion age window. Its default `ZO_INGEST_ALLOWED_UPTO=5`
rejects events older than five hours. Configure that server setting to cover the backlog age
you need to replay after an outage; the collector preserves original timestamps. See the
[OpenObserve environment reference](https://openobserve.ai/docs/administration/configuration/environment-variables/#ingestion-and-schema-management).

Use `-timestamp-field event_time` for another source key, or `-timestamp-field ''` to disable
mapping for generic JSON. Mapping is disabled by default when using `openobserve.Config`
directly rather than the CLI.

## CLI reference

`slogx-collector -help` prints the available flags. No positional arguments are accepted.

| Flag | Default | Purpose |
| --- | --- | --- |
| `-input` | `stdin` | Input mode; stdin is currently the only implementation. |
| `-destination` | Required | Destination type; currently `openobserve`. |
| `-endpoint` | Required | Complete destination ingestion URL. |
| `-journal-dir` | Required | Persistent directory with no configured disk quota. |
| `-username` | Empty | HTTP Basic authentication username. |
| `-password` | Empty | Literal Basic password; visible in process arguments. |
| `-password-env` | Empty | Name of an environment variable containing the password. |
| `-password-file` | Empty | Password file; trailing CR/LF removed. |
| `-header` | None | Repeatable `Name=Value` HTTP header. |
| `-header-env` | None | Repeatable `Name=ENV` header read from the named variable. |
| `-timestamp-field` | `time` | Field to copy to `_timestamp`; empty disables mapping. |
| `-timestamp-layout` | Automatic | Go layout for timestamp strings. |
| `-request-timeout` | `10s` | Timeout per HTTP attempt, including response reading. |
| `-batch-size` | `500` | Maximum records per batch. |
| `-batch-bytes` | `4194304` | Target raw JSON bytes per batch; larger records are sent alone. |
| `-flush-interval` | `1s` | Maximum accumulation time before attempting a partial batch. |
| `-retry-interval` | `1s` | Initial retry delay. |
| `-max-retry-interval` | `30s` | Exponential backoff cap; server Retry-After can exceed it. |
| `-max-retries` | `0` | Retries after the first attempt; zero means unlimited. |
| `-shutdown-timeout` | `10s` | Drain deadline after a signal or invalid input. |

The three password flags are mutually exclusive. Basic credentials cannot be combined with
an `Authorization` header. Batch sizes, intervals, and timeouts must be positive.

HTTP uses Go's standard proxy environment variables and system certificate trust.

Exit status is `0` after a complete EOF drain or help, `2` for invalid configuration, and `1`
for runtime errors or interruption, including an interruption whose drain completed.
Runtime diagnostics go to stderr; stdout is reserved for help output.

## Implementing another destination

The public contract is deliberately small:

```go
type Destination interface {
    Send(context.Context, []json.RawMessage) error
}
```

Implement it in a destination package and pass the implementation to `collector.Run`:

```go
err := collector.Run(ctx, input, destination, collector.Options{
    JournalDir: "/var/lib/my-collector/application",
    JournalKey: "stable-destination-identity",
})
```

`Send` must honor its context, leave records unchanged, and not retain the batch after
returning. Nil acknowledges the entire batch. Ordinary errors are terminal; return a
`*collector.RetryError` for a temporary failure, optionally setting its `After` delay.
The collector handles retries and checkpoints centrally.

Fixed errors are exported as `Err...` sentinel values in each package's `errors.go`.
Use `errors.Is` to match them, including through record context and retry wrappers:

```go
if errors.Is(err, openobserve.ErrNegativeTimeout) {
    // Correct the configured timeout.
}
```

Adding a destination to the distributed command also requires wiring its constructor and CLI
flags into [`main.go`](../main.go) at the repository root. Third parties can build their own
command around these packages.
There is no runtime plugin loader or destination registry to configure.

## Testing

Unit tests run from the repository root without Docker:

```sh
go test -race ./...
```

See [benchmarks](../BENCHMARKS.md) for performance measurements and commands covering
parsing, journal writes and replay, collection, and OpenObserve request preparation.

The separate `integration` module uses Testcontainers to start a real OpenObserve
`v0.92.2` instance. From the repository root, run:

```sh
cd integration
go test -count=1 -v -timeout=5m ./...
```

A running Docker daemon is required. The first run pulls the pinned image. Each run uses
a disposable container, temporary journals, test credentials, and a dynamically assigned
port; Testcontainers removes the container afterward. OpenObserve telemetry is disabled.
The nested module keeps container-testing dependencies out of the library and executable.

The suite ingests and queries records through OpenObserve's real APIs, checking timestamp
mapping, large integers, nested fields, authentication failures, and partial record rejection.
It also builds the actual command, pipes JSON into it, and verifies journal replay in a new
process after correcting failed authentication. Unit tests cover temporary outages, retry
scheduling, and journal crash recovery independently of Docker.
