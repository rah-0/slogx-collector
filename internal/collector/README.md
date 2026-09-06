# JSON collector

`slogx-collector` reads newline-delimited JSON objects, persists them in a disk journal, and sends
batches to a destination. OpenObserve JSON logs and OTLP HTTP/JSON traces are supported.
The executable uses the `collector.Destination` interface; input parsing and journal storage
have no dependency on OpenObserve or a logging library. Applications write JSON output to the
collector process and do not import its packages.

## Install and run

Requires Go 1.27.1 or later. The collector's durable journal currently supports Linux, macOS,
FreeBSD, NetBSD, OpenBSD, and DragonFly BSD on a local filesystem with file locking and sync
support. Other platforms return an explicit unsupported error.

Install a published version that includes the command:

```sh
go install github.com/rah-0/slogx-collector@latest
```

For local development, follow the [workspace setup](../../README.md#installation), then install
from the repository root:

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
needed, locked while running, and bound to the destination type and URL. Trace journals also
include the traces endpoint, `stream-name` header, and configured resource attributes in this
identity. Changing that identity fails before sending its backlog. Preserve the directory across
restarts; use a new journal when changing trace resource metadata.

## Checking configuration

Add `-check` to the normal collector arguments for a preflight check:

```sh
slogx-collector -check \
  -destination openobserve \
  -endpoint https://logs.example.com/api/default/application/_json \
  -journal-dir /var/lib/slogx-collector/application \
  -username ingest@example.com -password-env LOG_INGEST_PASSWORD
```

This validates configuration, including required flags, HTTP header syntax, and selected credential sources.
It reads configured password files and environment variables, then exits without reading
stdin, creating or opening journal files, or contacting the destination. Successful checks
are silent and return `0`; invalid configuration returns `2` with a diagnostic on stderr.
Credential values are not printed.

The check does not verify remote authentication or availability, journal contents, locking,
or filesystem permissions. It can run while another collector owns the journal. Supply the
same `-journal-dir` and other arguments intended for collection; required flags still apply.

## Input contract

Each nonblank physical line must contain one JSON object. A final object without a newline
is accepted. Arrays, scalar values, multiline objects, and malformed JSON are errors.
There is no configured record-size limit. Diagnostics identify the line without copying
its contents.

Objects can use any field names and types; slog's `time`, `level`, and `msg` fields are not
required. Without configured static fields, raw JSON preserves integer precision, nested
values, field order, and duplicate keys through collection. A destination can transform
fields or impose its own restrictions.

Only `-input stdin` is implemented. Shell redirection also reads a finite JSON file:

```sh
slogx-collector -destination openobserve \
  -endpoint http://localhost:5080/api/default/application/_json \
  -journal-dir ./collector-journal < events.jsonl
```

There is no file watcher yet. Internally, the command passes an `io.ReadCloser` to `collector.Run`,
so additional stream sources do not require changing destination implementations. `Run` owns
the reader; its `Close` must unblock an active `Read`. Close failures are returned alongside
any collection error and remain identifiable with `errors.Is`.

All bytes sent to this input must satisfy the JSON contract. Merge stderr into the pipe only
if it also contains JSON objects; ordinary panic stacks and plain-text output are invalid input.

## Static fields

Use repeatable `-field Name=Value` flags to attach deployment or source metadata to JSON records:

```sh
./my-service | slogx-collector \
  -destination openobserve \
  -endpoint https://logs.example.com/api/default/application/_json \
  -journal-dir /var/lib/slogx-collector/application \
  -username ingest@example.com -password-env LOG_INGEST_PASSWORD \
  -field organization=example -field project=application -field version=v1.2.3
```

Values are strings; empty values and additional `=` characters are preserved. Field names
are literal top-level keys, and the last flag for a repeated name wins. Configured fields
replace same-named input fields. With static fields enabled, objects are decoded and
re-encoded using standard `encoding/json`: top-level duplicate keys resolve to their last
value and field order or whitespace can change. Unrelated number values retain their exact
precision.

Fields are added **before journaling**, only to new input. Replaying a backlog preserves
the metadata originally stored with each record, even when a new process supplies different
fields. Existing unlabelled journal records remain unlabelled. The command passes these values
through `collector.Options.Fields`, a `map[string]string` copied when `Run` starts; metadata is
independent of the destination. On span records, fields become span attributes during conversion.
Do not overwrite the reserved `span` group with a static field.

Use repeatable `-resource Name=Value` flags for trace resource metadata, such as
`-resource service.name=example-service`. Values are strings, and the last flag for a repeated
name wins. Resources are applied during trace conversion and included in the journal identity,
so an existing backlog cannot be replayed under a different resource configuration.

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

## OpenObserve logs

`-destination` selects an output adapter. `openobserve` is the backend integration
for OpenObserve's JSON logs API, with optional OTLP trace routing. `otlp` is the
generic OTLP HTTP/JSON traces adapter. OpenObserve is a storage and query backend;
OTLP is a transmission protocol that OpenObserve and other compatible backends accept.

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

OpenObserve v0.92.2, used by the integration suite, defaults `ZO_INGEST_ALLOWED_UPTO` to five
hours, rejecting older events. This default is defined in its
[versioned configuration source](https://github.com/openobserve/openobserve/blob/v0.92.2/src/config/src/config.rs#L1831-L1833).
Configure that server setting to cover the backlog age you need to replay after an outage;
the collector preserves original timestamps. See the
[OpenObserve environment reference](https://openobserve.ai/docs/administration/configuration/environment-variables/#ingestion-and-schema-management)
for the deployed server's configuration options.

Use `-timestamp-field event_time` for another source key, or `-timestamp-field ''` to disable
mapping for generic JSON. Mapping is disabled by default when using `openobserve.Config`
directly rather than the CLI.

## OTLP traces

Slogx writes completed spans as ordinary JSON log records, identified by a root `span` group.
The application uses only Slogx; OTLP encoding takes place inside this executable.

For one input carrying both logs and spans, use `-destination openobserve` with `-endpoint` for
logs and `-traces-endpoint` for traces. Records with a root `span` group go to the traces endpoint;
other records go to the logs endpoint. Both endpoints share the configured authentication and
headers. Without `-traces-endpoint`, this destination sends all records to the logs endpoint.

For trace-only delivery to any compatible OTLP HTTP/JSON endpoint, including OpenObserve,
use `-destination otlp` with the trace URL as `-endpoint`. This adapter skips ordinary logs.
Each span record must retain Slogx's root `span` group and its
metadata names and types. Native `WithGroup` still changes output nesting; keep the default
logger ungrouped and use `slog.Group` for application attributes when using this destination.
Malformed span records stop delivery and remain in the journal. The trace destination also
accepts compact OTLP envelopes containing `resourceSpans`; their resource attributes are
retained, and `-resource` applies only to converted native span records.

The complete OpenObserve traces endpoint is `/api/<organization>/v1/traces`. Select the traces
stream with `-header stream-name=<stream>` and set the service with
`-resource service.name=<service>`. Trace journals bind resource metadata and routing settings
alongside the destination endpoint, preserving the configuration across replay.

The collector journals the original JSON records, then converts each delivery batch's spans
into one OTLP HTTP/JSON request. `-batch-size` counts input records, including ordinary logs in
a mixed stream. Nested attributes, source metadata, structured errors, identifiers, events,
and original span timestamps survive conversion. Duplicate attribute keys use the last value
when represented in OTLP. Log timestamp mapping applies only to the logs destination; span
start and end times come from the `span` group. Explicit `-timestamp-field` and
`-timestamp-layout` flags are configuration errors in trace-only mode.

Each trace send calls `http.Client.Do` at most once with the configured timeout. Go's transport
can reconnect and resend within that call; see the [trace delivery contract](otlp/README.md#delivery-and-recovery).
The collector retries transport failures and HTTP 429, 502, 503, and 504, respecting `Retry-After`.
Those response codes follow the [OTLP HTTP retry table](https://opentelemetry.io/docs/specs/otlp/#retryable-response-codes).
Redirects, malformed acknowledgments, other HTTP errors, and partial rejections are terminal.
The collector limits trace response bodies to 4 MiB after decompression.
All protocol conversion, HTTP configuration, acknowledgment handling, and retry scheduling
belong to the collector.

A full acknowledgment advances the journal checkpoint. Partial rejection stops collection and
retains the whole batch because rejected spans cannot be identified individually; accepted spans
may be duplicated if that backlog is replayed. A zero-rejection warning is success. A mixed batch
is acknowledged only after both destinations succeed; if one succeeds before the other fails,
replaying the batch can duplicate the accepted records. Original span IDs are preserved on
replay; delivery remains at least once.

Application writes do not wait for the collector to sync or for the remote endpoints to accept
the records. Stop the producer and allow its pipe and the collector to drain for a complete
shutdown. See the [trace destination contract](otlp/README.md) for conversion and delivery details.

## CLI reference

`slogx-collector -help` prints the available flags. No positional arguments are accepted.

| Flag | Default | Purpose |
| --- | --- | --- |
| `-check` | `false` | Validate configuration and credential sources, then exit without reading input, opening the journal, or contacting the destination. |
| `-input` | `stdin` | Input mode; stdin is currently the only implementation. |
| `-destination` | Required | Output adapter: `openobserve` integrates its JSON logs API with optional OTLP traces; `otlp` delivers only traces to a compatible OTLP HTTP/JSON endpoint. |
| `-endpoint` | Required | Complete destination ingestion URL. |
| `-traces-endpoint` | Empty | With `openobserve`, routes completed span records to this OTLP HTTP/JSON traces URL. |
| `-resource` | None | Repeatable `Name=Value` trace resource attribute; requires trace delivery. |
| `-journal-dir` | Required | Persistent directory with no configured disk quota. |
| `-username` | Empty | HTTP Basic authentication username. |
| `-password` | Empty | Literal Basic password; visible in process arguments. |
| `-password-env` | Empty | Name of an environment variable containing the password. |
| `-password-file` | Empty | Password file; trailing CR/LF removed. |
| `-header` | None | Repeatable `Name=Value` HTTP header. |
| `-header-env` | None | Repeatable `Name=ENV` header read from the named variable. |
| `-field` | None | Repeatable `Name=Value` field applied to input records before journaling; overrides input. |
| `-timestamp-field` | `time` | Log-only field to copy to `_timestamp`; empty disables mapping. |
| `-timestamp-layout` | Automatic | Log-only Go layout for timestamp strings. |
| `-request-timeout` | `10s` | Timeout per HTTP attempt, including response reading. |
| `-batch-size` | `500` | Maximum input records per batch. |
| `-batch-bytes` | `4194304` | Target raw JSON bytes per batch; larger records are sent alone. |
| `-flush-interval` | `1s` | Maximum accumulation time before attempting a partial batch. |
| `-retry-interval` | `1s` | Initial retry delay. |
| `-max-retry-interval` | `30s` | Exponential backoff cap; server Retry-After can exceed it. |
| `-max-retries` | `0` | Retries after the first attempt; zero means unlimited. |
| `-shutdown-timeout` | `10s` | Drain deadline after a signal or invalid input. |

The three password flags are mutually exclusive. Basic credentials cannot be combined with
an `Authorization` header. Batch sizes, intervals, and timeouts must be positive.

HTTP uses Go's standard proxy environment variables and system certificate trust.

Exit status is `0` after a complete EOF drain, a successful check, or help, `2` for invalid configuration, and `1`
for runtime errors or interruption, including an interruption whose drain completed.
Runtime diagnostics go to stderr; stdout is reserved for help output.

## Extending the collector

The internal destination interface is deliberately small:

```go
type Destination interface {
    Send(context.Context, []json.RawMessage) error
}
```

Implement it in a destination package and wire it into the command through `collector.Run`:

```go
err := collector.Run(ctx, input, destination, collector.Options{
    JournalDir: "/var/lib/my-collector/application",
    JournalKey: "stable-destination-identity",
})
```

`Send` must honor its context, leave records unchanged, and not retain the batch after
returning. Nil acknowledges the entire batch. Ordinary errors are terminal; return a
`*collector.RetryError` for a temporary failure, optionally setting its `After` delay.
`collector.Run` controls intake, delivery, retries, and shutdown, acknowledging batches
after `Send` succeeds. [`internal/jsonx`](../jsonx) provides NDJSON reading and
field transforms; `collector.Run` applies configured fields before journaling.
[`internal/journal`](../journal) owns durable storage, checkpoint updates, and crash
recovery. Both packages use only the standard library and have no destination dependencies.

Fixed errors are exported as `Err...` sentinel values in each package's `errors.go`.
Use `errors.Is` to match them, including through record context and retry wrappers:

```go
if errors.Is(err, openobserve.ErrNegativeTimeout) {
    // Correct the configured timeout.
}
```

Keep adapter constructors separate, as in [`internal/cli/openobserve.go`](../cli/openobserve.go)
and [`internal/cli/otlp.go`](../cli/otlp.go). Wire adapter selection in
[`internal/cli/destination.go`](../cli/destination.go), optional trace routing in
[`internal/cli/routing.go`](../cli/routing.go), and CLI flags in
[`internal/cli/config.go`](../cli/config.go). The root
[`main.go`](../../main.go) handles process signals and calls the CLI runner; command
configuration and execution live in `internal/cli`. Applications continue writing JSON to
the executable. There is no runtime plugin loader or destination registry to configure.

## Testing

Unit tests run from the repository root without Docker:

```sh
go test -race ./...
```

See [benchmarks](../../BENCHMARKS.md) for performance measurements and commands covering
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
The nested module keeps container-testing dependencies out of the executable module.

The suite ingests and queries records through OpenObserve's real APIs, checking timestamp
mapping, large integers, nested fields, authentication failures, and partial record rejection.
It also builds the actual command, pipes JSON into it, and verifies journal replay in a new
process after correcting failed authentication. Trace tests check parent relationships across
three layers, local log correlation, structured errors, and journal replay after an outage.
Unit tests cover temporary outages, retry scheduling, and journal crash recovery independently
of Docker.
