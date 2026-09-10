# Logs and three-layer traces

[main.go](main.go) uses global `slog` and `slogx` calls to produce four ordinary
logs and three completed spans as JSON. `handleRequest` calls `loadItem`, which
calls `queryItem`; each operation has its own attributes and span ID within one
trace. The simulated database error retains all three wrapping layers, is
recorded in each span, and is logged once at the request boundary. The program
exits successfully.

Run the producer alone from the repository root:

```sh
go run ./examples/traces
```

The application does not import or start the collector. [run.sh](run.sh) pipes
its JSON output into a separate `slogx-collector` process. Install the collector
first, or set `COLLECTOR_BIN` to the absolute path of a built executable. Running
the producer requires the Go toolchain used by this module.

Set the endpoints and a persistent journal directory, then run from the
repository root:

```sh
export LOG_ENDPOINT='https://logs.example.com/api/default/application/_json'
export TRACE_ENDPOINT='https://logs.example.com/api/default/v1/traces'
export JOURNAL_DIR="$HOME/.local/state/slogx-collector/catalog"
./examples/traces/run.sh \
  -username ingest@example.com \
  -password-env LOG_INGEST_PASSWORD
```

Set `LOG_INGEST_PASSWORD` in the launching environment. Trailing arguments are
passed to the collector, allowing authentication and other CLI options. Both
endpoints receive the supplied credentials and headers. OpenObserve's
organization is part of each URL; its log stream is in `LOG_ENDPOINT`, and the
script's `stream-name=application` header selects its trace stream.
`-resource service.name=catalog` identifies the producing application in traces.

The collector journals the original records. Its `openobserve` output adapter
routes ordinary logs to OpenObserve's JSON endpoint and delivers records with a
root `span` group using the OTLP HTTP/JSON protocol through `-traces-endpoint`.
Native source metadata keeps `source.function`, `source.file`, and `source.line`;
structured error groups retain their nesting. Keep the reserved `span` group
at the root and preserve its metadata names and types. Use `slog.Group` for
application fields instead of placing the default logger under `WithGroup`.

Durability begins when the collector writes and syncs the journal. Temporary
outages are retried, and retained records replay after restarts with their
original IDs, timestamps, and attributes. Mixed delivery is acknowledged only
after both destinations succeed, so a retry can resend an already accepted
partition. Keep the same destination and resources while records are pending;
a fully drained journal can adopt new settings after startup validation.

Without `-traces-endpoint`, the OpenObserve destination stores completed spans
as ordinary logs. For delivery to a generic traces endpoint, see
[trace-only OTLP](../otlp). See the
[trace delivery contract](../../internal/collector/otlp/README.md) and
[CLI reference](../../internal/collector/README.md#cli-reference) for options.

`main_test.go` checks the application's native records. `run_test.go` executes
the shell pipeline against local HTTP test servers, checking routing, converted
span identities, attributes, and authentication without an external backend.
