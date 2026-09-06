# JSON logs

After installing the collector, run from the repository root with an ingestion
endpoint and a dedicated journal directory:

```sh
export LOG_ENDPOINT=https://logs.example.com/api/default/application/_json
export JOURNAL_DIR=/var/lib/slogx-collector/application
bash examples/logs/run.sh -username ingest@example.com -password-env LOG_INGEST_PASSWORD
```

Set `LOG_INGEST_PASSWORD` in the launching environment. For a local endpoint
without authentication, omit those two flags. Trailing arguments are passed
directly to the collector; `COLLECTOR_BIN` can select another executable.

[run.sh](run.sh) sends [events.jsonl](events.jsonl) through stdin. These are generic
JSON objects: slog's `msg`, `level`, and `time` fields are not required. The large
integer, nested object, and duplicate `tag` fields remain intact in the request.
The collector drains accepted records before returning at EOF.

For a running application, replace the fixture redirection with a pipe:

```sh
./my-service | slogx-collector \
  -destination openobserve -endpoint "$LOG_ENDPOINT" -journal-dir "$JOURNAL_DIR" \
  -username ingest@example.com -password-env LOG_INGEST_PASSWORD
```

Every nonblank input line must be one complete JSON object. Pipe only JSON output;
plain-text stderr and stack traces do not satisfy that contract. The endpoint
contains the OpenObserve organization (`default`) and log stream (`application`).
These fixture records have no event timestamp, so OpenObserve uses ingestion time.
See [timestamp mapping](../../internal/collector/README.md#openobserve-logs) for
applications supplying timestamps.

`go test -count=1 -race -cover -covermode=atomic ./examples/logs` runs this script
against a local HTTP fixture and verifies exact record preservation.
