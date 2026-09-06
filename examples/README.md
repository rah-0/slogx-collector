# Examples

Each case demonstrates the collector as a separate command. Run scripts from
the repository root with `slogx-collector` installed on `PATH`, or set
`COLLECTOR_BIN` to an absolute path to a built executable. Scripts require Bash.

| Example | Files and behavior |
| --- | --- |
| [logs](logs) | JSON input fixture, ingestion command, and exact-record tests. |
| [fields](fields) | Static field overrides and preserved numeric precision. |
| [check](check) | Configuration and credential validation without collection. |
| [traces](traces) | A Go application with three span layers, plus a mixed log/trace pipeline. |
| [otlp](otlp) | The same application with trace-only delivery to a generic OTLP endpoint. |
| [replay](replay) | An outage, retained journal data, and restart replay. |

## Run a case

Read the selected case's README for its endpoint variables and command. Scripts
send to the endpoints you configure; they do not start a backend. Authentication
flags can be supplied as trailing arguments, using explicitly selected password
or header environment variables. Example data contains no credentials.

Use a dedicated persistent `JOURNAL_DIR` and keep it across restarts. Re-running
an ingestion script creates new input; the replay example shows how to drain
existing backlog without inserting it again. Scripts do not delete journals.

## Test the examples

From the repository root:

```sh
go test -count=1 -race -cover -covermode=atomic ./examples/...
```

Each script has a companion `run_test.go`; the tracing producer also has a
`main_test.go`. Tests build and execute the real collector against local HTTP
fixtures, using temporary journals and test credentials. No external backend or
Docker is required. The shared internal test helper only builds the executable;
application code imports no collector packages.

For full backend validation, use the separate
[integration suite](../internal/collector/README.md#testing).
