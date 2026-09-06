# Static fields

With `slogx-collector` installed, run from the repository root:

```sh
export LOG_ENDPOINT='https://logs.example.com/api/default/fields/_json'
export JOURNAL_DIR='./example-journals/fields'
bash examples/fields/run.sh
```

Replace the endpoint with your server's complete JSON ingestion URL. Trailing
arguments go to the collector, including credential options and more fields:

```sh
bash examples/fields/run.sh \
  -username ingest@example.com -password-env LOG_INGEST_PASSWORD \
  -field deployment=blue
```

Set the selected password environment variable before running that command.
`COLLECTOR_BIN` can select a different collector executable.

The script supplies `environment` twice; the last value, `staging`, wins. Both
records also receive `service=catalog`, replacing any matching input value.
Fields supplied afterward on the command line can override the script's fields.
Field values are strings and are attached before journaling, so replay retains
the values originally saved with each record.

The sample includes integers above `2^53`. Adding fields retains their exact
digits and nested JSON values. Re-encoding can change whitespace and field order;
duplicate root keys resolve to the last value.

The test builds the collector and runs this script against a loopback HTTP
fixture, checking field overrides, nested values, and exact integers. It needs
no credentials or external backend:

```sh
go test -count=1 -race -cover -covermode=atomic ./examples/fields
```
