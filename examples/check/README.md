# Check configuration

Run from the repository root after installing the collector:

```sh
export LOG_ENDPOINT=https://logs.example.com/api/default/application/_json
export JOURNAL_DIR=/var/lib/slogx-collector/application
bash examples/check/run.sh -username ingest@example.com -password-env LOG_INGEST_PASSWORD
```

Set `LOG_INGEST_PASSWORD` in the launching environment. [run.sh](run.sh) checks
the same CLI options and credential sources used for collection. It exits
silently with status `0` on success, or reports invalid configuration on stderr
with status `2`. It does not read stdin, open the journal, or contact the endpoint.

Pass the intended collection arguments after the script name, including trace
routing flags when applicable. A check can run while another collector owns the
journal. It validates configuration, not remote authentication, availability,
journal health, or filesystem permissions.

Credential alternatives include `-password-file /path/to/password` and
`-header-env Authorization=INGEST_AUTHORIZATION`. Set the selected variable to
the complete header value. Basic authentication and an `Authorization` header
cannot be combined; the three password-source flags are mutually exclusive.

`COLLECTOR_BIN` selects the executable, defaulting to `slogx-collector` on `PATH`.
`go test -count=1 -race -cover -covermode=atomic ./examples/check` exercises the
script with a local HTTP fixture, malformed input, and valid or missing credentials.
