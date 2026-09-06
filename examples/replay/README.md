# Replay after an outage

With `slogx-collector` installed, set the actual endpoint and a dedicated persistent
journal directory from the repository root:

```sh
export LOG_ENDPOINT='https://logs.example.com/api/default/replay/_json'
export JOURNAL_DIR='./example-journals/replay'
bash examples/replay/run.sh ingest
```

`ingest` reads `events.jsonl`. If the endpoint is unavailable, the command makes
an initial delivery attempt and one retry, then exits unsuccessfully with the
records still journaled. The script uses a two-second request timeout and a
250 ms retry interval to make the outage demonstration finite.

After that same endpoint becomes available, reuse the same environment:

```sh
bash examples/replay/run.sh replay
```

`replay` reads `/dev/null`, adding no new records. It drains the saved backlog
and exits when the endpoint acknowledges every record. Another `replay` then
sends nothing. Running `ingest` again intentionally adds the fixture records
again. Neither mode deletes the journal directory; the collector reclaims
acknowledged segments as part of its normal checkpoint handling.

Both modes accept trailing collector arguments for credentials and other options:

```sh
bash examples/replay/run.sh replay \
  -username ingest@example.com -password-env LOG_INGEST_PASSWORD
```

Set the selected password environment variable before running that command.
`COLLECTOR_BIN` can select a different collector executable. Keep the destination
endpoint unchanged because the journal is bound to its destination identity.
Delivery is at least once: losing an acknowledgment after remote acceptance can
cause duplicates on replay.

The test runs the real CLI against a loopback HTTP fixture: two HTTP 503 responses
leave a backlog, recovery accepts its original JSON, and a third process confirms
that acknowledged records are not sent again. No external outage or credentials
are needed:

```sh
go test -count=1 -race -cover -covermode=atomic ./examples/replay
```
