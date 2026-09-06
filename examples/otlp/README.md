# Trace-only OTLP delivery

[run.sh](run.sh) runs the [three-layer JSON producer](../traces/main.go) and pipes
its output into a separate collector process with `-destination otlp`. The
collector's generic OTLP adapter converts completed span records into OTLP
HTTP/JSON requests and skips ordinary logs in the same input. OTLP is the
transmission protocol; the endpoint can belong to OpenObserve or another
compatible backend.

Install `slogx-collector`, or set `COLLECTOR_BIN` to the absolute path of a built
executable. Running the producer requires Go. From the repository root:

```sh
export TRACE_ENDPOINT='https://traces.example.com/v1/traces'
export JOURNAL_DIR="$HOME/.local/state/slogx-collector/catalog-traces"
./examples/otlp/run.sh -header-env Authorization=TRACE_AUTHORIZATION
```

Set `TRACE_AUTHORIZATION` to the complete authorization header value in the
launching environment. The script forwards trailing arguments to the collector
and sets `-resource service.name=catalog`. Supply the endpoint's full path;
the collector does not append `/v1/traces` or follow redirects.

OTLP conversion, authentication, and HTTP delivery happen only in the collector.
The producer imports slogx and writes native JSON. Keep the reserved `span`
group at the record root with its metadata unchanged. Journaled records retain
their IDs and attributes on retry or replay; durability begins at journal sync,
and delivery is at least once. Use a dedicated persistent journal for these
destination and resource settings.

The destination also accepts existing OTLP JSON envelopes with `resourceSpans`.
Resources already present in those envelopes are retained; configured resources
apply to native span conversion. See the
[trace delivery contract](../../internal/collector/otlp/README.md) for
acknowledgment and retry behavior, or [mixed logs and traces](../traces) to route
ordinary logs to a separate endpoint.

`run_test.go` executes the script against a local HTTP test server and verifies
that only the three converted spans are delivered. No backend or credentials
are required for the test.
