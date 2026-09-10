# OTLP trace delivery

This package runs inside the collector executable. It converts Slogx's completed
span records into OTLP HTTP/JSON requests and delivers them to a compatible traces
endpoint, including OpenObserve. OTLP is the transmission protocol; the configured
endpoint determines the receiving backend.
Applications use Slogx's normal JSON output; they do not import this package or
produce OTLP payloads.

## Input and conversion

Each input line is a JSON record. Completed spans have a root `span` group with
trace and span identifiers, parent identity, kind, start and end times, status,
and any recorded events. Ordinary log records can share the same input. The
collector journals these JSON records before converting them for delivery.

Keep Slogx's reserved `span` group at the record root. Applications can use
`slog.Group` for operation attributes while leaving the default logger outside
an active `WithGroup`. Renaming required metadata with `ReplaceAttr`, removing
it, or changing its type prevents trace conversion. Redaction of application
and error attributes takes place in the application's native slog handler,
before data reaches the collector.

The destination converts root application attributes, including context
attributes and `source`, into span attributes. Native source metadata retains
its `function`, `file`, and `line` fields. Structured errors retain `err` and
nested wrapping attributes. Event groups under `span.events` retain their
order, timestamps, messages, and structured attributes. Objects and arrays stay
nested. Integers outside OTLP's signed 64-bit range and fractions that overflow
`float64` or underflow to zero retain their original text as strings, for example
`9223372036854775808` and `1e-400`. Other fractional numbers retain their JSON text
in `doubleValue`; the receiving backend interprets them as double precision values.
The application's JSON output has already applied native struct tags and marshalers,
including fields excluded with `json:"-"`. OTLP's
[KeyValueList contract](https://github.com/open-telemetry/opentelemetry-proto/blob/v1.9.0/opentelemetry/proto/common/v1/common.proto)
requires unique attribute keys, so the last duplicate key wins during conversion.

`-resource Name=Value` supplies trace resource attributes such as `service.name`.
These values belong to collector configuration and are included in the journal
identity. Changing resources while records are pending is rejected, preventing
spans from being relabelled on replay. A fully drained journal can adopt
new resources after its durable checkpoint is validated at startup.
`-field Name=Value` instead adds or overrides root record attributes before
journaling; on spans, these become span attributes.

## Command configuration

For a single input containing both logs and completed spans, use
`-destination openobserve` with a logs `-endpoint` and an OTLP
`-traces-endpoint`. Records with a root `span` group are converted and sent to
the traces endpoint; other records go to the logs endpoint. Credentials and
headers are shared by both destinations. See the
[application and command example](../../../examples/traces).

For trace-only delivery, select the generic `-destination otlp` output adapter
and provide the traces URL as `-endpoint`. Ordinary logs are skipped. Batch limits count
input records and their JSON bytes, rather than encoded OTLP bytes.

The destination also accepts compact OTLP trace envelopes containing
`resourceSpans`. Their resource and scope objects are retained. Configured
`-resource` attributes apply only when converting native span records; they do
not overwrite resources already present in an OTLP envelope. Outer envelope
fields other than `resourceSpans` are not copied into the merged request.

Supply the complete HTTP(S) ingestion path. The destination rejects redirects.
The command accepts Basic authentication or an explicit `Authorization` header;
these authentication options cannot be combined.

For OpenObserve, use `/api/<organization>/v1/traces` as the traces endpoint and
set `-header stream-name=<stream>` to select its traces stream. Configure
`-resource service.name=<service>` to identify the producing application.

## Delivery and recovery

The destination converts a batch's span records into one OTLP request. Invalid
span records fail before an HTTP attempt. A batch containing only ordinary logs
is a no-op for the trace destination. In mixed delivery, traces are sent before
logs; a batch is acknowledged only after both destinations succeed.

Each `Destination.Send` calls `http.Client.Do` at most once. Go's transport can
reconnect and resend within that call, for example after a reused connection fails
on a replayable request carrying an `Idempotency-Key` header. See the standard
[transport retry conditions](https://pkg.go.dev/net/http#Transport).
The request timeout defaults to ten seconds; a shorter caller deadline takes precedence. Attempt
timeouts are retryable, while caller cancellation and deadlines return directly.
Responses are limited to 4 MiB after decompression. Delivery sentinels live in
[errors.go](errors.go) and remain identifiable with `errors.Is`, including
through `collector.RetryError`.

Network failures and HTTP 429, 502, 503, and 504 return `collector.RetryError`,
including any `Retry-After` delay. `collector.Run` owns retry scheduling and
durable storage. HTTP 200 requires a valid OTLP acknowledgment. Positive
partial rejections and malformed acknowledgments stop collection and leave the
batch in the journal. OTLP does not identify rejected spans individually, so
the collector cannot acknowledge or retry only that subset. Zero rejected
spans, including warning-only responses, acknowledge the trace request.

The journal checkpoints accepted batches before reclaiming their records.
Delivery is at least once: lost acknowledgments or a crash before checkpointing
can produce duplicates with the original trace and span IDs. In mixed delivery,
if one destination accepts its records and the other fails, replay can resend
the accepted records too.

Application writes only confirm that their configured output accepted the
bytes. Durability begins when the collector writes and syncs each JSON record
to its journal. Stop the producer and allow the pipe and collector to drain
for a complete shutdown.

The integration suite checks that OpenObserve v0.92.2 preserves nested error
groups and stores the sample's boolean and integer event attributes as strings.
The collector retains their scalar types in the OTLP request.
