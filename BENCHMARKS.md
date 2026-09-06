# Benchmarks

The benchmarks cover NDJSON parsing, durable journal writes, restart recovery and
replay, the collection loop, and OpenObserve request preparation. They use the Go
standard library and do not need Docker.

## Running

From the repository root:

```sh
go test -run '^$' -bench . -benchmem \
  ./internal/collector/... ./internal/journal ./internal/jsonx
```

Journal benchmarks use `testing.B.TempDir`, which follows `TMPDIR` on Unix. Choose
a directory on the filesystem used for journals when measuring durable writes.
On Linux, for example:

```sh
bench_tmp=$(mktemp -d /var/tmp/slogx-collector-bench.XXXXXX)
TMPDIR="$bench_tmp" GOWORK=off GOGC=100 GOMEMLIMIT=off \
  taskset -c 0 go test -p=1 -run '^$' -bench . -benchmem \
  -benchtime=1s -count=5 -cpu=1 \
  ./internal/collector/... ./internal/journal ./internal/jsonx
rmdir "$bench_tmp"
```

Choose an available CPU for `taskset`; omit it on platforms without that command.
Go removes successful benchmarks' temporary journals. Disk synchronization can
make the suite take several minutes, including untimed fixture creation. A
memory-backed temporary filesystem measures a different workload.

## What each operation measures

| Benchmark | One operation | Scope |
| --- | --- | --- |
| `ObjectReader` | One JSON record | Steady input framing and validation from memory; includes line-ending bytes in throughput. |
| `JournalAppend` | 100 records | Disk writes and one sync per record; replay and acknowledgment cleanup are untimed. |
| `JournalReplay` | 500 records | Reopen, active-segment recovery, checksum/JSON validation, and close; fixture writes are untimed. |
| `Run` | 100 or 500 records | Complete collection and drain, including durable writes and checkpoints, into an in-memory destination. |
| `MapTimestamp` | One 256-byte record | Validation, field scanning, timestamp parsing, and insertion when enabled. |
| `Send` | 500 256-byte records | Mapping, JSON array construction, HTTP request/client work, and acknowledgment parsing through an in-memory transport. |

`Run` uses 1 KiB records and the default one-second flush interval. Its `batch`
parameter is the maximum batch size; slow intake can trigger smaller timed
flushes. Journal replay repeats the same backlog, so filesystem reads can be
served from the operating system's page cache.

`Send` excludes sockets, TLS, network latency, and OpenObserve ingestion/indexing.
Its throughput measures client work, not the capacity of an OpenObserve server.
The parser's larger records mostly contain string padding; other JSON shapes
can have different costs. `B/op` is total heap allocation, not peak memory use.

## Measurements

Measured on 2026-09-05 with Go 1.27.1, linux/amd64, on an AMD Ryzen AI 9 HX 370.
The journal directory was on ext4 backed by a Samsung SSD 990 PRO 2TB. The run
used Linux 6.12.105, CPU 0, `GOMAXPROCS=1`, `GOGC=100`, no memory limit, and the
toolchain's default experiment settings. Other tests and benchmark processes were
not run concurrently; this was a development machine, not an isolated performance host.

Figures below are medians of five one-second samples from one complete run.
Slow disk operations can exceed the requested sample duration. Latency ranges
show the minimum and maximum sample, not individual-record tail latency.
The `Run` and journal figures precede the coordination and journal readability
refactors and serve as their baseline.

### Input parsing

| Record | Time/record | Throughput | B/op | Allocs/op |
| --- | ---: | ---: | ---: | ---: |
| 256 B, LF | 207 ns | 1,243 MB/s | 288 | 1 |
| 4 KiB, CRLF | 1.87 µs | 2,193 MB/s | 8,984 | 3 |
| 1 MiB, LF | 429 µs | 2,442 MB/s | 2,121,130 | 266 |

Large lines allocate fragments plus the assembled record through
`bufio.Reader.ReadBytes`. The 1 MiB case allocates about twice the input size.
This is a possible future improvement for large-record workloads, but parsing
is much faster than durable intake in this run.

### Journal and collection

All sizes below exclude frame headers. `Run` throughput includes input newlines.
Replay figures include recovery and reads, with a warm filesystem cache.

| Operation | Records/op | Median time/op | Sample range | Records/s | B/op | Allocs/op |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Append, 256 B | 100 | 632 ms | 631–638 ms | 158 | 11,200 | 100 |
| Append, 1 KiB | 100 | 665 ms | 509–742 ms | 150 | 11,200 | 100 |
| Reopen/replay, 256 B | 500 | 1.16 ms | 1.14–1.21 ms | 432,653 | 16,562,205 | 2,064 |
| Reopen/replay, 1 KiB | 500 | 1.34 ms | 1.29–1.34 ms | 373,800 | 16,946,302 | 2,065 |
| Run, batch maximum 100 | 100 | 670 ms | 667–673 ms | 149 | 248,104 | 405 |
| Run, batch maximum 500 | 500 | 3.27 s | 3.00–3.27 s | 153 | 1,167,824 | 1,655 |
| Run, batch maximum 50 | 500 | 2.72 s | 2.67–3.37 s | 184 | 1,176,336 | 1,813 |

Syncing each input record dominates intake on this filesystem: median append
time is roughly 6–7 ms per record. Disk samples vary enough that these results
do not establish a benefit from reducing the delivery batch size. Delivery
batching does not remove the per-record journal sync.

Grouping journal writes before syncing could improve intake, provided records
only become available for delivery after their group's sync succeeds. That
requires a deliberate choice about buffering and flush latency; these
benchmarks leave the existing durability behavior unchanged.

### Recovery improvement experiment

An allocation profile of the 1 KiB replay benchmark attributed 96.5% of allocated
bytes to `io.copyBuffer`, called by `recoverSegment`. Recovery currently creates
a fresh CRC object and an implicit 32 KiB copy buffer for each record.

A separate prototype reused one CRC object and one lazily allocated 32 KiB
buffer for the segment, calling `hash.Reset()` and `io.CopyBuffer` for each
record. This buffer size controls scratch space, not record length; larger
records continue to stream through it. No file format, checksum, or sync logic
was changed. The prototype is **not applied to the collector**.

Using the same 500-record fixtures, filesystem, CPU settings, and five-sample
method:

| Record | Baseline time | Prototype time | Speedup | Baseline B/op | Prototype B/op | Allocation reduction |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 256 B | 1.16 ms | 459 µs | 2.52× | 16,562,205 | 188,884 | 98.9% |
| 1 KiB | 1.34 ms | 641 µs | 2.09× | 16,946,302 | 572,939 | 96.6% |

Existing journal tests passed against the prototype, covering recovery,
corruption, truncation, extended frames, and replay. This is the clearest small
optimization found: it reduces restart allocation pressure without changing
the delivery contract. It does not improve the per-record sync bottleneck.

### OpenObserve client work

Each timestamp operation uses a 256-byte JSON object. The existing-timestamp
case places `_timestamp` first, allowing an early return; timestamps later in
the object require more scanning.

| Timestamp mode | Time/record | B/op | Allocs/op |
| --- | ---: | ---: | ---: |
| Mapping disabled; JSON still validated | 143 ns | 0 | 0 |
| RFC3339 | 2.45 µs | 1,488 | 34 |
| slogx format, automatic detection | 2.65 µs | 1,616 | 37 |
| slogx format, explicit layout | 2.49 µs | 1,488 | 34 |
| Existing `_timestamp` first | 638 ns | 568 | 10 |

Specifying the known slogx timestamp layout avoids a failed RFC3339 parse and
saves three allocations per record. Timing differences are small, with
overlapping sample ranges; the allocation reduction is more consistent.

Each request below contains 500 records. Mapping uses automatic slogx timestamp
detection, matching the CLI default for that input format.

| Request mode | Time/batch | Records/s | B/op | Allocs/op |
| --- | ---: | ---: | ---: | ---: |
| Mapping disabled | 112 µs | 4,476,958 | 330,041 | 57 |
| Map `time` to `_timestamp` | 1.43 ms | 350,445 | 1,138,099 | 18,557 |

Timestamp mapping accounts for most of the measured client CPU and allocation
cost. Its scan preserves existing timestamps, exact field names, duplicate-key
behavior, and raw JSON values; an optimization must retain those contracts.
Even with mapping enabled, client work is far faster than journal intake on
this machine. These figures exclude the OpenObserve server and network.
