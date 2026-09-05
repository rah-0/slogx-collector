package collector

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

// BenchmarkObjectReader measures steady NDJSON framing and JSON validation.
// One operation reads one record; reader setup and fixture creation are excluded.
// Throughput includes line endings and uses an in-memory source, not pipe I/O.
func BenchmarkObjectReader(b *testing.B) {
	for _, tc := range []struct {
		name       string
		size       int
		lineEnding string
	}{
		{name: "256B", size: 256, lineEnding: "\n"},
		{name: "4KiB_CRLF", size: 4 << 10, lineEnding: "\r\n"},
		{name: "1MiB", size: 1 << 20, lineEnding: "\n"},
	} {
		b.Run(tc.name, func(b *testing.B) {
			const prefix = `{"level":"info","message":"`
			const suffix = `","sequence":9007199254740993}`
			record := prefix + strings.Repeat("x", tc.size-len(prefix)-len(suffix)) + suffix
			line := []byte(record + tc.lineEnding)
			reader := newObjectReader(&repeatingInput{data: line})
			b.SetBytes(int64(len(line)))
			b.ReportAllocs()
			var last json.RawMessage
			for b.Loop() {
				got, err := reader.next()
				if err != nil {
					b.Fatal(err)
				}
				if len(got) != len(record) {
					b.Fatalf("record length = %d, want %d", len(got), len(record))
				}
				last = got
			}
			if reader.line != b.N {
				b.Fatalf("records read = %d, want %d", reader.line, b.N)
			}
			if !bytes.Equal(last, []byte(record)) {
				b.Fatal("record contents changed")
			}
		})
	}
}

// repeatingInput supplies a continuous stream without per-record resets or
// allocations, filling reads across record boundaries as a pipe can.
type repeatingInput struct {
	data   []byte
	offset int
}

func (r *repeatingInput) Read(p []byte) (int, error) {
	for n := 0; n < len(p); {
		copied := copy(p[n:], r.data[r.offset:])
		n += copied
		r.offset += copied
		if r.offset == len(r.data) {
			r.offset = 0
		}
	}
	return len(p), nil
}
