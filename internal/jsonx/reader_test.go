package jsonx

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"
	"testing/iotest"
)

func TestObjectReaderPreservesGenericJSON(t *testing.T) {
	const record = `{"time":42,"level":{"custom":true},"n":9007199254740993,"x":1,"x":2}`
	reader := NewReader(strings.NewReader("\n  " + record + "\r\n{}"))
	for _, want := range []string{record, "{}"} {
		got, err := reader.Next()
		if err != nil || string(got) != want {
			t.Fatalf("next = %s, %v; want %s", got, err, want)
		}
	}
	if _, err := reader.Next(); !errors.Is(err, io.EOF) {
		t.Fatalf("end = %v, want EOF", err)
	}
}

func TestObjectReaderRejectsInvalidRecords(t *testing.T) {
	for _, input := range []string{"null", "[]", "123", `"secret"`, `{"secret":`, "{}{}", "{\n}", "\v{}", "{}\v"} {
		t.Run(input, func(t *testing.T) {
			_, err := NewReader(strings.NewReader(input)).Next()
			if !errors.Is(err, ErrInvalidRecord) || strings.Contains(err.Error(), "secret") {
				t.Fatalf("invalid input error = %v", err)
			}
		})
	}
}

func TestObjectReaderAcceptsLargeRecordsAndWhitespace(t *testing.T) {
	const largeSize = 1<<20 + 1
	record := `{"message":"` + strings.Repeat("x", largeSize) + `"}`
	whitespace := strings.Repeat(" ", largeSize)
	for _, suffix := range []string{"", "\n", "\r\n"} {
		reader := NewReader(strings.NewReader(whitespace + "\n" + whitespace + record + whitespace + suffix))
		got, err := reader.Next()
		if err != nil || string(got) != record {
			t.Fatalf("large record with %q: got %d bytes, %v; want %d bytes", suffix, len(got), err, len(record))
		}
		if _, err := reader.Next(); !errors.Is(err, io.EOF) {
			t.Fatalf("end = %v, want EOF", err)
		}
	}
	if _, err := NewReader(strings.NewReader(whitespace)).Next(); !errors.Is(err, io.EOF) {
		t.Fatalf("blank input = %v, want EOF", err)
	}
}

func TestObjectReaderReadFailure(t *testing.T) {
	for _, partial := range []string{"", "{}", `{"secret":`} {
		input := io.MultiReader(strings.NewReader("{}\n"+partial), iotest.ErrReader(errors.New("secret source")))
		reader := NewReader(input)
		if got, err := reader.Next(); err != nil || string(got) != "{}" {
			t.Fatalf("complete line = %s, %v; want {}", got, err)
		}
		got, err := reader.Next()
		if got != nil || !errors.Is(err, ErrRead) || !strings.Contains(err.Error(), "line 2") || strings.Contains(err.Error(), "secret") {
			t.Fatalf("read failure = %s, %v; want input error at line 2", got, err)
		}
	}
}

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
			reader := NewReader(&repeatingInput{data: line})
			b.SetBytes(int64(len(line)))
			b.ReportAllocs()
			var last json.RawMessage
			for b.Loop() {
				got, err := reader.Next()
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
