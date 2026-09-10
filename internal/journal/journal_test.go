//go:build darwin || dragonfly || freebsd || linux || netbsd || openbsd

package journal

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func testJournal(t *testing.T, dir string) *Journal {
	t.Helper()
	j, err := Open(dir, "test-destination")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := j.Close(); err != nil {
			t.Error(err)
		}
	})
	return j
}

func appendJournal(t *testing.T, j *Journal, record string) {
	t.Helper()
	if err := j.Append(json.RawMessage(record)); err != nil {
		t.Fatal(err)
	}
}

func syncJournal(t *testing.T, j *Journal) {
	t.Helper()
	if err := j.Sync(); err != nil {
		t.Fatal(err)
	}
}

func checkpointJournal(t testing.TB, j *Journal) {
	t.Helper()
	if err := j.Checkpoint(); err != nil {
		t.Fatal(err)
	}
}

func nextJournal(t *testing.T, j *Journal, want string) {
	t.Helper()
	record, err := j.Next(0)
	if err != nil || string(record) != want {
		t.Fatalf("next = %s, %v; want %s", record, err, want)
	}
}

func TestJournalReplaysOnlyUnacknowledgedRecords(t *testing.T) {
	dir := t.TempDir()
	j := testJournal(t, dir)
	for _, record := range []string{`{"id":1}`, `{"id":2}`, `{"id":3}`} {
		appendJournal(t, j, record)
	}
	syncJournal(t, j)
	nextJournal(t, j, `{"id":1}`)
	if err := j.Ack(); err != nil {
		t.Fatal(err)
	}
	nextJournal(t, j, `{"id":2}`) // Reading is not acknowledgement.
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	j = testJournal(t, dir)
	nextJournal(t, j, `{"id":2}`)
	nextJournal(t, j, `{"id":3}`)
	if err := j.Ack(); err != nil {
		t.Fatal(err)
	}
	checkpointJournal(t, j)
	if len(j.segments) != 1 || j.segments[0].durableSize != 0 {
		t.Fatalf("acknowledged segments were not reclaimed: %+v", j.segments)
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	j = testJournal(t, dir)
	if _, err := j.Next(0); !errors.Is(err, io.EOF) {
		t.Fatalf("acknowledged record replayed: %v", err)
	}
	appendJournal(t, j, `{"id":4}`)
	syncJournal(t, j)
	nextJournal(t, j, `{"id":4}`)
}

func TestJournalNextDoesNotAdvanceOnBatchLimit(t *testing.T) {
	j := testJournal(t, t.TempDir())
	appendJournal(t, j, `{"id":1}`)
	syncJournal(t, j)
	if _, err := j.Next(1); !errors.Is(err, ErrBatchFull) {
		t.Fatalf("batch limit = %v", err)
	}
	// An empty batch accepts the same record regardless of the batch budget.
	nextJournal(t, j, `{"id":1}`)
}

func TestJournalReplaysRecordLargerThanSegment(t *testing.T) {
	dir := t.TempDir()
	j := testJournal(t, dir)
	record := `{"data":"` + strings.Repeat("x", journalSegmentBytes) + `"}`
	appendJournal(t, j, record)
	appendJournal(t, j, `{"id":2}`)
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	j = testJournal(t, dir)
	nextJournal(t, j, record)
	if err := j.Ack(); err != nil {
		t.Fatal(err)
	}
	nextJournal(t, j, `{"id":2}`)
}

func extendedJournalFrame(record string) []byte {
	frame := make([]byte, 16+len(record))
	binary.LittleEndian.PutUint32(frame[4:8], crc32.ChecksumIEEE([]byte(record)))
	binary.LittleEndian.PutUint64(frame[8:16], uint64(len(record)))
	copy(frame[16:], record)
	return frame
}

func TestJournalReplaysExtendedFramesAcrossCheckpoints(t *testing.T) {
	dir := t.TempDir()
	j := testJournal(t, dir)
	path := filepath.Join(dir, segmentName(j.segments[0].id))
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	// Small payloads exercise the extended format without allocating 4 GiB.
	frames := append(extendedJournalFrame(`{"id":1}`), extendedJournalFrame(`{"id":2}`)...)
	if err := os.WriteFile(path, frames, 0o600); err != nil {
		t.Fatal(err)
	}
	j = testJournal(t, dir)
	nextJournal(t, j, `{"id":1}`)
	if err := j.Ack(); err != nil {
		t.Fatal(err)
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	j = testJournal(t, dir)
	appendJournal(t, j, `{"id":3}`)
	syncJournal(t, j)
	nextJournal(t, j, `{"id":2}`)
	nextJournal(t, j, `{"id":3}`)
}

func TestJournalReadsExtendedLength(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "header")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := file.Close(); err != nil {
			t.Error(err)
		}
	})
	var header [16]byte
	const wantLength = int64(math.MaxUint32) + 1
	binary.LittleEndian.PutUint64(header[8:], uint64(wantLength))
	if _, err := file.Write(header[:]); err != nil {
		t.Fatal(err)
	}
	length, _, headerBytes, err := readJournalHeader(file, 0)
	if err != nil || length != wantLength || headerBytes != 16 {
		t.Fatalf("extended header = %d, %d, %v", length, headerBytes, err)
	}
}

func TestJournalRotatesAndReclaimsSegments(t *testing.T) {
	dir := t.TempDir()
	j := testJournal(t, dir)
	// Two records cannot share a segment; this exercises real rotation without
	// reducing the production segment size or coupling to allocation details.
	record := `{"data":"` + strings.Repeat("x", journalSegmentBytes/2) + `"}`
	appendJournal(t, j, record)
	appendJournal(t, j, record)
	if len(j.segments) != 2 {
		t.Fatalf("segments = %d, want 2", len(j.segments))
	}
	firstID := j.segments[0].id
	nextJournal(t, j, record)
	if err := j.Ack(); err != nil {
		t.Fatal(err)
	}
	checkpointJournal(t, j)
	if _, err := os.Stat(filepath.Join(dir, segmentName(firstID))); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("acknowledged segment still exists: %v", err)
	}
	// The second large record also exceeds the byte threshold and is durable.
	nextJournal(t, j, record)
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	j = testJournal(t, dir)
	nextJournal(t, j, record)
	if err := j.Ack(); err != nil {
		t.Fatal(err)
	}
}

func benchmarkJournalRecord(size int) json.RawMessage {
	const prefix = `{"time":"2026-09-05T12:34:56.123456789Z","level":"INFO","msg":"request completed","http":{"method":"GET","status":200},"request_id":"a1b2c3d4","padding":"`
	const suffix = `"}`
	return json.RawMessage(prefix + strings.Repeat("x", size-len(prefix)-len(suffix)) + suffix)
}

// Each operation stages 100 records and makes them durable with one group sync.
// Untimed replay and acknowledgement reclaim the records after each operation,
// so a longer benchmark does not accumulate an ever-growing disk backlog.
func BenchmarkJournalAppend(b *testing.B) {
	const recordsPerOp = 100
	for _, recordBytes := range []int{256, 1024} {
		b.Run(fmt.Sprintf("record_bytes=%d", recordBytes), func(b *testing.B) {
			record := benchmarkJournalRecord(recordBytes)
			j, err := Open(b.TempDir(), "benchmark")
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() {
				if err := j.Close(); err != nil {
					b.Error(err)
				}
			})
			b.ReportAllocs()
			b.SetBytes(int64(recordsPerOp * len(record)))
			for b.Loop() {
				for range recordsPerOp {
					if err := j.Append(record); err != nil {
						b.Fatal(err)
					}
				}
				if err := j.Sync(); err != nil {
					b.Fatal(err)
				}
				b.StopTimer()
				for range recordsPerOp {
					if _, err := j.Next(0); err != nil {
						b.Fatal(err)
					}
				}
				if _, err := j.Next(0); !errors.Is(err, io.EOF) {
					b.Fatalf("after replay: got %v, want EOF", err)
				}
				if err := j.Ack(); err != nil {
					b.Fatal(err)
				}
				checkpointJournal(b, j)
				b.StartTimer()
			}
			b.ReportMetric(recordsPerOp, "records/op")
			b.ReportMetric(float64(b.N)*recordsPerOp/b.Elapsed().Seconds(), "records/s")
		})
	}
}

// Each operation reopens and replays the same 500-record backlog, including
// recovery checksums and sync, per-record checksum/JSON validation, and journal close.
// Fixture writes happen before timing; replay does not acknowledge or write.
// Repeated reads can use the operating system's page cache.
func BenchmarkJournalReplay(b *testing.B) {
	const recordsPerOp = 500
	for _, recordBytes := range []int{256, 1024} {
		b.Run(fmt.Sprintf("record_bytes=%d", recordBytes), func(b *testing.B) {
			record := benchmarkJournalRecord(recordBytes)
			dir := b.TempDir()
			j, err := Open(dir, "benchmark")
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() {
				if j != nil {
					if err := j.Close(); err != nil {
						b.Error(err)
					}
				}
			})
			for range recordsPerOp {
				if err := j.Append(record); err != nil {
					b.Fatal(err)
				}
			}
			if err := j.Close(); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.SetBytes(int64(recordsPerOp * len(record)))
			for b.Loop() {
				j, err = Open(dir, "benchmark")
				if err != nil {
					b.Fatal(err)
				}
				for range recordsPerOp {
					if _, err := j.Next(0); err != nil {
						b.Fatal(err)
					}
				}
				if _, err := j.Next(0); !errors.Is(err, io.EOF) {
					b.Fatalf("after replay: got %v, want EOF", err)
				}
				if err := j.Close(); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(recordsPerOp, "records/op")
			b.ReportMetric(float64(b.N)*recordsPerOp/b.Elapsed().Seconds(), "records/s")
		})
	}
}
