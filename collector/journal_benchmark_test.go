//go:build darwin || dragonfly || freebsd || linux || netbsd || openbsd

package collector

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
)

func benchmarkJournalRecord(size int) json.RawMessage {
	const prefix = `{"time":"2026-09-05T12:34:56.123456789Z","level":"INFO","msg":"request completed","http":{"method":"GET","status":200},"request_id":"a1b2c3d4","padding":"`
	const suffix = `"}`
	return json.RawMessage(prefix + strings.Repeat("x", size-len(prefix)-len(suffix)) + suffix)
}

// Each operation durably appends 100 records, including one fsync per record.
// Untimed replay and acknowledgement reclaim the records after each operation,
// so a longer benchmark does not accumulate an ever-growing disk backlog.
func BenchmarkJournalAppend(b *testing.B) {
	const recordsPerOp = 100
	for _, recordBytes := range []int{256, 1024} {
		b.Run(fmt.Sprintf("record_bytes=%d", recordBytes), func(b *testing.B) {
			record := benchmarkJournalRecord(recordBytes)
			j, err := openJournal(b.TempDir(), "benchmark")
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() {
				if err := j.close(); err != nil {
					b.Error(err)
				}
			})
			b.ReportAllocs()
			b.SetBytes(int64(recordsPerOp * len(record)))
			for b.Loop() {
				for range recordsPerOp {
					if err := j.append(record); err != nil {
						b.Fatal(err)
					}
				}
				b.StopTimer()
				for range recordsPerOp {
					if _, err := j.next(0); err != nil {
						b.Fatal(err)
					}
				}
				if _, err := j.next(0); !errors.Is(err, io.EOF) {
					b.Fatalf("after replay: got %v, want EOF", err)
				}
				if err := j.ack(); err != nil {
					b.Fatal(err)
				}
				b.StartTimer()
			}
			b.ReportMetric(recordsPerOp, "records/op")
			b.ReportMetric(float64(b.N)*recordsPerOp/b.Elapsed().Seconds(), "records/s")
		})
	}
}

// Each operation reopens and replays the same 500-record backlog, including
// recovery checksums, per-record checksum/JSON validation, and journal close.
// Fixture writes happen before timing; replay does not acknowledge or write.
// Repeated reads can use the operating system's page cache.
func BenchmarkJournalReplay(b *testing.B) {
	const recordsPerOp = 500
	for _, recordBytes := range []int{256, 1024} {
		b.Run(fmt.Sprintf("record_bytes=%d", recordBytes), func(b *testing.B) {
			record := benchmarkJournalRecord(recordBytes)
			dir := b.TempDir()
			j, err := openJournal(dir, "benchmark")
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() {
				if j != nil {
					if err := j.close(); err != nil {
						b.Error(err)
					}
				}
			})
			for range recordsPerOp {
				if err := j.append(record); err != nil {
					b.Fatal(err)
				}
			}
			if err := j.close(); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.SetBytes(int64(recordsPerOp * len(record)))
			for b.Loop() {
				j, err = openJournal(dir, "benchmark")
				if err != nil {
					b.Fatal(err)
				}
				for range recordsPerOp {
					if _, err := j.next(0); err != nil {
						b.Fatal(err)
					}
				}
				if _, err := j.next(0); !errors.Is(err, io.EOF) {
					b.Fatalf("after replay: got %v, want EOF", err)
				}
				if err := j.close(); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(recordsPerOp, "records/op")
			b.ReportMetric(float64(b.N)*recordsPerOp/b.Elapsed().Seconds(), "records/s")
		})
	}
}
