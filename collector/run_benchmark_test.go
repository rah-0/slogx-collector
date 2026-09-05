//go:build darwin || dragonfly || freebsd || linux || netbsd || openbsd

package collector

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"testing"
)

// Each operation collects and drains a complete NDJSON input to a destination
// that counts records without network I/O. Timing includes input parsing, a
// journal reopen and close, durable appends, and batch checkpoints. Successful
// runs reclaim their records; the initialized, empty journal directory is reused.
// Batch size is a maximum: the default one-second flush can send smaller batches.
func BenchmarkRun(b *testing.B) {
	for _, tc := range []struct {
		records   int
		batchSize int
	}{
		{records: 100, batchSize: 100},
		{records: 500, batchSize: 500},
		{records: 500, batchSize: 50},
	} {
		b.Run(fmt.Sprintf("records=%d/batch=%d", tc.records, tc.batchSize), func(b *testing.B) {
			input := strings.Repeat(string(benchmarkJournalRecord(1024))+"\n", tc.records)
			dir := b.TempDir()
			j, err := openJournal(dir, "benchmark")
			if err != nil {
				b.Fatal(err)
			}
			if err := j.close(); err != nil {
				b.Fatal(err)
			}
			var received int
			destination := destinationFunc(func(_ context.Context, records []json.RawMessage) error {
				received += len(records)
				return nil
			})
			options := Options{JournalDir: dir, JournalKey: "benchmark", BatchSize: tc.batchSize}
			b.ReportAllocs()
			b.SetBytes(int64(len(input)))
			for b.Loop() {
				received = 0
				if err := Run(b.Context(), io.NopCloser(strings.NewReader(input)), destination, options); err != nil {
					b.Fatal(err)
				}
				if received != tc.records {
					b.Fatalf("received %d records, want %d", received, tc.records)
				}
			}
			b.ReportMetric(float64(tc.records), "records/op")
			b.ReportMetric(float64(b.N)*float64(tc.records)/b.Elapsed().Seconds(), "records/s")
		})
	}
}
