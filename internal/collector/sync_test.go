//go:build darwin || dragonfly || freebsd || linux || netbsd || openbsd

package collector

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/rah-0/slogx-collector/internal/journal"
)

func TestIngestSyncsEverySecondWithOpenInput(t *testing.T) {
	store, err := journal.Open(t.TempDir(), "")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	}()
	synctest.Test(t, func(t *testing.T) {
		reader, writer := io.Pipe()
		defer reader.Close()
		defer writer.Close()
		done := make(chan error, 1)
		go func() {
			done <- ingest(t.Context(), reader, store, nil, func() { _ = reader.Close() })
		}()
		for _, want := range []string{`{"group":1}`, `{"group":2}`} {
			if _, err := io.WriteString(writer, want+"\n"); err != nil {
				t.Fatal(err)
			}
			synctest.Wait()
			time.Sleep(time.Second - time.Nanosecond)
			synctest.Wait()
			if record, err := store.Next(0); !errors.Is(err, io.EOF) {
				t.Fatalf("before sync: Next = %s, %v; want EOF", record, err)
			}
			time.Sleep(time.Nanosecond)
			synctest.Wait()
			record, err := store.Next(0)
			if err != nil || string(record) != want {
				t.Fatalf("after sync: Next = %s, %v; want %s", record, err, want)
			}
		}
		if err := writer.Close(); err != nil {
			t.Fatal(err)
		}
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	})
}

func TestRunEOFSyncsWithoutWaitingForInterval(t *testing.T) {
	dir := t.TempDir()
	synctest.Test(t, func(t *testing.T) {
		start := time.Now()
		var batches [][]string
		err := Run(t.Context(), io.NopCloser(strings.NewReader("{\"final\":true}\n")), captureBatches(&batches), Options{JournalDir: dir})
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(batches, [][]string{{`{"final":true}`}}) {
			t.Fatalf("batches = %#v, want final record", batches)
		}
		if elapsed := time.Since(start); elapsed != 0 {
			t.Fatalf("EOF waited %v; want no timer delay", elapsed)
		}
	})
}

func TestRunCancellationSyncsPendingRecords(t *testing.T) {
	for _, outage := range []bool{false, true} {
		t.Run(map[bool]string{false: "healthy", true: "outage"}[outage], func(t *testing.T) {
			dir := t.TempDir()
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				reader, writer := io.Pipe()
				defer writer.Close()
				done := make(chan error, 1)
				var batches [][]string
				go func() {
					done <- Run(ctx, reader, destinationFunc(func(sendCtx context.Context, records []json.RawMessage) error {
						if outage {
							return &RetryError{Err: errors.New("destination unavailable")}
						}
						return captureBatches(&batches).Send(sendCtx, records)
					}), Options{JournalDir: dir, BatchSize: 1, ShutdownTimeout: 10 * time.Millisecond})
				}()
				if _, err := io.WriteString(writer, "{\"pending\":true}\n"); err != nil {
					t.Fatal(err)
				}
				synctest.Wait()
				if len(batches) != 0 {
					t.Fatal("record delivered before its group was synced")
				}
				start := time.Now()
				cancel()
				err := <-done
				if !errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) != outage {
					t.Fatalf("Run = %v; want cancellation and deadline only during outage", err)
				}
				if elapsed := time.Since(start); elapsed >= time.Second {
					t.Fatalf("shutdown waited %v for periodic sync", elapsed)
				}
				if outage {
					if err := Run(t.Context(), io.NopCloser(strings.NewReader("")), captureBatches(&batches), Options{JournalDir: dir}); err != nil {
						t.Fatal(err)
					}
				}
				if !reflect.DeepEqual(batches, [][]string{{`{"pending":true}`}}) {
					t.Fatalf("batches = %#v, want pending record once", batches)
				}
			})
		})
	}
}

func TestIngestSyncFailureUnblocksInput(t *testing.T) {
	store, err := journal.Open(t.TempDir(), "")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	synctest.Test(t, func(t *testing.T) {
		reader, writer := io.Pipe()
		defer writer.Close()
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		start := time.Now()
		err := ingest(ctx, reader, store, nil, func() {
			cancel()
			_ = reader.Close()
		})
		if _, ok := errors.AsType[*terminalError](err); !ok || !errors.Is(err, journal.ErrClosed) {
			t.Fatalf("ingest = %v; want terminal journal sync failure", err)
		}
		if elapsed := time.Since(start); elapsed != time.Second {
			t.Fatalf("sync failure after %v; want first one-second tick", elapsed)
		}
	})
}
