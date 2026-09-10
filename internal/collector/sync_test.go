//go:build darwin || dragonfly || freebsd || linux || netbsd || openbsd

package collector

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"testing/synctest"
	"time"
)

func TestRunSyncsEverySecondWithOpenInput(t *testing.T) {
	dir := t.TempDir()
	synctest.Test(t, func(t *testing.T) {
		reader, writer := io.Pipe()
		defer writer.Close()
		done := make(chan error, 1)
		batches := make(chan string, 2)
		go func() {
			done <- Run(t.Context(), reader, destinationFunc(func(_ context.Context, records []json.RawMessage) error {
				batches <- string(records[0])
				return nil
			}), Options{JournalDir: dir, BatchSize: 1})
		}()
		for _, want := range []string{`{"group":1}`, `{"group":2}`} {
			if _, err := io.WriteString(writer, want+"\n"); err != nil {
				t.Fatal(err)
			}
			synctest.Wait()
			time.Sleep(time.Second - time.Nanosecond)
			synctest.Wait()
			select {
			case record := <-batches:
				t.Fatalf("record delivered before sync: %s", record)
			default:
			}
			time.Sleep(time.Nanosecond)
			synctest.Wait()
			select {
			case record := <-batches:
				if record != want {
					t.Fatalf("after sync: got %s, want %s", record, want)
				}
			default:
				t.Fatal("record not delivered after sync")
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
		var replay [][]string
		if err := Run(t.Context(), io.NopCloser(strings.NewReader("")), captureBatches(&replay), Options{JournalDir: dir}); err != nil {
			t.Fatal(err)
		}
		if len(replay) != 0 {
			t.Fatalf("clean EOF did not checkpoint delivery: %v", replay)
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

func TestRunCheckpointFailureUnblocksInput(t *testing.T) {
	dir := t.TempDir()
	synctest.Test(t, func(t *testing.T) {
		reader, writer := io.Pipe()
		defer writer.Close()
		done := make(chan error, 1)
		var batches [][]string
		go func() {
			done <- Run(t.Context(), reader, captureBatches(&batches), Options{JournalDir: dir, BatchSize: 1})
		}()
		if _, err := io.WriteString(writer, "{\"id\":1}\n"); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		time.Sleep(time.Second)
		synctest.Wait()
		if len(batches) != 1 {
			t.Fatalf("batches = %v; want delivered record awaiting checkpoint", batches)
		}
		// A directory at the temporary checkpoint path forces the next save to
		// fail while ingestion is blocked on an open, idle input stream.
		if err := os.Mkdir(filepath.Join(dir, "state.tmp"), 0o700); err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Second)
		if err := <-done; err == nil {
			t.Fatal("Run ignored checkpoint failure")
		} else if _, ok := errors.AsType[*terminalError](err); !ok {
			t.Fatalf("Run = %v; want terminal journal failure", err)
		}
		if _, err := io.WriteString(writer, "{\"id\":2}\n"); !errors.Is(err, io.ErrClosedPipe) {
			t.Fatalf("write after failure = %v; want closed input", err)
		}
	})
}

func TestRunCheckpointsWhileEOFDeliveryIsBlocked(t *testing.T) {
	dir := t.TempDir()
	synctest.Test(t, func(t *testing.T) {
		blocked := make(chan struct{})
		release := make(chan struct{})
		done := make(chan error, 1)
		failure := errors.New("second batch failed")
		go func() {
			calls := 0
			done <- Run(t.Context(), io.NopCloser(strings.NewReader("{\"id\":1}\n{\"id\":2}\n")), destinationFunc(func(context.Context, []json.RawMessage) error {
				calls++
				if calls == 1 {
					return nil
				}
				close(blocked)
				<-release
				return failure
			}), Options{JournalDir: dir, BatchSize: 1})
		}()
		<-blocked
		synctest.Wait()
		readOffset := func() int64 {
			t.Helper()
			data, err := os.ReadFile(filepath.Join(dir, "state.json"))
			if err != nil {
				t.Fatal(err)
			}
			var state struct{ Offset int64 }
			if err := json.Unmarshal(data, &state); err != nil {
				t.Fatal(err)
			}
			return state.Offset
		}
		if offset := readOffset(); offset != 0 {
			t.Fatalf("checkpoint advanced before group timer: %d", offset)
		}
		time.Sleep(time.Second)
		synctest.Wait()
		if offset := readOffset(); offset == 0 {
			t.Fatal("checkpoint timer stopped at input EOF")
		}
		close(release)
		if err := <-done; !errors.Is(err, failure) {
			t.Fatalf("Run = %v, want second batch failure", err)
		}
		var replay [][]string
		if err := Run(t.Context(), io.NopCloser(strings.NewReader("")), captureBatches(&replay), Options{JournalDir: dir}); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(replay, [][]string{{`{"id":2}`}}) {
			t.Fatalf("replay = %v; want failed batch only", replay)
		}
	})
}

func TestRunDeliversFullJournalGroupWithoutTimer(t *testing.T) {
	dir := t.TempDir()
	synctest.Test(t, func(t *testing.T) {
		reader, writer := io.Pipe()
		defer writer.Close()
		done := make(chan error, 1)
		var batches [][]string
		go func() {
			done <- Run(t.Context(), reader, captureBatches(&batches), Options{JournalDir: dir})
		}()
		start := time.Now()
		if _, err := io.WriteString(writer, strings.Repeat("{\"id\":1}\n", 500)); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		if len(batches) != 1 || len(batches[0]) != 500 || time.Since(start) != 0 {
			t.Fatalf("full group waited for timer: batches=%d elapsed=%v", len(batches), time.Since(start))
		}
		if err := writer.Close(); err != nil {
			t.Fatal(err)
		}
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	})
}
