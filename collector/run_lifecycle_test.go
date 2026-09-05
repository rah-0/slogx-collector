//go:build darwin || dragonfly || freebsd || linux || netbsd || openbsd

package collector

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestRunPermanentFailureClosesBlockedInput(t *testing.T) {
	reader, writer := io.Pipe()
	t.Cleanup(func() {
		if err := writer.Close(); err != nil {
			t.Error(err)
		}
	})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	input := &lifecycleInput{ReadCloser: reader, reads: make(chan struct{}, 2)}
	failure := errors.New("destination rejected record")
	release := make(chan struct{})
	result := make(chan error, 1)
	dir := t.TempDir()
	go func() {
		result <- Run(ctx, input, destinationFunc(func(ctx context.Context, records []json.RawMessage) error {
			if len(records) != 1 || string(records[0]) != `{"pending":true}` {
				t.Errorf("batch = %s, want the pending record", records)
			}
			select {
			case <-release:
				return failure
			case <-ctx.Done():
				return ctx.Err()
			}
		}), Options{JournalDir: dir, BatchSize: 1})
	}()
	select {
	case <-input.reads:
	case <-time.After(5 * time.Second):
		t.Fatal("collector did not start reading input")
	}
	if _, err := io.WriteString(writer, "{\"pending\":true}\n"); err != nil {
		t.Fatal(err)
	}
	select {
	case <-input.reads:
	case <-time.After(5 * time.Second):
		t.Fatal("producer did not resume reading while delivery was blocked")
	}
	close(release)
	select {
	case err := <-result:
		if !errors.Is(err, failure) {
			t.Fatalf("Run = %v, want the destination failure", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("destination failure did not unblock input")
	}
	if calls := input.closeCalls.Load(); calls != 1 {
		t.Fatalf("input closed %d times, want once", calls)
	}
}

func TestRunCancellationPreservesBlockedInputCloseError(t *testing.T) {
	reader, writer := io.Pipe()
	t.Cleanup(func() {
		if err := writer.Close(); err != nil {
			t.Error(err)
		}
	})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	closeErr := errors.New("input close failed")
	input := &lifecycleInput{ReadCloser: reader, reads: make(chan struct{}, 1), closeErr: closeErr}
	result := make(chan error, 1)
	dir := t.TempDir()
	go func() {
		result <- Run(ctx, input, destinationFunc(func(context.Context, []json.RawMessage) error {
			t.Error("unexpected delivery for empty input")
			return nil
		}), Options{JournalDir: dir})
	}()
	select {
	case <-input.reads:
	case <-time.After(5 * time.Second):
		t.Fatal("collector did not start reading input")
	}
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) || !errors.Is(err, closeErr) {
			t.Fatalf("Run = %v, want cancellation and input close failure", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("cancellation did not unblock input")
	}
	if calls := input.closeCalls.Load(); calls != 1 {
		t.Fatalf("input closed %d times, want once", calls)
	}
}

func TestIngestJournalFailureIsTerminal(t *testing.T) {
	journal, err := openJournal(t.TempDir(), "")
	if err != nil {
		t.Fatal(err)
	}
	if err := journal.close(); err != nil {
		t.Fatal(err)
	}
	err = ingest(t.Context(), strings.NewReader("{}\n"), journal, nil)
	if !errors.Is(err, ErrJournalClosed) {
		t.Fatalf("ingest = %v, want closed journal error", err)
	}
	if _, ok := errors.AsType[*terminalError](err); !ok {
		t.Fatalf("ingest = %v, want terminal journal failure", err)
	}
}

type lifecycleInput struct {
	io.ReadCloser
	reads      chan struct{}
	closeErr   error
	closeCalls atomic.Int32
}

func (r *lifecycleInput) Read(p []byte) (int, error) {
	select {
	case r.reads <- struct{}{}:
	default:
	}
	return r.ReadCloser.Read(p)
}

func (r *lifecycleInput) Close() error {
	r.closeCalls.Add(1)
	return errors.Join(r.ReadCloser.Close(), r.closeErr)
}
