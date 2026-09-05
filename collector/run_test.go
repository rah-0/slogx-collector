//go:build darwin || dragonfly || freebsd || linux || netbsd || openbsd

package collector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"
)

type destinationFunc func(context.Context, []json.RawMessage) error

func (f destinationFunc) Send(ctx context.Context, records []json.RawMessage) error {
	return f(ctx, records)
}

func captureBatches(batches *[][]string) Destination {
	return destinationFunc(func(_ context.Context, records []json.RawMessage) error {
		var batch []string
		for _, record := range records {
			batch = append(batch, string(record))
		}
		*batches = append(*batches, batch)
		return nil
	})
}

func TestRunPreservesObjectsAndFlushesEOF(t *testing.T) {
	input := "{\"n\":9007199254740993,\"n\":2}\n{\"nested\":{\"n\":[1,2]}}\n{\"last\":true}"
	var batches [][]string
	err := Run(t.Context(), io.NopCloser(strings.NewReader(input)), captureBatches(&batches), Options{
		JournalDir: t.TempDir(), BatchSize: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	want := [][]string{{`{"n":9007199254740993,"n":2}`, `{"nested":{"n":[1,2]}}`}, {`{"last":true}`}}
	if !reflect.DeepEqual(batches, want) {
		t.Fatalf("batches = %#v, want %#v", batches, want)
	}
}

func TestRunLimitsBatchBytes(t *testing.T) {
	var batches [][]string
	err := Run(t.Context(), io.NopCloser(strings.NewReader("{\"n\":1}\n{\"long\":2}\n{\"n\":3}\n")), captureBatches(&batches), Options{
		JournalDir: t.TempDir(), BatchBytes: 16,
	})
	if err != nil {
		t.Fatal(err)
	}
	want := [][]string{{`{"n":1}`}, {`{"long":2}`}, {`{"n":3}`}}
	if !reflect.DeepEqual(batches, want) {
		t.Fatalf("batches = %#v, want %#v", batches, want)
	}
}

func TestRunReplaysRecordLargerThanBatchTarget(t *testing.T) {
	dir := t.TempDir()
	large := `{"payload":"` + strings.Repeat("x", 5<<20) + `"}`
	failure := errors.New("destination rejected batch")
	err := Run(t.Context(), io.NopCloser(strings.NewReader(large+"\n")), destinationFunc(func(_ context.Context, records []json.RawMessage) error {
		if len(records) != 1 || string(records[0]) != large {
			t.Error("large record was split or changed before delivery")
		}
		return failure
	}), Options{JournalDir: dir})
	if !errors.Is(err, failure) {
		t.Fatalf("Run = %v; want destination failure after journaling large record", err)
	}

	var batches [][]string
	err = Run(t.Context(), io.NopCloser(strings.NewReader("{\"next\":true}\n")), captureBatches(&batches), Options{
		JournalDir: dir, BatchBytes: 32,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(batches, [][]string{{large}, {`{"next":true}`}}) {
		t.Fatal("replay did not send the complete large record alone before new input")
	}
}

func TestRunFlushesValidRecordsBeforeInputError(t *testing.T) {
	for _, invalid := range []string{"not-json", "null", "[]", `{"broken":`} {
		t.Run(invalid, func(t *testing.T) {
			var batches [][]string
			err := Run(t.Context(), io.NopCloser(strings.NewReader("{\"ok\":true}\n"+invalid)), captureBatches(&batches), Options{JournalDir: t.TempDir()})
			if err == nil || !strings.Contains(err.Error(), "input line 2") {
				t.Fatalf("error = %v, want invalid second line", err)
			}
			if want := [][]string{{`{"ok":true}`}}; !reflect.DeepEqual(batches, want) {
				t.Fatalf("batches = %#v, want %#v", batches, want)
			}
		})
	}
}

func TestRunInputErrorBoundsRetryDrain(t *testing.T) {
	dir := t.TempDir()
	err := Run(t.Context(), io.NopCloser(strings.NewReader("{\"ok\":true}\ninvalid")), destinationFunc(func(context.Context, []json.RawMessage) error {
		return &RetryError{Err: errors.New("destination unavailable")}
	}), Options{JournalDir: dir, ShutdownTimeout: 10 * time.Millisecond})
	if err == nil || !strings.Contains(err.Error(), "input line 2") || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error = %v, want input error and bounded drain deadline", err)
	}
	var batches [][]string
	if err := Run(t.Context(), io.NopCloser(strings.NewReader("")), captureBatches(&batches), Options{JournalDir: dir}); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(batches, [][]string{{`{"ok":true}`}}) {
		t.Fatalf("replayed batches = %#v", batches)
	}
}

func TestRunFlushesTimerWithOpenInput(t *testing.T) {
	reader, writer := io.Pipe()
	delivered := make(chan string, 1)
	result := make(chan error, 1)
	go func() {
		result <- Run(t.Context(), reader, destinationFunc(func(_ context.Context, records []json.RawMessage) error {
			delivered <- string(records[0])
			return nil
		}), Options{JournalDir: t.TempDir(), FlushInterval: 10 * time.Millisecond})
	}()
	t.Cleanup(func() { _ = writer.Close() })
	if _, err := io.WriteString(writer, "{\"timer\":true}\n"); err != nil {
		t.Fatal(err)
	}
	select {
	case record := <-delivered:
		if record != `{"timer":true}` {
			t.Fatalf("record = %s", record)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("batch did not flush while input remained open")
	}
	_ = writer.Close()
	if err := <-result; err != nil {
		t.Fatal(err)
	}
}

func TestRunCancellationUnblocksInput(t *testing.T) {
	reader, writer := io.Pipe()
	t.Cleanup(func() { _ = writer.Close() })
	ctx, cancel := context.WithCancel(t.Context())
	result := make(chan error, 1)
	inputRead := make(chan struct{})
	input := &readObserver{ReadCloser: reader, firstRead: inputRead}
	go func() {
		result <- Run(ctx, input, destinationFunc(func(context.Context, []json.RawMessage) error {
			t.Error("unexpected destination call for empty input")
			return nil
		}), Options{JournalDir: t.TempDir()})
	}()
	<-inputRead
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("error = %v, want context cancellation", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("cancellation did not unblock input")
	}
}

func TestRunCancellationFlushesPendingBatch(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	var batches [][]string
	calls := 0
	err := Run(ctx, io.NopCloser(strings.NewReader("{\"pending\":true}\n")), destinationFunc(func(sendCtx context.Context, records []json.RawMessage) error {
		calls++
		if calls == 1 {
			cancel()
			<-sendCtx.Done()
			return sendCtx.Err()
		}
		if sendCtx.Err() != nil {
			t.Fatalf("shutdown context already canceled: %v", sendCtx.Err())
		}
		return captureBatches(&batches).Send(sendCtx, records)
	}), Options{JournalDir: t.TempDir(), BatchSize: 1})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context cancellation", err)
	}
	if calls != 2 || !reflect.DeepEqual(batches, [][]string{{`{"pending":true}`}}) {
		t.Fatalf("calls = %d, delivered batches = %#v", calls, batches)
	}
}

func TestRunCancellationDoesNotRetryPermanentFailure(t *testing.T) {
	for _, joined := range []bool{false, true} {
		t.Run(map[bool]string{false: "permanent", true: "joined with cancellation"}[joined], func(t *testing.T) {
			dir := t.TempDir()
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			failure := errors.New("partial batch rejected")
			calls := 0
			err := Run(ctx, io.NopCloser(strings.NewReader("{\"pending\":true}\n")), destinationFunc(func(context.Context, []json.RawMessage) error {
				calls++
				cancel()
				if joined {
					return errors.Join(context.Canceled, failure)
				}
				return failure
			}), Options{JournalDir: dir, BatchSize: 1})
			if !errors.Is(err, failure) || !errors.Is(err, context.Canceled) || calls != 1 {
				t.Fatalf("error = %v, calls = %d; want permanent failure, cancellation, and one call", err, calls)
			}
			var batches [][]string
			if err := Run(t.Context(), io.NopCloser(strings.NewReader("")), captureBatches(&batches), Options{JournalDir: dir}); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(batches, [][]string{{`{"pending":true}`}}) {
				t.Fatalf("unacknowledged batches = %#v", batches)
			}
		})
	}
}

func TestRunInputErrorDoesNotRetryPermanentFailure(t *testing.T) {
	reader, writer := io.Pipe()
	t.Cleanup(func() { _ = writer.Close() })
	started := make(chan struct{})
	result := make(chan error, 1)
	failure := errors.New("partial batch rejected")
	calls := 0
	dir := t.TempDir()
	go func() {
		result <- Run(t.Context(), reader, destinationFunc(func(ctx context.Context, _ []json.RawMessage) error {
			calls++
			if calls == 1 {
				close(started)
				<-ctx.Done()
			}
			return failure
		}), Options{JournalDir: dir, BatchSize: 1})
	}()
	if _, err := io.WriteString(writer, "{\"pending\":true}\n"); err != nil {
		t.Fatal(err)
	}
	<-started
	if _, err := io.WriteString(writer, "invalid\n"); err != nil {
		t.Fatal(err)
	}
	err := <-result
	if !errors.Is(err, failure) || !strings.Contains(err.Error(), "input line 2") || calls != 1 {
		t.Fatalf("error = %v, calls = %d; want permanent failure, input error, and one call", err, calls)
	}
}

func TestRunReplaysFailedDelivery(t *testing.T) {
	dir := t.TempDir()
	failure := errors.New("destination unavailable")
	err := Run(t.Context(), io.NopCloser(strings.NewReader("{\"first\":1}\n{\"second\":2}\n")), destinationFunc(func(context.Context, []json.RawMessage) error {
		return failure
	}), Options{JournalDir: dir})
	if !errors.Is(err, failure) {
		t.Fatalf("error = %v, want destination error", err)
	}
	var batches [][]string
	err = Run(t.Context(), io.NopCloser(strings.NewReader("")), captureBatches(&batches), Options{JournalDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	if want := [][]string{{`{"first":1}`, `{"second":2}`}}; !reflect.DeepEqual(batches, want) {
		t.Fatalf("replayed batches = %#v, want %#v", batches, want)
	}
	batches = nil
	if err := Run(t.Context(), io.NopCloser(strings.NewReader("")), captureBatches(&batches), Options{JournalDir: dir}); err != nil {
		t.Fatal(err)
	}
	if len(batches) != 0 {
		t.Fatalf("acknowledged records replayed: %#v", batches)
	}
}

func TestRunJournalsWhileDeliveryBlocked(t *testing.T) {
	inputEOF := make(chan struct{})
	input := &readObserver{ReadCloser: io.NopCloser(strings.NewReader(strings.Repeat("{\"n\":1}\n", 100))), eof: inputEOF}
	started := make(chan struct{})
	release := make(chan struct{})
	result := make(chan error, 1)
	var batches [][]string
	go func() {
		first := true
		result <- Run(t.Context(), input, destinationFunc(func(ctx context.Context, records []json.RawMessage) error {
			if first {
				first = false
				close(started)
				<-release
			}
			return captureBatches(&batches).Send(ctx, records)
		}), Options{JournalDir: t.TempDir(), BatchSize: 1})
	}()
	<-started
	select {
	case <-inputEOF:
	case <-time.After(5 * time.Second):
		close(release)
		t.Fatal("input ingestion stopped while destination was blocked")
	}
	close(release)
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	if len(batches) != 100 {
		t.Fatalf("delivered batches = %d, want 100", len(batches))
	}
}

func TestRunShutdownDeadlinePreservesJournal(t *testing.T) {
	dir := t.TempDir()
	ctx, cancel := context.WithCancel(t.Context())
	err := Run(ctx, io.NopCloser(strings.NewReader("{\"pending\":true}\n")), destinationFunc(func(sendCtx context.Context, _ []json.RawMessage) error {
		cancel()
		<-sendCtx.Done()
		return sendCtx.Err()
	}), Options{JournalDir: dir, BatchSize: 1, ShutdownTimeout: 10 * time.Millisecond})
	if !errors.Is(err, context.Canceled) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error = %v, want cancellation and shutdown deadline", err)
	}
	var batches [][]string
	if err := Run(t.Context(), io.NopCloser(strings.NewReader("")), captureBatches(&batches), Options{JournalDir: dir}); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(batches, [][]string{{`{"pending":true}`}}) {
		t.Fatalf("replayed batches = %#v", batches)
	}
}

func TestSendBatchRetryBackoff(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var delays []time.Duration
		options, err := (Options{
			JournalDir: "unused", RetryInterval: time.Second, MaxRetryInterval: 3 * time.Second,
			OnRetry: func(_ error, delay time.Duration) { delays = append(delays, delay) },
		}).normalized()
		if err != nil {
			t.Fatal(err)
		}
		calls := 0
		start := time.Now()
		err = sendBatch(t.Context(), destinationFunc(func(context.Context, []json.RawMessage) error {
			calls++
			if calls <= 3 {
				return &RetryError{Err: errors.New("retry")}
			}
			if calls == 4 {
				return &RetryError{Err: errors.New("retry later"), After: 5 * time.Second}
			}
			return nil
		}), []json.RawMessage{json.RawMessage(`{}`)}, options)
		if err != nil {
			t.Fatal(err)
		}
		if want := []time.Duration{time.Second, 2 * time.Second, 3 * time.Second, 5 * time.Second}; !reflect.DeepEqual(delays, want) {
			t.Fatalf("delays = %v, want %v", delays, want)
		}
		if elapsed := time.Since(start); elapsed != 11*time.Second {
			t.Fatalf("elapsed = %v, want 11s", elapsed)
		}
	})
}

func TestSendBatchRetryClassificationAndLimit(t *testing.T) {
	for _, retryable := range []bool{false, true} {
		t.Run(map[bool]string{false: "permanent", true: "retryable"}[retryable], func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				failure := errors.New("delivery failed")
				options, err := (Options{JournalDir: "unused", MaxRetries: 2}).normalized()
				if err != nil {
					t.Fatal(err)
				}
				calls := 0
				err = sendBatch(t.Context(), destinationFunc(func(context.Context, []json.RawMessage) error {
					calls++
					if retryable {
						return &RetryError{Err: failure}
					}
					return failure
				}), nil, options)
				if !errors.Is(err, failure) {
					t.Fatalf("error = %v, want original delivery failure", err)
				}
				want := 1
				if retryable {
					want = 3
				}
				if calls != want {
					t.Fatalf("attempts = %d, want %d", calls, want)
				}
			})
		})
	}
}

func TestSendBatchCancellationDuringRetry(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		options, err := (Options{JournalDir: "unused", OnRetry: func(error, time.Duration) { cancel() }}).normalized()
		if err != nil {
			t.Fatal(err)
		}
		calls := 0
		err = sendBatch(ctx, destinationFunc(func(context.Context, []json.RawMessage) error {
			calls++
			return &RetryError{Err: errors.New("retry")}
		}), nil, options)
		if !errors.Is(err, context.Canceled) || calls != 1 {
			t.Fatalf("error = %v, attempts = %d", err, calls)
		}
	})
}

func TestSendBatchPreservesExhaustedLimitDuringCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		options, err := (Options{JournalDir: "unused", MaxRetries: 1}).normalized()
		if err != nil {
			t.Fatal(err)
		}
		failure := errors.New("destination unavailable")
		calls := 0
		err = sendBatch(ctx, destinationFunc(func(context.Context, []json.RawMessage) error {
			calls++
			if calls == 2 {
				cancel()
			}
			return &RetryError{Err: failure}
		}), nil, options)
		if _, ok := errors.AsType[*terminalError](err); !ok || !errors.Is(err, failure) || calls != 2 {
			t.Fatalf("error = %v, calls = %d; want terminal exhausted limit after two calls", err, calls)
		}
	})
}

func TestCancellationOnly(t *testing.T) {
	failure := errors.New("permanent failure")
	for _, test := range []struct {
		name string
		err  error
		want bool
	}{
		{"plain", context.Canceled, true},
		{"wrapped", fmt.Errorf("send: %w", context.Canceled), true},
		{"joined cancellation", errors.Join(context.Canceled, context.Canceled), true},
		{"joined permanent", errors.Join(context.Canceled, failure), false},
		{"wrapped joined permanent", fmt.Errorf("send: %w", errors.Join(context.Canceled, failure)), false},
		{"other context error", context.DeadlineExceeded, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := cancellationOnly(test.err, context.Canceled); got != test.want {
				t.Fatalf("cancellationOnly(%v) = %v, want %v", test.err, got, test.want)
			}
		})
	}
}

func TestRunValidatesOptionsAndClosesInput(t *testing.T) {
	cases := map[string]Options{
		"missing journal": {},
		"negative size":   {JournalDir: "unused", BatchSize: -1},
		"negative bytes":  {JournalDir: "unused", BatchBytes: -1},
		"negative flush":  {JournalDir: "unused", FlushInterval: -1},
		"negative retry":  {JournalDir: "unused", RetryInterval: -1},
		"negative max":    {JournalDir: "unused", MaxRetryInterval: -1},
		"negative count":  {JournalDir: "unused", MaxRetries: -1},
		"negative stop":   {JournalDir: "unused", ShutdownTimeout: -1},
		"retry too big":   {JournalDir: "unused", RetryInterval: 2 * time.Second, MaxRetryInterval: time.Second},
	}
	for name, options := range cases {
		t.Run(name, func(t *testing.T) {
			input := &closeObserver{Reader: strings.NewReader("")}
			err := Run(t.Context(), input, destinationFunc(func(context.Context, []json.RawMessage) error {
				t.Fatal("invalid options reached destination")
				return nil
			}), options)
			if err == nil || !input.closed {
				t.Fatalf("error = %v, input closed = %v", err, input.closed)
			}
		})
	}
}

func TestRunPreservesInputCloseError(t *testing.T) {
	closeErr := errors.New("input close failed")
	deliveryErr := errors.New("delivery failed")
	for _, test := range []struct {
		name string
		err  error
	}{
		{name: "successful drain"},
		{name: "delivery failure", err: deliveryErr},
	} {
		t.Run(test.name, func(t *testing.T) {
			input := &closeObserver{Reader: strings.NewReader("{}\n"), closeErr: closeErr}
			err := Run(t.Context(), input, destinationFunc(func(context.Context, []json.RawMessage) error {
				return test.err
			}), Options{JournalDir: t.TempDir()})
			if !errors.Is(err, closeErr) || (test.err != nil && !errors.Is(err, test.err)) {
				t.Fatalf("Run = %v; want close error and delivery error %v", err, test.err)
			}
			if input.closeCalls != 1 {
				t.Fatalf("input closed %d times; want once", input.closeCalls)
			}
		})
	}
}

type closeObserver struct {
	io.Reader
	closed     bool
	closeErr   error
	closeCalls int
}

func (r *closeObserver) Close() error {
	r.closed = true
	r.closeCalls++
	return r.closeErr
}

type readObserver struct {
	io.ReadCloser
	firstRead chan struct{}
	eof       chan struct{}
	readOnce  sync.Once
	eofOnce   sync.Once
}

func (r *readObserver) Read(p []byte) (int, error) {
	if r.firstRead != nil {
		r.readOnce.Do(func() { close(r.firstRead) })
	}
	n, err := r.ReadCloser.Read(p)
	if errors.Is(err, io.EOF) && r.eof != nil {
		r.eofOnce.Do(func() { close(r.eof) })
	}
	return n, err
}
