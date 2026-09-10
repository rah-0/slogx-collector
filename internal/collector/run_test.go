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
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/rah-0/slogx-collector/internal/journal"
	"github.com/rah-0/slogx-collector/internal/jsonx"
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
	// Keep the input open between deliveries so each new record must wake the
	// consumer again after the previous batch drains.
	for _, want := range []string{`{"timer":1}`, `{"timer":2}`, `{"timer":3}`} {
		if _, err := io.WriteString(writer, want+"\n"); err != nil {
			t.Fatal(err)
		}
		select {
		case record := <-delivered:
			if record != want {
				t.Fatalf("record = %s, want %s", record, want)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("batch did not flush while input remained open")
		}
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
	dir := t.TempDir()
	ctx, cancel := context.WithCancel(t.Context())
	reader, writer := io.Pipe()
	inputEOF := make(chan struct{})
	input := &readObserver{ReadCloser: reader, eof: inputEOF}
	started := make(chan struct{})
	release := make(chan struct{})
	result := make(chan error, 1)
	var batches [][]string
	t.Cleanup(func() {
		cancel()
		_ = writer.Close()
		_ = reader.Close()
		select {
		case <-result:
		case <-time.After(5 * time.Second):
			t.Error("collector did not stop during cleanup")
		}
	})
	go func() {
		defer close(result)
		first := true
		result <- Run(ctx, input, destinationFunc(func(ctx context.Context, records []json.RawMessage) error {
			if first {
				first = false
				close(started)
				select {
				case <-release:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			return captureBatches(&batches).Send(ctx, records)
		}), Options{JournalDir: dir, BatchSize: 1})
	}()
	writeInput := func(records string) {
		t.Helper()
		written := make(chan error, 1)
		go func() {
			_, err := io.WriteString(writer, records)
			written <- err
		}()
		select {
		case err := <-written:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("input ingestion stopped while writing records")
		}
	}
	writeInput("{\"n\":1}\n")
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("collector did not start delivering the first record")
	}
	// The rest of the input arrives only after Send is blocked. Reaching EOF
	// here proves intake can continue independently of delivery.
	writeInput(strings.Repeat("{\"n\":1}\n", 99))
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-inputEOF:
	case <-time.After(5 * time.Second):
		t.Fatal("input ingestion stopped while destination was blocked")
	}
	close(release)
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("collector did not drain after releasing the destination")
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
	store, err := journal.Open(t.TempDir(), "")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	err = ingest(t.Context(), strings.NewReader("{}\n"), store, nil)
	if !errors.Is(err, journal.ErrClosed) {
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
			const recordBytes = 1024
			const prefix = `{"time":"2026-09-05T12:34:56.123456789Z","level":"INFO","msg":"request completed","http":{"method":"GET","status":200},"request_id":"a1b2c3d4","padding":"`
			const suffix = `"}`
			record := prefix + strings.Repeat("x", recordBytes-len(prefix)-len(suffix)) + suffix
			input := strings.Repeat(record+"\n", tc.records)
			dir := b.TempDir()
			j, err := journal.Open(dir, "benchmark")
			if err != nil {
				b.Fatal(err)
			}
			if err := j.Close(); err != nil {
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

func TestRunFieldsPreservesUnconfiguredRecords(t *testing.T) {
	const record = `{ "n":9007199254740993, "n":2, "nested": { "value":true } }`
	for _, tc := range []struct {
		name   string
		fields map[string]string
	}{
		{name: "nil"},
		{name: "empty", fields: map[string]string{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var batches [][]string
			err := Run(t.Context(), io.NopCloser(strings.NewReader(record+"\n")), captureBatches(&batches), Options{
				JournalDir: t.TempDir(), Fields: tc.fields,
			})
			if err != nil {
				t.Fatal(err)
			}
			if want := [][]string{{record}}; !reflect.DeepEqual(batches, want) {
				t.Fatalf("batches = %#v, want original record bytes %#v", batches, want)
			}
		})
	}
}

func TestRunFieldsReplaysOriginalFields(t *testing.T) {
	dir := t.TempDir()
	failure := errors.New("destination rejected batch")
	var failedBatches [][]string
	err := Run(t.Context(), io.NopCloser(strings.NewReader(`{"id":1,"version":"source"}`)), destinationFunc(func(ctx context.Context, records []json.RawMessage) error {
		if err := captureBatches(&failedBatches).Send(ctx, records); err != nil {
			return err
		}
		return failure
	}), Options{JournalDir: dir, Fields: map[string]string{"version": "v1"}})
	if !errors.Is(err, failure) {
		t.Fatalf("Run = %v, want destination failure", err)
	}
	if len(failedBatches) != 1 || len(failedBatches[0]) != 1 {
		t.Fatalf("failed batches = %#v, want one record", failedBatches)
	}

	var batches [][]string
	err = Run(t.Context(), io.NopCloser(strings.NewReader(`{"id":2}`)), captureBatches(&batches), Options{
		JournalDir: dir, Fields: map[string]string{"version": "v2", "new": "current"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(batches) != 1 || len(batches[0]) != 2 {
		t.Fatalf("batches = %#v, want replay before new record", batches)
	}
	if batches[0][0] != failedBatches[0][0] {
		t.Fatalf("replayed record = %s, want saved bytes %s", batches[0][0], failedBatches[0][0])
	}
	for index, want := range []map[string]json.RawMessage{
		{"id": json.RawMessage(`1`), "version": json.RawMessage(`"v1"`)},
		{"id": json.RawMessage(`2`), "version": json.RawMessage(`"v2"`), "new": json.RawMessage(`"current"`)},
	} {
		var got map[string]json.RawMessage
		if err := json.Unmarshal([]byte(batches[0][index]), &got); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("record %d = %s, want %v", index, batches[0][index], want)
		}
	}
}

func TestRunFieldsFlushesBeforeInputError(t *testing.T) {
	var batches [][]string
	err := Run(t.Context(), io.NopCloser(strings.NewReader("{\"ok\":true}\n{\"broken\":")), captureBatches(&batches), Options{
		JournalDir: t.TempDir(), Fields: map[string]string{"version": "v1"},
	})
	if !errors.Is(err, jsonx.ErrInvalidRecord) || !strings.Contains(err.Error(), "input line 2") {
		t.Fatalf("Run = %v, want invalid second line", err)
	}
	if len(batches) != 1 || len(batches[0]) != 1 {
		t.Fatalf("batches = %#v, want preceding valid record", batches)
	}
	var got map[string]json.RawMessage
	if err := json.Unmarshal([]byte(batches[0][0]), &got); err != nil {
		t.Fatal(err)
	}
	want := map[string]json.RawMessage{"ok": json.RawMessage(`true`), "version": json.RawMessage(`"v1"`)}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("record = %s, want %v", batches[0][0], want)
	}
}
