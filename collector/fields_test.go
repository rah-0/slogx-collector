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
)

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

func TestRunFieldsOverridesInputValues(t *testing.T) {
	for _, input := range []string{
		`{}`,
		`{"version":"from input"}`,
		`{"version":42}`,
		`{"version":{"nested":true}}`,
		`{"version":null}`,
		`{"version":"first","versi\u006fn":"second"}`,
	} {
		t.Run(input, func(t *testing.T) {
			var batches [][]string
			err := Run(t.Context(), io.NopCloser(strings.NewReader(input)), captureBatches(&batches), Options{
				JournalDir: t.TempDir(), Fields: map[string]string{"version": "configured"},
			})
			if err != nil {
				t.Fatal(err)
			}
			// Only one key may remain, including when input uses duplicate or
			// escaped spellings of the configured name.
			if want := [][]string{{`{"version":"configured"}`}}; !reflect.DeepEqual(batches, want) {
				t.Fatalf("batches = %#v, want %#v", batches, want)
			}
		})
	}
}

func TestRunFieldsPreservesJSONValues(t *testing.T) {
	const input = `{"version":42,"n":9007199254740993123456789,"decimal":1.234567890123456789,"nested":{"version":"nested","n":9007199254740993},"Version":"capital"}`
	fields := map[string]string{
		"version":        "v1",
		"build\"tag\\路径": "line\n=\"β\"",
		"empty":          "",
	}
	var batches [][]string
	err := Run(t.Context(), io.NopCloser(strings.NewReader(input)), captureBatches(&batches), Options{
		JournalDir: t.TempDir(), Fields: fields,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(batches) != 1 || len(batches[0]) != 1 {
		t.Fatalf("batches = %#v, want one record", batches)
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal([]byte(batches[0][0]), &object); err != nil {
		t.Fatal(err)
	}
	if len(object) != 7 {
		t.Fatalf("object = %s, want seven fields", batches[0][0])
	}
	for key, want := range map[string]string{
		"n":       "9007199254740993123456789",
		"decimal": "1.234567890123456789",
		"nested":  `{"version":"nested","n":9007199254740993}`,
		"Version": `"capital"`,
	} {
		if got := string(object[key]); got != want {
			t.Errorf("%q = %s, want %s", key, got, want)
		}
	}
	for key, want := range fields {
		var got string
		if err := json.Unmarshal(object[key], &got); err != nil {
			t.Fatalf("decode field %q: %v", key, err)
		}
		if got != want {
			t.Errorf("%q = %q, want %q", key, got, want)
		}
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
	if !errors.Is(err, ErrInvalidJSONRecord) || !strings.Contains(err.Error(), "input line 2") {
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
