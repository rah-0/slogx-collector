package collector

import (
	"errors"
	"io"
	"strings"
	"testing"
	"testing/iotest"
)

func TestObjectReaderPreservesGenericJSON(t *testing.T) {
	const record = `{"time":42,"level":{"custom":true},"n":9007199254740993,"x":1,"x":2}`
	reader := newObjectReader(strings.NewReader("\n  " + record + "\r\n{}"))
	for _, want := range []string{record, "{}"} {
		got, err := reader.next()
		if err != nil || string(got) != want {
			t.Fatalf("next = %s, %v; want %s", got, err, want)
		}
	}
	if _, err := reader.next(); !errors.Is(err, io.EOF) {
		t.Fatalf("end = %v, want EOF", err)
	}
}

func TestObjectReaderRejectsInvalidRecords(t *testing.T) {
	for _, input := range []string{"null", "[]", "123", `"secret"`, `{"secret":`, "{}{}", "{\n}", "\v{}", "{}\v"} {
		t.Run(input, func(t *testing.T) {
			_, err := newObjectReader(strings.NewReader(input)).next()
			if !errors.Is(err, ErrInvalidJSONRecord) || strings.Contains(err.Error(), "secret") {
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
		reader := newObjectReader(strings.NewReader(whitespace + "\n" + whitespace + record + whitespace + suffix))
		got, err := reader.next()
		if err != nil || string(got) != record {
			t.Fatalf("large record with %q: got %d bytes, %v; want %d bytes", suffix, len(got), err, len(record))
		}
		if _, err := reader.next(); !errors.Is(err, io.EOF) {
			t.Fatalf("end = %v, want EOF", err)
		}
	}
	if _, err := newObjectReader(strings.NewReader(whitespace)).next(); !errors.Is(err, io.EOF) {
		t.Fatalf("blank input = %v, want EOF", err)
	}
}

func TestObjectReaderReadFailure(t *testing.T) {
	for _, partial := range []string{"", "{}", `{"secret":`} {
		input := io.MultiReader(strings.NewReader("{}\n"+partial), iotest.ErrReader(errors.New("secret source")))
		reader := newObjectReader(input)
		if got, err := reader.next(); err != nil || string(got) != "{}" {
			t.Fatalf("complete line = %s, %v; want {}", got, err)
		}
		got, err := reader.next()
		if got != nil || !errors.Is(err, ErrInputRead) || !strings.Contains(err.Error(), "line 2") || strings.Contains(err.Error(), "secret") {
			t.Fatalf("read failure = %s, %v; want input error at line 2", got, err)
		}
	}
}
