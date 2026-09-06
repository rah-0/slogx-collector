package cli

import (
	"errors"
	"io"
	"testing"
)

func TestConfigurationErrorsCanBeReferenced(t *testing.T) {
	for _, test := range []struct {
		args []string
		want error
	}{
		{[]string{"-unknown"}, ErrInvalidOptions},
		{[]string{"-input", "file"}, ErrUnsupportedInput},
		{[]string{"-destination", ""}, ErrUnsupportedDestination},
		{[]string{"-request-timeout", "-1s"}, ErrNonpositiveLimits},
		{[]string{"-password", "value", "-password-env", "NAME"}, ErrPasswordSourceConflict},
		{[]string{"-field", "missing-separator"}, ErrInvalidField},
	} {
		args := []string{"-destination", "openobserve", "-endpoint", "http://localhost:5080", "-journal-dir", t.TempDir()}
		_, err := parseConfig(append(args, test.args...), io.Discard)
		if !errors.Is(err, test.want) {
			t.Fatalf("configuration error = %v, want %v", err, test.want)
		}
	}
}

func TestMixedTraceConfigurationValidation(t *testing.T) {
	base := []string{"-destination", "openobserve", "-endpoint", "http://localhost:5080/api/default/logs/_json", "-journal-dir", t.TempDir()}
	for _, test := range []struct {
		args []string
		want error
	}{
		{[]string{"-resource", "service.name=catalog"}, ErrTraceResourceDestination},
		{[]string{"-traces-endpoint", "http://localhost:5080/api/default/v1/traces", "-resource", "=catalog"}, ErrInvalidResource},
	} {
		_, err := parseConfig(append(append([]string(nil), base...), test.args...), io.Discard)
		if !errors.Is(err, test.want) {
			t.Fatalf("config error = %v, want %v", err, test.want)
		}
	}
}
