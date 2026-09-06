package check

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/rah-0/slogx-collector/examples/internal/exampletest"
)

func TestRun(t *testing.T) {
	binary := exampletest.BuildCollector(t)
	var requests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		writer.WriteHeader(http.StatusUnauthorized)
	}))
	defer server.Close()
	journal := filepath.Join(t.TempDir(), "journal")
	for _, test := range []struct {
		Name     string
		Password string
		Code     int
	}{
		{Name: "valid credentials", Password: "fixture-password"},
		{Name: "missing credentials", Code: 2},
	} {
		t.Run(test.Name, func(t *testing.T) {
			command := exec.CommandContext(t.Context(), "bash", "run.sh",
				"-username", "fixture", "-password-env", "EXAMPLE_CHECK_PASSWORD")
			command.Env = append(os.Environ(),
				"COLLECTOR_BIN="+binary, "LOG_ENDPOINT="+server.URL,
				"JOURNAL_DIR="+journal, "EXAMPLE_CHECK_PASSWORD="+test.Password,
			)
			// Malformed stdin would fail collection; a check must ignore it.
			command.Stdin = strings.NewReader("not JSON\n")
			output, err := command.CombinedOutput()
			if test.Code == 0 {
				if err != nil || len(output) != 0 {
					t.Fatalf("valid check: %v\n%s", err, output)
				}
			} else {
				var exit *exec.ExitError
				if !errors.As(err, &exit) || exit.ExitCode() != test.Code || len(output) == 0 {
					t.Fatalf("invalid check: %v\n%s", err, output)
				}
			}
			if strings.Contains(string(output), "fixture-password") {
				t.Fatal("check exposed credentials")
			}
			if _, err := os.Stat(journal); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("check touched journal: %v", err)
			}
			if requests.Load() != 0 {
				t.Fatal("check contacted the destination")
			}
		})
	}
}
