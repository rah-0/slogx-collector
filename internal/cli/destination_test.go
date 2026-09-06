package cli

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestDestinationErrorsCanBeReferenced(t *testing.T) {
	cfg := commandConfig{headers: repeatedFlag{"missing-separator"}}
	_, err := cfg.newDestination()
	if !errors.Is(err, ErrInvalidHeader) {
		t.Fatalf("header error = %v, want %v", err, ErrInvalidHeader)
	}
}

func TestPasswordFile(t *testing.T) {
	wantPassword := " " + strings.Repeat("secret-value", 6000) + " "
	passwordFile := filepath.Join(t.TempDir(), "password")
	if err := os.WriteFile(passwordFile, []byte(wantPassword+"\r\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, password, ok := r.BasicAuth()
		if !ok || password != wantPassword {
			t.Error("password file did not preserve spaces and remove line endings")
		}
		fmt.Fprint(w, `{"code":200,"status":[{"successful":1,"failed":0}]}`)
	}))
	defer server.Close()
	cfg := commandConfig{destination: "openobserve", endpoint: server.URL, username: "user", passwordFile: passwordFile}
	destination, err := cfg.newDestination()
	if err != nil {
		t.Fatal(err)
	}
	if err := destination.Send(t.Context(), []json.RawMessage{json.RawMessage(`{}`)}); err != nil {
		t.Fatal(err)
	}
}

func TestTraceDestinationIdentityIncludesEndpoint(t *testing.T) {
	base := []string{"-destination", "openobserve", "-endpoint", "http://localhost:5080/api/default/logs/_json", "-journal-dir", t.TempDir()}
	args := append(base, "-traces-endpoint", "http://localhost:5080/api/default/v1/traces", "-resource", "service.name=catalog", "-resource", "version=1")
	cfg, err := parseConfig(args, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cfg.newDestination(); err != nil {
		t.Fatal(err)
	}
	original := cfg.options.JournalKey
	cfg.tracesEndpoint += "changed"
	if _, err := cfg.newDestination(); err != nil {
		t.Fatal(err)
	}
	if cfg.options.JournalKey == original {
		t.Fatal("trace endpoint missing from journal identity")
	}
}

func TestDestinationJournalKey(t *testing.T) {
	// These fixed keys protect the binding of existing journals to their destinations.
	for _, test := range []struct {
		name string
		cfg  commandConfig
		want string
	}{
		{
			name: "logs",
			cfg: commandConfig{
				destination: "openobserve",
				endpoint:    "https://logs.example.com/api/default/application/_json",
			},
			want: "6f59fdfb9349a956103c1976f7bcb7635bc4ab04e362dac97b4607c3c75c4eeb",
		},
		{
			name: "otlp",
			cfg: commandConfig{
				destination: "otlp",
				endpoint:    "https://traces.example.com/v1/traces",
				headers:     repeatedFlag{"stream-name=application"},
				resource:    map[string]any{"service.name": "catalog", "version": "1"},
			},
			want: "1fd716c02192e9680c90baaa9e80826ce6e9752ae546d1d1835cc72bc608b789",
		},
		{
			name: "mixed",
			cfg: commandConfig{
				destination:    "openobserve",
				endpoint:       "https://logs.example.com/api/default/application/_json",
				tracesEndpoint: "https://logs.example.com/api/default/v1/traces",
				headers:        repeatedFlag{"Stream-Name=application"},
				resource:       map[string]any{"service.name": "catalog", "version": "1"},
			},
			want: "65793d0775986a10c7a7c17e1faa70fb53c04476ed1000e336914bec929fd408",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := test.cfg.newDestination(); err != nil {
				t.Fatal(err)
			}
			if got := test.cfg.options.JournalKey; got != test.want {
				t.Fatalf("journal key = %q, want %q", got, test.want)
			}
		})
	}
}
