// Package exampletest builds the collector executable for CLI example tests.
package exampletest

import (
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
)

// BuildCollector builds from a case directory under examples into a temporary directory.
func BuildCollector(t *testing.T) string {
	t.Helper()
	switch runtime.GOOS {
	case "darwin", "dragonfly", "freebsd", "linux", "netbsd", "openbsd":
	default:
		t.Skip("the durable journal requires a supported Unix platform")
	}
	binary := filepath.Join(t.TempDir(), "slogx-collector")
	command := exec.CommandContext(t.Context(), "go", "build", "-o", binary, "../..")
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("build collector: %v\n%s", err, output)
	}
	return binary
}
