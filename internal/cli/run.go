// Package cli implements the collector command behind its process entrypoint.
package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"time"

	"github.com/rah-0/slogx-collector/internal/collector"
)

const (
	exitSuccess       = 0
	exitFailure       = 1
	exitInvalidConfig = 2
)

// Run executes the collector command using the supplied arguments and streams.
// It returns the process exit status; the caller provides cancellation and signals.
func Run(ctx context.Context, args []string, input io.ReadCloser, stdout, stderr io.Writer) int {
	cfg, err := parseConfig(args, stdout)
	if errors.Is(err, flag.ErrHelp) {
		return exitSuccess
	}
	if err != nil {
		fmt.Fprintln(stderr, "slogx-collector:", err)
		return exitInvalidConfig
	}
	destination, err := cfg.newDestination()
	if err != nil {
		fmt.Fprintln(stderr, "slogx-collector:", err)
		return exitInvalidConfig
	}
	if cfg.check {
		return exitSuccess
	}
	if cfg.ready {
		cfg.options.OnReady = func() error {
			_, err := io.WriteString(stdout, "ready\n")
			return err
		}
	}
	cfg.options.OnRetry = func(err error, delay time.Duration) {
		fmt.Fprintf(stderr, "slogx-collector: %v; retrying in %s\n", err, delay)
	}
	if err := collector.Run(ctx, input, destination, cfg.options); err != nil {
		fmt.Fprintln(stderr, "slogx-collector:", err)
		return exitFailure
	}
	return exitSuccess
}
