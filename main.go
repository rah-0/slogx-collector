// Command slogx-collector journals newline-delimited JSON from stdin and forwards
// batches to a configured destination.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/rah-0/slogx-collector/collector"
	"github.com/rah-0/slogx-collector/collector/openobserve"
)

type commandConfig struct {
	input           string
	destination     string
	endpoint        string
	username        string
	password        string
	passwordEnv     string
	passwordFile    string
	headers         repeatedFlag
	headerEnv       repeatedFlag
	requestTimeout  time.Duration
	timestampField  string
	timestampLayout string
	options         collector.Options
}

type repeatedFlag []string

func (f *repeatedFlag) String() string { return strings.Join(*f, ", ") }
func (f *repeatedFlag) Set(value string) error {
	*f = append(*f, value)
	return nil
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	code := run(ctx, os.Args[1:], os.Stdin, os.Stdout, os.Stderr)
	stop()
	os.Exit(code)
}

func run(ctx context.Context, args []string, input io.ReadCloser, stdout, stderr io.Writer) int {
	cfg, err := parseConfig(args, stdout)
	if errors.Is(err, flag.ErrHelp) {
		return 0
	}
	if err != nil {
		fmt.Fprintln(stderr, "slogx-collector:", err)
		return 2
	}
	destination, err := cfg.openObserve()
	if err != nil {
		fmt.Fprintln(stderr, "slogx-collector:", err)
		return 2
	}
	target := sha256.Sum256([]byte("openobserve:" + cfg.endpoint))
	cfg.options.JournalKey = hex.EncodeToString(target[:])
	cfg.options.OnRetry = func(err error, delay time.Duration) {
		fmt.Fprintf(stderr, "slogx-collector: %v; retrying in %s\n", err, delay)
	}
	if err := collector.Run(ctx, input, destination, cfg.options); err != nil {
		fmt.Fprintln(stderr, "slogx-collector:", err)
		return 1
	}
	return 0
}

func parseConfig(args []string, stdout io.Writer) (commandConfig, error) {
	var cfg commandConfig
	flags := flag.NewFlagSet("slogx-collector", flag.ContinueOnError)
	// The flag package includes raw arguments in some errors. Keep its output
	// private so misspelled options and invalid secret values cannot be logged.
	flags.SetOutput(io.Discard)
	flags.Usage = func() {}
	flags.StringVar(&cfg.input, "input", "stdin", "Input source (stdin)")
	flags.StringVar(&cfg.destination, "destination", "", "Destination type (required: openobserve)")
	flags.StringVar(&cfg.endpoint, "endpoint", "", "Complete OpenObserve JSON ingestion URL (required)")
	flags.StringVar(&cfg.options.JournalDir, "journal-dir", "", "Persistent journal directory (required; no disk quota)")
	flags.StringVar(&cfg.username, "username", "", "HTTP Basic authentication username")
	flags.StringVar(&cfg.password, "password", "", "HTTP Basic password (visible in process arguments)")
	flags.StringVar(&cfg.passwordEnv, "password-env", "", "Environment variable containing the HTTP Basic password")
	flags.StringVar(&cfg.passwordFile, "password-file", "", "File containing the HTTP Basic password (trailing line endings removed)")
	flags.Var(&cfg.headers, "header", "Additional HTTP header as Name=Value; repeatable")
	flags.Var(&cfg.headerEnv, "header-env", "HTTP header as Name=ENV, reading its value from ENV; repeatable")
	flags.StringVar(&cfg.timestampField, "timestamp-field", "time", "Source field copied to _timestamp; empty disables mapping")
	flags.StringVar(&cfg.timestampLayout, "timestamp-layout", "", "Go layout for string timestamps (default: RFC3339Nano or slogx UTC layout)")
	flags.DurationVar(&cfg.requestTimeout, "request-timeout", 10*time.Second, "Timeout for each HTTP request")
	flags.IntVar(&cfg.options.BatchSize, "batch-size", 500, "Maximum records per delivery batch")
	flags.IntVar(&cfg.options.BatchBytes, "batch-bytes", 4*1024*1024, "Target JSON bytes per delivery batch; larger records are sent alone")
	flags.DurationVar(&cfg.options.FlushInterval, "flush-interval", time.Second, "Maximum wait before sending a partial batch")
	flags.DurationVar(&cfg.options.RetryInterval, "retry-interval", time.Second, "Initial delay between delivery retries")
	flags.DurationVar(&cfg.options.MaxRetryInterval, "max-retry-interval", 30*time.Second, "Maximum retry backoff (Retry-After may be longer)")
	flags.IntVar(&cfg.options.MaxRetries, "max-retries", 0, "Maximum retries per batch; zero retries indefinitely")
	flags.DurationVar(&cfg.options.ShutdownTimeout, "shutdown-timeout", 10*time.Second, "Time allowed to drain the journal after interruption")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			fmt.Fprintln(stdout, "Usage: slogx-collector -destination openobserve -endpoint URL -journal-dir DIR [options]")
			fmt.Fprintln(stdout, "\nReads newline-delimited JSON objects from stdin and journals them before delivery.")
			fmt.Fprintln(stdout, "\nOptions:")
			flags.SetOutput(stdout)
			flags.PrintDefaults()
			return cfg, flag.ErrHelp
		}
		return cfg, ErrInvalidOptions
	}
	if flags.NArg() != 0 {
		return cfg, ErrPositionalArguments
	}
	if cfg.input != "stdin" {
		return cfg, ErrUnsupportedInput
	}
	if cfg.destination != "openobserve" {
		return cfg, ErrUnsupportedDestination
	}
	if cfg.endpoint == "" || cfg.options.JournalDir == "" {
		return cfg, ErrMissingPaths
	}
	if cfg.requestTimeout <= 0 || cfg.options.BatchSize <= 0 || cfg.options.BatchBytes <= 0 || cfg.options.FlushInterval <= 0 || cfg.options.RetryInterval <= 0 || cfg.options.MaxRetryInterval <= 0 || cfg.options.ShutdownTimeout <= 0 {
		return cfg, ErrNonpositiveLimits
	}
	if cfg.options.MaxRetries < 0 {
		return cfg, ErrNegativeMaxRetries
	}
	if cfg.options.MaxRetryInterval < cfg.options.RetryInterval {
		return cfg, ErrRetryIntervalOrder
	}
	passwordOptions := 0
	emptyPasswordSource := false
	flags.Visit(func(f *flag.Flag) {
		if f.Name == "password" || f.Name == "password-env" || f.Name == "password-file" {
			passwordOptions++
		}
		if f.Name == "password-env" && cfg.passwordEnv == "" || f.Name == "password-file" && cfg.passwordFile == "" {
			emptyPasswordSource = true
		}
	})
	if passwordOptions > 1 {
		return cfg, ErrPasswordSourceConflict
	}
	if emptyPasswordSource {
		return cfg, ErrEmptyPasswordSource
	}
	return cfg, nil
}

func (cfg commandConfig) openObserve() (*openobserve.Destination, error) {
	headers := make(http.Header)
	for _, setting := range cfg.headers {
		name, value, ok := strings.Cut(setting, "=")
		if !ok || name == "" {
			return nil, ErrInvalidHeader
		}
		headers.Add(name, value)
	}
	for _, setting := range cfg.headerEnv {
		name, variable, ok := strings.Cut(setting, "=")
		if !ok || name == "" || variable == "" {
			return nil, ErrInvalidHeaderEnv
		}
		value, ok := os.LookupEnv(variable)
		if !ok || value == "" {
			return nil, ErrEmptyHeaderEnv
		}
		headers.Add(name, value)
	}
	if cfg.passwordEnv != "" {
		value, ok := os.LookupEnv(cfg.passwordEnv)
		if !ok || value == "" {
			return nil, ErrEmptyPasswordEnv
		}
		cfg.password = value
	}
	if cfg.passwordFile != "" {
		data, err := os.ReadFile(cfg.passwordFile)
		if err != nil {
			return nil, ErrPasswordFileRead
		}
		cfg.password = strings.TrimRight(string(data), "\r\n")
		if cfg.password == "" {
			return nil, ErrEmptyPasswordFile
		}
	}
	return openobserve.New(openobserve.Config{
		Endpoint:        cfg.endpoint,
		Username:        cfg.username,
		Password:        cfg.password,
		Headers:         headers,
		Timeout:         cfg.requestTimeout,
		TimestampField:  cfg.timestampField,
		TimestampLayout: cfg.timestampLayout,
	})
}
