package cli

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/rah-0/slogx-collector/internal/collector"
)

const defaultRequestTimeout = 10 * time.Second

type commandConfig struct {
	check           bool
	ready           bool
	input           string
	destination     string
	endpoint        string
	tracesEndpoint  string
	username        string
	password        string
	passwordEnv     string
	passwordFile    string
	headers         repeatedFlag
	headerEnv       repeatedFlag
	fields          repeatedFlag
	resources       repeatedFlag
	resource        map[string]any
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

func parseConfig(args []string, stdout io.Writer) (commandConfig, error) {
	var cfg commandConfig
	flags := flag.NewFlagSet("slogx-collector", flag.ContinueOnError)
	// flag prints unknown option names and rejected values, for example
	// -batch-size=secret-value. Discard its diagnostics to keep arguments private.
	flags.SetOutput(io.Discard)
	flags.Usage = func() {}
	flags.BoolVar(&cfg.check, "check", false, "Validate configuration and credential sources, then exit without reading input, opening the journal, or contacting the destination")
	flags.BoolVar(&cfg.ready, "ready", false, "Write ready to stdout after opening the journal, before reading input or delivering records")
	flags.StringVar(&cfg.input, "input", "stdin", "Input source (stdin)")
	flags.StringVar(&cfg.destination, "destination", "", "Output adapter (required: openobserve or otlp; see adapters above)")
	flags.StringVar(&cfg.endpoint, "endpoint", "", "Complete ingestion URL for the selected output adapter (required)")
	flags.StringVar(&cfg.tracesEndpoint, "traces-endpoint", "", "OpenObserve OTLP HTTP/JSON traces URL; enables separate span routing with -destination openobserve")
	flags.StringVar(&cfg.options.JournalDir, "journal-dir", "", "Persistent journal directory (required; no disk quota)")
	flags.StringVar(&cfg.username, "username", "", "HTTP Basic authentication username")
	flags.StringVar(&cfg.password, "password", "", "HTTP Basic password (visible in process arguments)")
	flags.StringVar(&cfg.passwordEnv, "password-env", "", "Environment variable containing the HTTP Basic password")
	flags.StringVar(&cfg.passwordFile, "password-file", "", "File containing the HTTP Basic password (trailing line endings removed)")
	flags.Var(&cfg.headers, "header", "Additional HTTP header as Name=Value; repeatable")
	flags.Var(&cfg.headerEnv, "header-env", "HTTP header as Name=ENV, reading its value from ENV; repeatable")
	flags.Var(&cfg.fields, "field", "Record string field as Name=Value added before journaling; overrides input; repeatable")
	flags.Var(&cfg.resources, "resource", "Trace resource string attribute as Name=Value, for example service.name=catalog; repeatable")
	flags.StringVar(&cfg.timestampField, "timestamp-field", "time", "Log source field copied to _timestamp; empty disables mapping")
	flags.StringVar(&cfg.timestampLayout, "timestamp-layout", "", "Go layout for log timestamps (default: RFC3339Nano or slogx UTC layout)")
	flags.DurationVar(&cfg.requestTimeout, "request-timeout", defaultRequestTimeout, "Timeout for each HTTP request")
	flags.IntVar(&cfg.options.BatchSize, "batch-size", collector.DefaultBatchSize, "Maximum JSON records per delivery batch")
	flags.IntVar(&cfg.options.BatchBytes, "batch-bytes", collector.DefaultBatchBytes, "Target JSON bytes per delivery batch; larger records are sent alone")
	flags.DurationVar(&cfg.options.FlushInterval, "flush-interval", collector.DefaultFlushInterval, "Maximum wait before sending a partial batch")
	flags.DurationVar(&cfg.options.RetryInterval, "retry-interval", collector.DefaultRetryInterval, "Initial delay between delivery retries")
	flags.DurationVar(&cfg.options.MaxRetryInterval, "max-retry-interval", collector.DefaultMaxRetryInterval, "Maximum retry backoff (Retry-After may be longer)")
	flags.IntVar(&cfg.options.MaxRetries, "max-retries", 0, "Maximum retries per batch; zero retries indefinitely")
	flags.DurationVar(&cfg.options.ShutdownTimeout, "shutdown-timeout", collector.DefaultShutdownTimeout, "Time allowed to drain the journal after interruption")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			fmt.Fprintln(stdout, "Usage: slogx-collector -destination ADAPTER -endpoint URL -journal-dir DIR [options]")
			fmt.Fprintln(stdout, "\nReads newline-delimited JSON objects from stdin and journals them before delivery.")
			fmt.Fprintln(stdout, "\nOutput adapters:")
			fmt.Fprintln(stdout, "  openobserve  OpenObserve backend integration: JSON logs and optional OTLP traces.")
			fmt.Fprintln(stdout, "  otlp         Generic OTLP HTTP/JSON trace export to a compatible backend, including OpenObserve; skips ordinary logs.")
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
	if cfg.destination != "openobserve" && cfg.destination != "otlp" {
		return cfg, ErrUnsupportedDestination
	}
	if cfg.tracesEndpoint != "" && cfg.destination != "openobserve" {
		return cfg, ErrTraceEndpointDestination
	}
	if len(cfg.resources) != 0 && cfg.destination != "otlp" && cfg.tracesEndpoint == "" {
		return cfg, ErrTraceResourceDestination
	}
	if cfg.destination == "otlp" {
		logOptions := false
		flags.Visit(func(f *flag.Flag) {
			if f.Name == "timestamp-field" || f.Name == "timestamp-layout" {
				logOptions = true
			}
		})
		if logOptions {
			return cfg, ErrTraceLogOptions
		}
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
	for _, field := range cfg.fields {
		name, value, ok := strings.Cut(field, "=")
		if !ok || name == "" {
			return cfg, ErrInvalidField
		}
		if cfg.options.Fields == nil {
			cfg.options.Fields = make(map[string]string)
		}
		cfg.options.Fields[name] = value
	}
	for _, attribute := range cfg.resources {
		name, value, ok := strings.Cut(attribute, "=")
		if !ok || name == "" {
			return cfg, ErrInvalidResource
		}
		if cfg.resource == nil {
			cfg.resource = make(map[string]any)
		}
		cfg.resource[name] = value
	}
	return cfg, nil
}
