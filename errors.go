package main

import "errors"

var (
	ErrInvalidOptions         = errors.New("invalid command-line options; use -help for usage")
	ErrPositionalArguments    = errors.New("positional arguments are not supported; use -help for usage")
	ErrUnsupportedInput       = errors.New("-input must be stdin")
	ErrUnsupportedDestination = errors.New("-destination is required and must be openobserve")
	ErrMissingPaths           = errors.New("-endpoint and -journal-dir are required")
	ErrNonpositiveLimits      = errors.New("batch limits, intervals, and timeouts must be positive")
	ErrNegativeMaxRetries     = errors.New("-max-retries must not be negative")
	ErrRetryIntervalOrder     = errors.New("-max-retry-interval must be at least -retry-interval")
	ErrPasswordSourceConflict = errors.New("-password, -password-env, and -password-file are mutually exclusive")
	ErrEmptyPasswordSource    = errors.New("-password-env and -password-file require a nonempty value")
	ErrInvalidHeader          = errors.New("-header must use Name=Value")
	ErrInvalidHeaderEnv       = errors.New("-header-env must use Name=ENV")
	ErrInvalidField           = errors.New("-field must use Name=Value with a nonempty name")
	ErrEmptyHeaderEnv         = errors.New("header environment variable is unset or empty")
	ErrEmptyPasswordEnv       = errors.New("password environment variable is unset or empty")
	ErrPasswordFileRead       = errors.New("cannot read password file")
	ErrEmptyPasswordFile      = errors.New("password file is empty")
)
