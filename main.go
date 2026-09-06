// Command slogx-collector journals newline-delimited JSON from stdin and forwards
// batches to a configured destination.
package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/rah-0/slogx-collector/internal/cli"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	code := cli.Run(ctx, os.Args[1:], os.Stdin, os.Stdout, os.Stderr)
	stop()
	os.Exit(code)
}
