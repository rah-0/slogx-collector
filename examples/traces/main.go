package main

import (
	"context"
	"io"
	"log/slog"
	"os"

	"github.com/rah-0/slogx"
)

func main() {
	run(os.Stdout)
}

func run(output io.Writer) {
	slogx.SetDefault(slogx.Options{
		Format: slogx.JSON, AddSource: true, Writer: output,
	})
	handleRequest(context.Background(), 42)
}

func handleRequest(ctx context.Context, id int) {
	ctx, span := slogx.StartSpan(ctx, "handle request",
		slog.Group("http", "method", "GET", "route", "/items/{id}"),
	)
	span.SetKind(slogx.SpanKindServer)
	defer span.End()
	slog.InfoContext(ctx, "request started")

	if err := loadItem(ctx, id); err != nil {
		err = slogx.Wrap(err, "handle request", "method", "GET")
		span.RecordError(err)
		span.SetStatus(slogx.StatusError, "request failed")
		slogx.ErrorContext(ctx, "request failed", err)
	}
}

func loadItem(ctx context.Context, id int) error {
	ctx, span := slogx.StartSpan(ctx, "load item", slog.Bool("cache_hit", false))
	defer span.End()
	slog.InfoContext(ctx, "cache miss")

	if err := queryItem(ctx, id); err != nil {
		err = slogx.Wrap(err, "load item", "cache_hit", false)
		span.RecordError(err)
		span.SetStatus(slogx.StatusError, "load failed")
		return err
	}
	return nil
}

func queryItem(ctx context.Context, id int) error {
	ctx, span := slogx.StartSpan(ctx, "query item",
		slog.Int("item_id", id), slog.Int("attempt", 2),
	)
	span.SetKind(slogx.SpanKindClient)
	defer span.End()
	slog.InfoContext(ctx, "query started")

	// Simulate a database failure; no external database is needed.
	err := slogx.Wrap(ErrConnectionRefused, "query item", "item_id", id, "attempt", 2)
	span.RecordError(err)
	span.SetStatus(slogx.StatusError, "query failed")
	return err
}
