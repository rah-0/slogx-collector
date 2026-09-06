package main

import (
	"context"
	"errors"
	"log/slog"

	"github.com/rah-0/slogx"
)

func main() {
	slogx.SetDefault(slogx.Options{Format: slogx.JSON, AddSource: true})
	ctx := slogx.WithAttrs(context.Background(), slog.String("request_id", "example-request"))
	if err := handleRequest(ctx); err != nil {
		slogx.ErrorContext(ctx, "request failed", err)
	}
}

func handleRequest(ctx context.Context) error {
	ctx, span := slogx.StartSpan(ctx, "handle request", slog.Group("http", "method", "GET", "route", "/items/{id}"))
	defer span.End()
	span.SetKind(slogx.SpanKindServer)
	slog.InfoContext(ctx, "request started")
	err := slogx.Wrap(loadItem(ctx), "handle request", slog.Group("http", "method", "GET"))
	span.RecordError(err)
	span.SetStatus(slogx.StatusError, "request failed")
	return err
}

func loadItem(ctx context.Context) error {
	ctx, span := slogx.StartSpan(ctx, "load item", slog.Bool("cache_hit", false))
	defer span.End()
	slog.InfoContext(ctx, "load started")
	err := slogx.Wrap(queryItem(ctx), "load item", "cache_hit", false)
	span.RecordError(err)
	span.SetStatus(slogx.StatusError, "load failed")
	return err
}

func queryItem(ctx context.Context) error {
	ctx, span := slogx.StartSpan(ctx, "query item", slog.Int64("item_id", 9007199254740993), slog.Int("attempt", 2))
	defer span.End()
	span.SetKind(slogx.SpanKindClient)
	slog.InfoContext(ctx, "query started")
	err := slogx.Wrap(errors.New("connection refused"), "query item", slog.Group("db", "item_id", 9007199254740993, "attempt", 2))
	span.RecordError(err, slog.Group("retry", "scheduled", false))
	span.SetStatus(slogx.StatusError, "query failed")
	return err
}
