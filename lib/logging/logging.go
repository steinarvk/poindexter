package logging

import (
	"context"

	"go.uber.org/zap"
)

type contextKey string

const (
	contextKeyLogger contextKey = "logger"
)

func NewContextWithLogger(ctx context.Context, logger *zap.Logger) context.Context {
	return context.WithValue(ctx, contextKeyLogger, logger)
}

func FromContext(ctx context.Context) *zap.Logger {
	logger, ok := ctx.Value(contextKeyLogger).(*zap.Logger)
	if !ok {
		return zap.L()
	}
	return logger
}
