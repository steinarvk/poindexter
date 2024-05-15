package logging

import (
	"context"

	"go.uber.org/zap"
)

type contextKey string

const (
	contextKeyLogger contextKey = "logger"
)

type ContextData struct {
	Logger *zap.Logger
	Debug  bool
}

func NewContextWithLogger(ctx context.Context, logger *zap.Logger, debug bool) context.Context {
	return context.WithValue(ctx, contextKeyLogger, ContextData{Logger: logger, Debug: debug})
}

func FromContext(ctx context.Context) *zap.Logger {
	cdata, ok := ctx.Value(contextKeyLogger).(ContextData)
	if !ok {
		return zap.L()
	}
	return cdata.Logger
}

func DataFromContext(ctx context.Context) ContextData {
	cdata, ok := ctx.Value(contextKeyLogger).(ContextData)
	if !ok {
		return ContextData{
			Logger: zap.L(),
		}
	}
	return cdata
}
