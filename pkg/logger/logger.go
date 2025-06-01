package logger

import (
	"context"
	"time"

	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/tenant"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// Log is the global logger
var Log *zap.Logger

// Initialize sets up the global logger with the specified log level
func Initialize(level string) error {
	// Parse log level
	var zapLevel zapcore.Level
	err := zapLevel.UnmarshalText([]byte(level))
	if err != nil {
		zapLevel = zap.InfoLevel
	}

	// Create custom time encoder for UTC timestamps
	customTimeEncoder := func(t time.Time, enc zapcore.PrimitiveArrayEncoder) {
		enc.AppendString(t.UTC().Format(time.RFC3339))
	}

	// Create logger config
	config := zap.Config{
		Level:             zap.NewAtomicLevelAt(zapLevel),
		Development:       false,
		DisableCaller:     false,
		DisableStacktrace: false,
		Sampling:          nil,
		Encoding:          "json",
		EncoderConfig: zapcore.EncoderConfig{
			TimeKey:        "ts",
			LevelKey:       "level",
			NameKey:        "logger",
			CallerKey:      "caller",
			FunctionKey:    zapcore.OmitKey,
			MessageKey:     "msg",
			StacktraceKey:  "stacktrace",
			LineEnding:     zapcore.DefaultLineEnding,
			EncodeLevel:    zapcore.LowercaseLevelEncoder,
			EncodeTime:     customTimeEncoder,
			EncodeDuration: zapcore.SecondsDurationEncoder,
			EncodeCaller:   zapcore.ShortCallerEncoder,
		},
		OutputPaths:      []string{"stdout"},
		ErrorOutputPaths: []string{"stderr"},
	}

	// Build the logger
	logger, err := config.Build(
		zap.AddCaller(),
		zap.AddCallerSkip(1),
	)
	if err != nil {
		return err
	}

	// Set global logger
	Log = logger

	return nil
}

// WithLogger attaches a scoped logger to the context
func WithLogger(ctx context.Context, logger *zap.Logger) context.Context {
	return context.WithValue(ctx, loggerKey, logger)
}

// FromContext extracts a logger from the context
func FromContext(ctx context.Context) *zap.Logger {
	if ctx == nil {
		return Log // Return global logger if context is nil
	}

	baseLogger := Log // Start with the global logger
	if logger, ok := ctx.Value(loggerKey).(*zap.Logger); ok {
		baseLogger = logger // Use logger from context if available
	}

	// Check for requestID in context and add it as a field
	if requestID, err := tenant.FromRequestIDContext(ctx); err == nil && requestID != "" {
		return baseLogger.With(zap.String("request_id", requestID))
	}

	return baseLogger
}

// Sync flushes any buffered log entries
func Sync() {
	if Log != nil {
		_ = Log.Sync()
	}
}

// FromContextOr returns the logger from the context or the default logger if not found.
func FromContextOr(ctx context.Context, defaultLogger *zap.Logger) *zap.Logger {
	if l, ok := ctx.Value(loggerKey).(*zap.Logger); ok {
		return l
	}
	if defaultLogger != nil {
		return defaultLogger
	}
	// Fallback to the global logger if default is also nil
	return Log
}

type contextKey int

const (
	loggerKey contextKey = iota
)
