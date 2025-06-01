package utils

import (
	"context"
	"fmt"
	"os"
	"runtime/debug"
	"time"

	"gitlab.com/timkado/api/daisi-wa-events-processor/pkg/logger"
	"go.uber.org/zap"
)

// RecoverFn is a function that handles a recovered panic
type RecoverFn func(r interface{}, stack []byte)

// SafeGo executes the given function in a goroutine with panic recovery
func SafeGo(fn func(), onPanic RecoverFn) {
	go func() {
		defer func() {
			if r := recover(); r != nil {
				stack := debug.Stack()
				if onPanic != nil {
					onPanic(r, stack)
				} else {
					// Default handler logs the panic
					if logger.Log != nil {
						logger.Log.Error("[panic] Recovered from panic in goroutine",
							zap.Any("panic", r),
							zap.ByteString("stack", stack),
						)
					} else {
						// Fallback to printing to stderr if logger isn't available
						fmt.Fprintf(os.Stderr, "[PANIC] Recovered from panic in goroutine: %v\n%s\n", r, stack)
					}
				}
			}
		}()
		fn()
	}()
}

// RecoverWithLog provides a standard way to recover from panics with logging
func RecoverWithLog(ctx context.Context, operation string) {
	if r := recover(); r != nil {
		stack := debug.Stack()

		// Try to get logger from context
		log := logger.FromContext(ctx)
		if log != nil {
			log.Error(fmt.Sprintf("[panic] Recovered from panic during %s", operation),
				zap.Any("panic", r),
				zap.ByteString("stack", stack),
				zap.Time("recovery_time", time.Now()),
			)
		} else if logger.Log != nil {
			// Fall back to global logger
			logger.Log.Error(fmt.Sprintf("[panic] Recovered from panic during %s", operation),
				zap.Any("panic", r),
				zap.ByteString("stack", stack),
				zap.Time("recovery_time", time.Now()),
			)
		} else {
			// Last resort: print to stderr
			fmt.Fprintf(os.Stderr, "[PANIC] Recovered from panic during %s: %v\n%s\n",
				operation, r, stack)
		}
	}
}

// WrapWithRecovery wraps a function with panic recovery
func WrapWithRecovery(fn func() error) func() (err error) {
	return func() (err error) {
		defer func() {
			if r := recover(); r != nil {
				stack := debug.Stack()

				// Check if logger is initialized to avoid nil pointer dereference
				if logger.Log != nil {
					logger.Log.Error("[panic] Recovered from panic",
						zap.Any("panic", r),
						zap.ByteString("stack", stack),
					)
				} else {
					// Fallback to printing to stderr if logger isn't available
					fmt.Fprintf(os.Stderr, "[PANIC] Recovered from panic: %v\n%s\n", r, stack)
				}

				err = fmt.Errorf("panic recovered: %v", r)
			}
		}()
		return fn()
	}
}

// WrapWithContextRecovery wraps a function that takes a context with panic recovery
func WrapWithContextRecovery(fn func(ctx context.Context) error) func(ctx context.Context) (err error) {
	return func(ctx context.Context) (err error) {
		defer func() {
			if r := recover(); r != nil {
				stack := debug.Stack()

				// Get logger from context or use fallback
				log := logger.FromContext(ctx)
				if log != nil {
					log.Error("[panic] Recovered from panic",
						zap.Any("panic", r),
						zap.ByteString("stack", stack),
					)
				} else if logger.Log != nil {
					// Fallback to global logger
					logger.Log.Error("[panic] Recovered from panic",
						zap.Any("panic", r),
						zap.ByteString("stack", stack),
					)
				} else {
					// Last resort: print to stderr
					fmt.Fprintf(os.Stderr, "[PANIC] Recovered from panic in context: %v\n%s\n", r, stack)
				}

				err = fmt.Errorf("panic recovered: %v", r)
			}
		}()
		return fn(ctx)
	}
}
