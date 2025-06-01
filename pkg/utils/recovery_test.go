package utils

import (
	"context"
	"errors"
	"sync"
	"testing"

	"gitlab.com/timkado/api/daisi-wa-events-processor/pkg/logger"
	"go.uber.org/zap/zaptest"
)

// setupTestLogger sets up a test logger and returns a function to restore the original logger
func setupTestLogger(t *testing.T) func() {
	testLogger := zaptest.NewLogger(t)
	originalLogger := logger.Log
	logger.Log = testLogger
	return func() {
		logger.Log = originalLogger
	}
}

// setupContextWithLogger creates a context with a test logger
func setupContextWithLogger(t *testing.T) context.Context {
	testLogger := zaptest.NewLogger(t)
	return logger.WithLogger(context.Background(), testLogger)
}

func TestSafeGo(t *testing.T) {
	// Initialize test logger
	cleanup := setupTestLogger(t)
	defer cleanup()

	// Test case 1: Function runs without panic
	successChan := make(chan bool, 1)
	SafeGo(func() {
		successChan <- true
	}, nil)

	select {
	case success := <-successChan:
		if !success {
			t.Error("Expected function to execute successfully")
		}
	case <-make(chan bool):
		t.Error("Function did not execute in time")
	}

	// Test case 2: Function panics and is recovered
	var wg sync.WaitGroup
	wg.Add(1)
	var recoveredPanic interface{}

	SafeGo(func() {
		defer wg.Done()
		panic("test panic")
	}, func(r interface{}, stack []byte) {
		recoveredPanic = r
	})

	wg.Wait()
	if recoveredPanic != "test panic" {
		t.Errorf("Expected panic to be recovered with 'test panic', got %v", recoveredPanic)
	}
}

func TestWrapWithRecovery(t *testing.T) {
	// Initialize test logger
	cleanup := setupTestLogger(t)
	defer cleanup()

	// Test case 1: Function executes without panic
	normalFn := func() error {
		return nil
	}

	wrappedNormal := WrapWithRecovery(normalFn)
	if err := wrappedNormal(); err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	// Test case 2: Function returns error without panic
	errFn := func() error {
		return errors.New("test error")
	}

	wrappedErr := WrapWithRecovery(errFn)
	if err := wrappedErr(); err == nil || err.Error() != "test error" {
		t.Errorf("Expected 'test error', got %v", err)
	}

	// Test case 3: Function panics
	panicFn := func() error {
		panic("test panic")
		return nil
	}

	wrappedPanic := WrapWithRecovery(panicFn)
	if err := wrappedPanic(); err == nil || err.Error() != "panic recovered: test panic" {
		t.Errorf("Expected 'panic recovered: test panic', got %v", err)
	}
}

func TestWrapWithContextRecovery(t *testing.T) {
	// Initialize test logger
	cleanup := setupTestLogger(t)
	defer cleanup()

	// Initialize test context with logger
	ctx := setupContextWithLogger(t)

	// Test case 1: Function executes without panic
	normalFn := func(ctx context.Context) error {
		return nil
	}

	wrappedNormal := WrapWithContextRecovery(normalFn)
	if err := wrappedNormal(ctx); err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	// Test case 2: Function returns error without panic
	errFn := func(ctx context.Context) error {
		return errors.New("test error with context")
	}

	wrappedErr := WrapWithContextRecovery(errFn)
	if err := wrappedErr(ctx); err == nil || err.Error() != "test error with context" {
		t.Errorf("Expected 'test error with context', got %v", err)
	}

	// Test case 3: Function panics
	panicFn := func(ctx context.Context) error {
		panic("test panic with context")
		return nil
	}

	wrappedPanic := WrapWithContextRecovery(panicFn)
	if err := wrappedPanic(ctx); err == nil || err.Error() != "panic recovered: test panic with context" {
		t.Errorf("Expected 'panic recovered: test panic with context', got %v", err)
	}
}
