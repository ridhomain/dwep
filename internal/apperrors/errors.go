package apperrors

import (
	"errors"
	"fmt"
)

// RetryableError indicates an error that might be resolved by retrying.
type RetryableError struct {
	Err error
}

// Error implements the error interface.
func (e *RetryableError) Error() string {
	return fmt.Sprintf("retryable: %v", e.Err)
}

// Unwrap returns the wrapped error.
func (e *RetryableError) Unwrap() error {
	return e.Err
}

// NewRetryable wraps the given error as a RetryableError, adding a message.
// It uses fmt.Errorf with %w to maintain the error chain.
func NewRetryable(err error, message string, args ...interface{}) error {
	// Ensure the original error is appended to the format arguments for %w
	format := message + ": %w"
	// Prepend formatted message args, then append the error itself
	allArgs := append(args, err)
	return &RetryableError{Err: fmt.Errorf(format, allArgs...)}
}

// FatalError indicates an error that is unlikely to be resolved by retrying.
type FatalError struct {
	Err error
}

// Error implements the error interface.
func (e *FatalError) Error() string {
	return fmt.Sprintf("fatal: %v", e.Err)
}

// Unwrap returns the wrapped error.
func (e *FatalError) Unwrap() error {
	return e.Err
}

// NewFatal wraps the given error as a FatalError, adding a message.
// It uses fmt.Errorf with %w to maintain the error chain.
func NewFatal(err error, message string, args ...interface{}) error {
	// Ensure the original error is appended to the format arguments for %w
	format := message + ": %w"
	// Prepend formatted message args, then append the error itself
	allArgs := append(args, err)
	return &FatalError{Err: fmt.Errorf(format, allArgs...)}
}

// --- Standard Error Definitions ---

// These sentinel errors define common application-level error conditions.
// They can be checked using errors.Is and potentially wrapped by RetryableError or FatalError
// depending on the context where they are handled.
var (
	// ErrNotFound indicates a requested resource was not found.
	ErrNotFound = errors.New("resource not found")
	// ErrValidation indicates failure during data validation.
	ErrValidation = errors.New("validation failed")
	// ErrDatabase indicates a general database interaction error.
	ErrDatabase = errors.New("database error")
	// ErrNATS indicates a general NATS communication error.
	ErrNATS = errors.New("nats communication error")
	// ErrUnauthorized indicates an authorization failure.
	ErrUnauthorized = errors.New("unauthorized access")
	// ErrDuplicate indicates a conflict due to duplicate data (e.g., unique constraint).
	ErrDuplicate = errors.New("duplicate resource")
	// ErrConflict indicates a general conflict state (e.g., optimistic locking failure).
	ErrConflict = errors.New("resource conflict")
	// ErrBadRequest indicates a malformed or invalid request from the client/caller.
	ErrBadRequest = errors.New("bad request")
	// ErrTimeout indicates an operation timed out.
	ErrTimeout = errors.New("operation timeout")
	// ErrRateLimited indicates an operation was rate limited.
	ErrRateLimited = errors.New("rate limited")
)

// --- Helper functions for checking ---

// IsRetryable checks if the error is a RetryableError or wraps one.
func IsRetryable(err error) bool {
	var target *RetryableError
	return errors.As(err, &target)
}

// IsFatal checks if the error is a FatalError or wraps one.
func IsFatal(err error) bool {
	var target *FatalError
	return errors.As(err, &target)
}

// --- Specific Standard Error Checkers ---

// IsNotFoundError checks if the error is or wraps ErrNotFound.
func IsNotFoundError(err error) bool {
	return errors.Is(err, ErrNotFound)
}

// IsValidationError checks if the error is or wraps ErrValidation.
func IsValidationError(err error) bool {
	return errors.Is(err, ErrValidation)
}

// IsDatabaseError checks if the error is or wraps ErrDatabase.
func IsDatabaseError(err error) bool {
	return errors.Is(err, ErrDatabase)
}

// IsNATSError checks if the error is or wraps ErrNATS.
func IsNATSError(err error) bool {
	return errors.Is(err, ErrNATS)
}

// IsUnauthorizedError checks if the error is or wraps ErrUnauthorized.
func IsUnauthorizedError(err error) bool {
	return errors.Is(err, ErrUnauthorized)
}

// IsDuplicateError checks if the error is or wraps ErrDuplicate.
func IsDuplicateError(err error) bool {
	return errors.Is(err, ErrDuplicate)
}

// IsConflictError checks if the error is or wraps ErrConflict.
func IsConflictError(err error) bool {
	return errors.Is(err, ErrConflict)
}

// IsBadRequestError checks if the error is or wraps ErrBadRequest.
func IsBadRequestError(err error) bool {
	return errors.Is(err, ErrBadRequest)
}

// IsTimeoutError checks if the error is or wraps ErrTimeout.
func IsTimeoutError(err error) bool {
	return errors.Is(err, ErrTimeout)
}

// IsRateLimitedError checks if the error is or wraps ErrRateLimited.
func IsRateLimitedError(err error) bool {
	return errors.Is(err, ErrRateLimited)
}
