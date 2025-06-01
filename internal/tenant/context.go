package tenant

import (
	"context"
	"errors"
	"fmt"
)

// Key for tenant ID in context
type contextKey string

const (
	companyIDKey contextKey = "companyID"
	requestIDKey contextKey = "requestID"
)

// ErrNoCompanyInContext is returned when no tenant ID is found in context
var ErrNoCompanyInContext = errors.New("no company ID found in context")

// ErrCompanyIDNotFound is returned when tenant ID is not found in context
var ErrCompanyIDNotFound = errors.New("company ID not found in context")

// WithCompanyID adds a tenant ID to the context
func WithCompanyID(ctx context.Context, companyID string) context.Context {
	return context.WithValue(ctx, companyIDKey, companyID)
}

// FromContext extracts the tenant ID from the context
func FromContext(ctx context.Context) (string, error) {
	companyID, ok := ctx.Value(companyIDKey).(string)
	if !ok || companyID == "" {
		return "", ErrCompanyIDNotFound
	}
	return companyID, nil
}

// MustFromContext extracts the tenant ID from the context or panics
func MustFromContext(ctx context.Context) string {
	companyID, err := FromContext(ctx)
	if err != nil {
		panic(err)
	}
	return companyID
}

// ErrNoRequestIDInContext is returned when no request ID is found in context
var ErrNoRequestIDInContext = errors.New("no request ID found in context")

// WithRequestID adds a request ID to the context
func WithRequestID(ctx context.Context, requestID string) context.Context {
	return context.WithValue(ctx, requestIDKey, requestID)
}

// FromRequestIDContext extracts the request ID from the context
func FromRequestIDContext(ctx context.Context) (string, error) {
	requestID, ok := ctx.Value(requestIDKey).(string)
	if !ok || requestID == "" {
		return "", ErrNoRequestIDInContext
	}
	return requestID, nil
}

// MustFromRequestIDContext extracts the request ID from the context or panics
func MustFromRequestIDContext(ctx context.Context) string {
	requestID, err := FromRequestIDContext(ctx)
	if err != nil {
		panic(err)
	}
	return requestID
}

// validateClusterTenant validates that the cluster field matches the tenant ID from context
func validateClusterTenant(ctx context.Context, cluster string) error {
	if cluster == "" {
		return nil // Skip validation if cluster is not provided
	}

	companyID, err := FromContext(ctx)
	if err != nil {
		return fmt.Errorf("failed to get tenant ID: %w", err)
	}

	if cluster != companyID {
		return fmt.Errorf("cluster (%s) does not match tenant ID (%s)", cluster, companyID)
	}

	return nil
}
