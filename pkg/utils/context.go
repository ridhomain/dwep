package utils

import (
	"context"
	"errors"
)

// ContextKey is a type for context keys with string values
type ContextKey int

// Generic context key extraction with error handling
var (
	ErrValueNotFound = errors.New("value not found in context")
)

// GetContextString extracts a string value from the context by its key
func GetContextString(ctx context.Context, key interface{}) (string, error) {
	value, ok := ctx.Value(key).(string)
	if !ok || value == "" {
		return "", ErrValueNotFound
	}
	return value, nil
}

// GetContextStringOrDefault extracts a string value from the context or returns a default
func GetContextStringOrDefault(ctx context.Context, key interface{}, defaultValue string) string {
	value, err := GetContextString(ctx, key)
	if err != nil {
		return defaultValue
	}
	return value
} 