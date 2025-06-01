package utils

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

// Define a key type for testing
type testKey int

const (
	testKeyValue testKey = iota
)

func TestGetContextString(t *testing.T) {
	tests := []struct {
		name          string
		setupContext  func() context.Context
		key           interface{}
		expectedValue string
		expectedError error
	}{
		{
			name: "valid string value",
			setupContext: func() context.Context {
				return context.WithValue(context.Background(), testKeyValue, "test-value")
			},
			key:           testKeyValue,
			expectedValue: "test-value",
			expectedError: nil,
		},
		{
			name: "empty string value",
			setupContext: func() context.Context {
				return context.WithValue(context.Background(), testKeyValue, "")
			},
			key:           testKeyValue,
			expectedValue: "",
			expectedError: ErrValueNotFound,
		},
		{
			name: "key not found",
			setupContext: func() context.Context {
				return context.Background()
			},
			key:           testKeyValue,
			expectedValue: "",
			expectedError: ErrValueNotFound,
		},
		{
			name: "non-string value",
			setupContext: func() context.Context {
				return context.WithValue(context.Background(), testKeyValue, 123)
			},
			key:           testKeyValue,
			expectedValue: "",
			expectedError: ErrValueNotFound,
		},
		{
			name: "different key type",
			setupContext: func() context.Context {
				return context.WithValue(context.Background(), "string-key", "test-value")
			},
			key:           testKeyValue,
			expectedValue: "",
			expectedError: ErrValueNotFound,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := tc.setupContext()
			value, err := GetContextString(ctx, tc.key)

			assert.Equal(t, tc.expectedValue, value)
			assert.ErrorIs(t, err, tc.expectedError)
		})
	}
}

func TestGetContextStringOrDefault(t *testing.T) {
	tests := []struct {
		name           string
		setupContext   func() context.Context
		key            interface{}
		defaultValue   string
		expectedResult string
	}{
		{
			name: "valid string value",
			setupContext: func() context.Context {
				return context.WithValue(context.Background(), testKeyValue, "test-value")
			},
			key:            testKeyValue,
			defaultValue:   "default-value",
			expectedResult: "test-value",
		},
		{
			name: "empty string value uses default",
			setupContext: func() context.Context {
				return context.WithValue(context.Background(), testKeyValue, "")
			},
			key:            testKeyValue,
			defaultValue:   "default-value",
			expectedResult: "default-value",
		},
		{
			name: "key not found uses default",
			setupContext: func() context.Context {
				return context.Background()
			},
			key:            testKeyValue,
			defaultValue:   "default-value",
			expectedResult: "default-value",
		},
		{
			name: "non-string value uses default",
			setupContext: func() context.Context {
				return context.WithValue(context.Background(), testKeyValue, 123)
			},
			key:            testKeyValue,
			defaultValue:   "default-value",
			expectedResult: "default-value",
		},
		{
			name: "empty default is returned when key not found",
			setupContext: func() context.Context {
				return context.Background()
			},
			key:            testKeyValue,
			defaultValue:   "",
			expectedResult: "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := tc.setupContext()
			result := GetContextStringOrDefault(ctx, tc.key, tc.defaultValue)

			assert.Equal(t, tc.expectedResult, result)
		})
	}
} 