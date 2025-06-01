package utils

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNow(t *testing.T) {
	// Get current time using Now() and standard time.Now().UTC()
	utilsTime := Now()
	standardTime := time.Now().UTC()

	// The times should be very close - within a small delta
	assert.WithinDuration(t, standardTime, utilsTime, 10*time.Millisecond)
	
	// Ensure the timezone is UTC
	assert.Equal(t, time.UTC, utilsTime.Location())
}

func TestUnixToTime(t *testing.T) {
	tests := []struct {
		name      string
		timestamp int64
		expected  time.Time
	}{
		{
			name:      "valid timestamp",
			timestamp: 1609459200, // 2021-01-01 00:00:00 UTC
			expected:  time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			name:      "zero timestamp",
			timestamp: 0,
			expected:  time.Time{},
		},
		{
			name:      "negative timestamp",
			timestamp: -1,
			expected:  time.Time{},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := UnixToTime(tc.timestamp)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestUnixToTimeWithMilliseconds(t *testing.T) {
	tests := []struct {
		name      string
		timestamp int64
		expected  time.Time
	}{
		{
			name:      "valid timestamp",
			timestamp: 1609459200000, // 2021-01-01 00:00:00.000 UTC
			expected:  time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			name:      "timestamp with milliseconds",
			timestamp: 1609459200123, // 2021-01-01 00:00:00.123 UTC
			expected:  time.Date(2021, 1, 1, 0, 0, 0, 123000000, time.UTC),
		},
		{
			name:      "zero timestamp",
			timestamp: 0,
			expected:  time.Time{},
		},
		{
			name:      "negative timestamp",
			timestamp: -1,
			expected:  time.Time{},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := UnixToTimeWithMilliseconds(tc.timestamp)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestFormatISO8601(t *testing.T) {
	tests := []struct {
		name     string
		input    time.Time
		expected string
	}{
		{
			name:     "UTC time",
			input:    time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC),
			expected: "2021-01-01T00:00:00Z",
		},
		{
			name:     "non-UTC time is converted to UTC",
			input:    time.Date(2021, 1, 1, 0, 0, 0, 0, time.FixedZone("EST", -5*60*60)),
			expected: "2021-01-01T05:00:00Z", // 00:00 EST is 05:00 UTC
		},
		{
			name:     "with milliseconds",
			input:    time.Date(2021, 1, 1, 0, 0, 0, 123000000, time.UTC),
			expected: "2021-01-01T00:00:00Z", // RFC3339 format truncates to seconds
		},
		{
			name:     "zero time",
			input:    time.Time{},
			expected: "0001-01-01T00:00:00Z",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := FormatISO8601(tc.input)
			assert.Equal(t, tc.expected, result)
		})
	}
} 