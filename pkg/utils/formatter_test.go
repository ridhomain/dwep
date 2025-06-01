package utils

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestByteCountSI(t *testing.T) {
	tests := []struct {
		name     string
		bytes    int
		expected string
	}{
		{
			name:     "bytes",
			bytes:    500,
			expected: "500 B",
		},
		{
			name:     "kilobytes",
			bytes:    1500,
			expected: "1.5 kB",
		},
		{
			name:     "exact kilobytes",
			bytes:    1000,
			expected: "1.0 kB",
		},
		{
			name:     "megabytes",
			bytes:    1500000,
			expected: "1.5 MB",
		},
		{
			name:     "exact megabytes",
			bytes:    1000000,
			expected: "1.0 MB",
		},
		{
			name:     "gigabytes",
			bytes:    1500000000,
			expected: "1.5 GB",
		},
		{
			name:     "exact gigabytes",
			bytes:    1000000000,
			expected: "1.0 GB",
		},
		{
			name:     "terabytes",
			bytes:    1500000000000,
			expected: "1.5 TB",
		},
		{
			name:     "zero bytes",
			bytes:    0,
			expected: "0 B",
		},
		{
			name:     "boundary case - 999 bytes",
			bytes:    999,
			expected: "999 B",
		},
		{
			name:     "boundary case - 999.5 KB (rounding)",
			bytes:    999500,
			expected: "999.5 kB",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := ByteCountSI(tc.bytes)
			assert.Equal(t, tc.expected, result)
		})
	}
}
