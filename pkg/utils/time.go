package utils

import "time"

// Now returns the current time in UTC timezone
func Now() time.Time {
	return time.Now().UTC()
}

// UnixToTime converts a unix timestamp to a UTC time.Time
func UnixToTime(timestamp int64) time.Time {
	if timestamp <= 0 {
		return time.Time{}
	}
	return time.Unix(timestamp, 0).UTC()
}

// UnixToTimeWithMilliseconds converts a unix timestamp with milliseconds to a UTC time.Time
func UnixToTimeWithMilliseconds(timestamp int64) time.Time {
	if timestamp <= 0 {
		return time.Time{}
	}
	seconds := timestamp / 1000
	nanos := (timestamp % 1000) * 1000000
	return time.Unix(seconds, nanos).UTC()
}

// FormatISO8601 formats a time.Time to ISO8601 format in UTC
func FormatISO8601(t time.Time) string {
	return t.UTC().Format(time.RFC3339)
} 