package utils

import (
	"fmt"
)

// ByteCountSI formats a byte count in SI units (kB, MB, GB, etc.)
// For example: 1024 -> "1.0 kB"
func ByteCountSI(b int) string {
	const unit = 1000
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(b)/float64(div), "kMGTPE"[exp])
} 