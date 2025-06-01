# Utils Package

This package contains shared utility functions used throughout the daisi-wa-events-processor project.

## Contents

### Formatter Utilities (`formatter.go`)
- `ByteCountSI`: Formats byte counts in SI units (kB, MB, GB, etc.)

### HTTP Utilities (`http.go`)
- `WriteJSONResponse`: Helper for writing JSON responses with proper headers

### Comparison Utilities (`compare.go`)
- `StreamConfigEqual`: Compares NATS stream configurations
- `ConsumerConfigEqual`: Compares NATS consumer configurations

### Context Utilities (`context.go`)
- `GetContextString`: Extract string values from context
- `GetContextStringOrDefault`: Extract string values with default fallback

## Usage

Import the package:

```go
import "gitlab.com/timkado/api/daisi-wa-events-processor/pkg/utils"
```

Examples:

```go
// Format bytes
fmt.Println(utils.ByteCountSI(1500))  // "1.5 kB"

// Write JSON response
utils.WriteJSONResponse(w, http.StatusOK, data)

// Compare NATS configurations
equal := utils.StreamConfigEqual(streamA, streamB)

// Get value from context
value, err := utils.GetContextString(ctx, myKey)
``` 