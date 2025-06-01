package utils

import (
	"encoding/json"
)

// MustMarshalJSON marshals v into a json byte array
// It panics if marshaling fails
func MustMarshalJSON(v interface{}) []byte {
	data, err := json.Marshal(v)
	if err != nil {
		panic("failed to marshal JSON: " + err.Error())
	}
	return data
}

// UnmarshalJSON unmarshals json data into v
// Returns error if unmarshaling fails
func UnmarshalJSON(data []byte, v interface{}) error {
	return json.Unmarshal(data, v)
} 