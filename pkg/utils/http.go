package utils

import (
	"encoding/json"
	"net/http"
)

// WriteJSONResponse writes a JSON response with the given status code
// Sets Content-Type header and handles JSON encoding
func WriteJSONResponse(w http.ResponseWriter, statusCode int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(data)
} 