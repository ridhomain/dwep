package validator

import (
	"fmt"
	"reflect"
	"strings"
	"sync"

	"github.com/go-playground/validator/v10"
)

var (
	validate *validator.Validate
	once     sync.Once
)

// Get returns a singleton validator instance
func Get() *validator.Validate {
	once.Do(func() {
		validate = validator.New()
		
		// Register validation for extracting JSON field names instead of struct field names
		validate.RegisterTagNameFunc(func(fld reflect.StructField) string {
			name := strings.SplitN(fld.Tag.Get("json"), ",", 2)[0]
			if name == "-" {
				return ""
			}
			return name
		})
	})
	return validate
}

// Validate validates a struct and returns formatted errors
func Validate(s interface{}) error {
	err := Get().Struct(s)
	if err == nil {
		return nil
	}

	validationErrors, ok := err.(validator.ValidationErrors)
	if !ok {
		return err
	}

	messages := make([]string, 0, len(validationErrors))
	for _, e := range validationErrors {
		field := e.Field()
		messages = append(messages, fmt.Sprintf("field '%s' failed validation: %s", field, getErrorMessage(e)))
	}

	return fmt.Errorf("validation failed: %s", strings.Join(messages, "; "))
}

// ValidateVar validates a single variable
func ValidateVar(field interface{}, tag string) error {
	return Get().Var(field, tag)
}

// getErrorMessage returns a user-friendly error message for a validation tag
func getErrorMessage(e validator.FieldError) string {
	switch e.Tag() {
	case "required":
		return "is required"
	case "email":
		return "must be a valid email address"
	case "min":
		return fmt.Sprintf("must be at least %s characters long", e.Param())
	case "max":
		return fmt.Sprintf("must not exceed %s characters", e.Param())
	case "gte":
		return fmt.Sprintf("must be greater than or equal to %s", e.Param())
	case "lte":
		return fmt.Sprintf("must be less than or equal to %s", e.Param())
	case "oneof":
		return fmt.Sprintf("must be one of: %s", e.Param())
	default:
		return fmt.Sprintf("validation tag '%s' with value '%s' failed", e.Tag(), e.Value())
	}
} 