package parser

import (
	reflect "reflect"
	"time"
)

// IsTimeZero checks if a time.Time value is zero (i.e., equal to the zero time).
func IsTimeZero(t time.Time) bool {
	return t.IsZero()
}

// IsTimeNotZero checks if a time.Time value is not zero (i.e., not equal to the zero time).
func IsTimeNotZero(t time.Time) bool {
	return !t.IsZero()
}

func JSONOmitEmpty(input interface{}) interface{} {
	// Check if the input is nil
	if input == nil {
		return `"__null__"`
	}

	// Use reflection to check if the input is a zero value
	v := reflect.ValueOf(input)
	if v.Kind() == reflect.Ptr && v.IsNil() {
		return `"__null__"`
	}
	if v.IsValid() && v.IsZero() {
		return `"__null__"`
	}

	// Return the input as is
	return input
}
