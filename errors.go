package breaql

import (
	"fmt"
)

type ParseError struct {
	Message string // simple and human-readable error message

	funcName string
	original error
}

func (e *ParseError) Error() string {
	return fmt.Sprintf("error %s: %s", e.funcName, e.Message)
}

func (e *ParseError) Unwrap() error {
	return e.original
}
