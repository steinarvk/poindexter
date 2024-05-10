package dexapi

import "errors"

type ErrorType struct {
	StatusCode int
	Message    string
}

func (e ErrorType) Error() string {
	return e.Message
}

func Error(code int, message string) error {
	return ErrorType{StatusCode: code, Message: message}
}

func AsError(err error) (*ErrorType, bool) {
	var maybeErr ErrorType
	if errors.As(err, &maybeErr) {
		return &maybeErr, true
	}

	return nil, false
}
