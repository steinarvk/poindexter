package dexapi

import "errors"

type PublicErrorDetail struct {
	PublicMessage string `json:"message"`
}

type ErrorType struct {
	StatusCode int
	Message    string
}

type errorOptions struct {
	publicMessage   string
	publicFields    map[string]interface{}
	internalMessage string
	internalFields  map[string]interface{}
}

type ErrorOption func(*errorOptions)

func WithPublicMessage(message string) ErrorOption {
	return func(opts *errorOptions) {
		opts.publicMessage = message
	}
}

func WithInternalMessage(message string) ErrorOption {
	return func(opts *errorOptions) {
		opts.internalMessage = message
	}
}

func WithPublicValue(key string, value interface{}) ErrorOption {
	return func(opts *errorOptions) {
		if opts.publicFields == nil {
			opts.publicFields = make(map[string]interface{})
		}
		opts.publicFields[key] = value
	}
}

func WithInternalValue(key string, value interface{}) ErrorOption {
	return func(opts *errorOptions) {
		if opts.internalFields == nil {
			opts.internalFields = make(map[string]interface{})
		}
		opts.internalFields[key] = value
	}
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

func (e *ErrorType) PublicErrorDetail() PublicErrorDetail {
	return PublicErrorDetail{PublicMessage: e.Message}
}
