package dexerror

import (
	"errors"
	"fmt"
	"net/http"

	"go.uber.org/zap"
)

type ErrorDetail struct {
	Message string                 `json:"message"`
	Data    map[string]interface{} `json:"data"`
}

type InternalErrorDetail struct {
	ErrorID string `json:"error_id"`
	ErrorDetail
}

type PublicErrorDetail struct {
	ErrorDetail
}

type PoindexterError interface {
	Error() string
	PublicMessage() string
	HTTPStatusCode() int
	PublicErrorDetail() PublicErrorDetail
	InternalErrorDetail() InternalErrorDetail
	InternalErrorMessage() string
	InternalZapFields() []zap.Field
	IsUnremarkable() bool
}

type errorOptions struct {
	httpCode     int
	public       PublicErrorDetail
	internal     InternalErrorDetail
	unremarkable bool
}

func (e *errorOptions) IsUnremarkable() bool {
	return e.unremarkable
}

func (e *errorOptions) PublicErrorDetail() PublicErrorDetail {
	return e.public
}

func (e *errorOptions) PublicMessage() string {
	if e.public.Message != "" {
		return e.public.Message
	}
	return "internal error"
}

func (e *errorOptions) InternalErrorDetail() InternalErrorDetail {
	return e.internal
}

func (e *errorOptions) HTTPStatusCode() int {
	if e.httpCode == 0 {
		return http.StatusInternalServerError
	}
	return e.httpCode
}

func (e *errorOptions) InternalZapFields() []zap.Field {
	fields := map[string]interface{}{}

	for k, v := range e.public.Data {
		fields[k] = v
	}

	for k, v := range e.internal.Data {
		fields[k] = v
	}

	var rv []zap.Field
	for k, v := range fields {
		rv = append(rv, zap.Any(k, v))
	}

	return rv
}

func (e *errorOptions) InternalErrorMessage() string {
	if e.public.Message != "" && e.internal.Message != "" && e.public.Message != e.internal.Message {
		return fmt.Sprintf("%s (%s)", e.public.Message, e.internal.Message)
	}
	if e.public.Message != "" {
		return e.public.Message
	}
	if e.internal.Message != "" {
		return e.internal.Message
	}
	return "unknown error"
}

func (e *errorOptions) Error() string {
	return e.public.Message
}

type ErrorOption func(*errorOptions)

func WithUnremarkable() ErrorOption {
	return func(opts *errorOptions) {
		opts.unremarkable = true
	}
}

func WithHTTPCode(code int) ErrorOption {
	return func(opts *errorOptions) {
		opts.httpCode = code
	}
}

func WithErrorID(errorID string) ErrorOption {
	return func(opts *errorOptions) {
		opts.internal.ErrorID = errorID
	}
}

func WithPublicMessage(message string) ErrorOption {
	return func(opts *errorOptions) {
		opts.public.Message = message
	}
}

func WithInternalMessage(message string) ErrorOption {
	return func(opts *errorOptions) {
		opts.internal.Message = message
	}
}

func WithPublicData(key string, value interface{}) ErrorOption {
	return func(opts *errorOptions) {
		if opts.public.Data == nil {
			opts.public.Data = make(map[string]interface{})
		}
		opts.public.Data[key] = value
	}
}

func WithInternalData(key string, value interface{}) ErrorOption {
	return func(opts *errorOptions) {
		if opts.internal.Data == nil {
			opts.internal.Data = make(map[string]interface{})
		}
		opts.internal.Data[key] = value
	}
}

func New(options ...ErrorOption) PoindexterError {
	opts := errorOptions{}
	for _, option := range options {
		option(&opts)
	}

	if opts.httpCode == 0 {
		opts.httpCode = http.StatusInternalServerError
	}

	if opts.public.Message == "" {
		opts.public.Message = "Internal server error"
	}

	if opts.internal.ErrorID == "" {
		opts.internal.ErrorID = "unknown-error"
	}

	return &opts
}

func asError(err error) (PoindexterError, bool) {
	var maybeErr PoindexterError
	if errors.As(err, &maybeErr) {
		return maybeErr, true
	}

	return nil, false
}

func AsPoindexterError(err error) PoindexterError {
	pde, ok := asError(err)
	if ok {
		return pde
	}

	return New(
		WithErrorID("unknown_error"),
		WithHTTPCode(http.StatusInternalServerError),
		WithPublicMessage("Internal server error"),
		WithInternalMessage("non-API error: "+err.Error()),
	)
}
