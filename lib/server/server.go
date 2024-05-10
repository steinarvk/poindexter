package server

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"runtime/debug"
	"time"

	"github.com/XSAM/otelsql"
	"github.com/steinarvk/poindexter/lib/config"
	"github.com/steinarvk/poindexter/lib/dexapi"
	"github.com/steinarvk/poindexter/lib/dexerror"
	"github.com/steinarvk/poindexter/lib/poindexterdb"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.7.0"
)

// Option is the type for functional options that can return an error
type Option func(*Server) error

// Server holds the HTTP server configuration
type Server struct {
	host          string
	port          int
	config        config.Config
	db            *poindexterdb.DB
	httpServer    *http.Server
	postgresCreds poindexterdb.PostgresConfig
}

// NewServer creates a new server with default values and applies given options
func New(options ...Option) (*Server, error) {
	s := &Server{
		host: "127.0.0.1",
		port: 5244,
	}
	for _, option := range options {
		err := option(s)
		if err != nil {
			return nil, err
		}
	}

	address := fmt.Sprintf("%s:%d", s.host, s.port)
	s.httpServer = &http.Server{
		Addr: address,
	}

	driverName, err := otelsql.Register("postgres")
	if err != nil {
		return nil, err
	}

	params := poindexterdb.Params{
		Postgres:      s.postgresCreds,
		SQLDriverName: driverName,
		Verbosity:     0,
	}

	ctx := context.Background()

	db, err := poindexterdb.Open(ctx, params, s.config)
	if err != nil {
		return nil, err
	}

	s.db = db

	return s, nil
}

func (s *Server) Close() error {
	return s.db.Close()
}

func WithPostgresCreds(creds poindexterdb.PostgresConfig) Option {
	return func(s *Server) error {
		s.postgresCreds = creds
		return nil
	}
}

func WithConfig(cfg config.Config) Option {
	return func(s *Server) error {
		if err := cfg.Validate(); err != nil {
			return err
		}

		s.config = cfg
		return nil
	}
}

// WithHost is a functional option to set the server's host
func WithHost(host string) Option {
	return func(s *Server) error {
		if host == "" {
			return fmt.Errorf("host cannot be empty")
		}
		s.host = host
		return nil
	}
}

// WithPort is a functional option to set the server's port
func WithPort(port int) Option {
	return func(s *Server) error {
		if port <= 0 {
			return fmt.Errorf("port must be positive")
		}
		s.port = port
		return nil
	}
}

// apiHandlerFunc is a custom type for our API handlers
type apiHandlerFunc func(namespace string, w http.ResponseWriter, r *http.Request) error

// Run starts the server
func (s *Server) Run() error {
	mux := http.NewServeMux()
	mux.Handle("/api/read/records/", s.middleware(readApiHandler{s.readQueryRecordsHandler}))
	mux.Handle("/api/read/fields/", s.middleware(readApiHandler{s.readQueryFieldsHandler}))

	mux.Handle("/api/write/record/", s.middleware(writeApiHandler{s.writeSingleRecordHandler}))
	mux.Handle("/api/write/jsonl/", s.middleware(writeApiHandler{s.writeJSONLHandler}))

	var wrappedHandler http.Handler = mux
	wrappedHandler = otelhttp.NewHandler(wrappedHandler, "poindexter-server")

	s.httpServer.Handler = wrappedHandler

	zap.L().Sugar().Infof("Server is running on %s", s.httpServer.Addr)

	return s.httpServer.ListenAndServe()
}

func (s *Server) HTTPServer() *http.Server {
	return s.httpServer
}

type VerifyingApiHandler interface {
	CheckAndServeHTTP(access config.AccessLevel, namespace string, w http.ResponseWriter, r *http.Request) error
}

type readApiHandler struct {
	handler apiHandlerFunc
}

func (v readApiHandler) CheckAndServeHTTP(access config.AccessLevel, namespace string, w http.ResponseWriter, r *http.Request) error {
	if !access.ReadAccess {
		return errUnauthorized
	}

	return v.handler(namespace, w, r)
}

var (
	errUnauthorized = errors.New("unauthorized")
)

type writeApiHandler struct {
	handler apiHandlerFunc
}

func (v writeApiHandler) CheckAndServeHTTP(access config.AccessLevel, namespace string, w http.ResponseWriter, r *http.Request) error {
	if !access.WriteAccess {
		return errUnauthorized
	}

	return v.handler(namespace, w, r)
}

// middleware enforces basic auth and checks for a custom header, passing the validated username and namespace forward
func (s *Server) middleware(next VerifyingApiHandler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var authClient *config.Client

		t0 := time.Now()
		username, password, ok := r.BasicAuth()
		if ok {
			client, ok := s.config.Clients[username]
			if ok && client.SharedSecret == password {
				authClient = &client
			}
		}

		namespace := r.Header.Get("X-Namespace")

		serveUnauthorized := func() {
			zap.L().Sugar().Infof("rejecting access to user %q to namespace %q", username, namespace)

			leftUntilSecond := time.Until(t0.Add(time.Second))
			time.Sleep(leftUntilSecond)
			writeJSONError(w, "Unauthorized", http.StatusUnauthorized)
		}

		if authClient == nil {
			serveUnauthorized()
			return
		}

		if namespace == "" {
			serveUnauthorized()
			return
		}

		accessLevel, ok := authClient.Access[namespace]
		if !ok {
			serveUnauthorized()
			return
		}

		zap.L().Sugar().Infof("checked access for user %q to namespace %q: %+v", username, namespace, accessLevel)

		if err := next.CheckAndServeHTTP(accessLevel, namespace, w, r); err != nil {
			if err == errUnauthorized {
				serveUnauthorized()
				return
			}

			apiErr := dexerror.AsPoindexterError(err)

			zap.L().Error(
				apiErr.InternalErrorMessage(),
				apiErr.InternalZapFields()...,
			)
			response := dexapi.ErrorResponse{
				Error: apiErr.PublicErrorDetail(),
			}

			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(apiErr.HTTPStatusCode())

			marshalled, oopsErr := json.MarshalIndent(response, "", "  ")
			if oopsErr != nil {
				zap.L().Sugar().Error("error marshalling error", zap.Error(oopsErr))
				w.Write([]byte(`{"error": {"message": "internal error"}}`))
			} else {
				w.Write(marshalled)
			}
		}
	})
}

func (s *Server) readQueryFieldsHandler(namespace string, w http.ResponseWriter, r *http.Request) error {
	var req interface{}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		return err
	}

	defer r.Body.Close()

	response := map[string]interface{}{
		"message":        "foo called",
		"namespace":      namespace,
		"request":        req,
		"implementation": "TODO",
	}
	w.Header().Set("Content-Type", "application/json")
	return json.NewEncoder(w).Encode(response)
}

type simpleReq struct {
	Namespace             string                   `json:"namespace"`
	Limit                 int                      `json:"limit"`
	LowLevelFieldsPresent []string                 `json:"low_level_fields_present"`
	LowLevelFieldHasValue map[string][]interface{} `json:"low_level_field_has_value"`
}

func (s *Server) readQueryRecordsHandler(namespace string, w http.ResponseWriter, r *http.Request) error {
	ctx := r.Context()

	var queryReq dexapi.Query
	if err := json.NewDecoder(r.Body).Decode(&queryReq); err != nil {
		return err
	}
	defer r.Body.Close()

	cq, err := s.db.CompileQuery(&queryReq)
	if err != nil {
		return fmt.Errorf("query error: %w", err)
	}

	items, err := s.db.QueryRecordsList(ctx, namespace, cq)
	if err != nil {
		return fmt.Errorf("query error: %w", err)
	}

	response := dexapi.RecordList{Records: items}

	w.Header().Set("Content-Type", "application/json")
	return json.NewEncoder(w).Encode(response)
}

func (s *Server) writeJSONLHandler(namespace string, w http.ResponseWriter, r *http.Request) error {
	ctx := r.Context()

	const (
		maxLines      = 100000
		maxLineLength = 1024 * 1024
	)

	scanner := bufio.NewScanner(r.Body)
	defer r.Body.Close()

	buf := make([]byte, maxLineLength)
	scanner.Buffer(buf, maxLineLength)

	var lines []string
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
		if len(lines) > maxLines {
			return fmt.Errorf("too many lines")
		}
	}

	response, err := s.db.InsertFlattenedRecords(ctx, namespace, lines)
	if err != nil {
		return err
	}

	w.Header().Set("Content-Type", "application/json")
	return json.NewEncoder(w).Encode(response)
}

func (s *Server) writeSingleRecordHandler(namespace string, w http.ResponseWriter, r *http.Request) error {
	ctx := r.Context()

	var req interface{}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		return err
	}

	newUUID, err := s.db.InsertObject(ctx, namespace, req)
	if err != nil {
		return err
	}

	response := map[string]string{"namespace": namespace, "uuid": newUUID.String()}

	w.Header().Set("Content-Type", "application/json")
	return json.NewEncoder(w).Encode(response)
}

// writeJSONError sends an error message in JSON format
func writeJSONError(w http.ResponseWriter, message string, statusCode int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(map[string]string{"error": message})
}

type CustomSpanExporter struct {
	delegate trace.SpanExporter
}

func NewCustomSpanExporter(delegate trace.SpanExporter) *CustomSpanExporter {
	return &CustomSpanExporter{delegate: delegate}
}

func (e *CustomSpanExporter) ExportSpans(ctx context.Context, spans []trace.ReadOnlySpan) error {
	var filteredSpans []trace.ReadOnlySpan
	for _, span := range spans {
		duration := span.EndTime().Sub(span.StartTime())

		if duration >= time.Millisecond {
			filteredSpans = append(filteredSpans, span)
		}
	}

	if len(filteredSpans) > 0 {
		return e.delegate.ExportSpans(ctx, filteredSpans)
	}
	return nil
}

func (e *CustomSpanExporter) Shutdown(ctx context.Context) error {
	return e.delegate.Shutdown(ctx)
}

type VersionInfo struct {
	CommitHash  string
	CommitTime  string
	DirtyCommit bool
	BinaryHash  string
}

func (v VersionInfo) VersionString() string {
	var rv string
	if v.CommitHash != "" {
		if v.DirtyCommit {
			rv = fmt.Sprintf("%s-dirty", v.CommitHash[:16])
		} else {
			rv = v.CommitHash[:16]
		}
	}

	if (rv == "" || v.DirtyCommit) && v.BinaryHash != "" {
		if rv != "" {
			rv += "@"
		}
		rv += fmt.Sprintf("sha256:%s", v.BinaryHash[:8])
	}

	return rv
}

func getVersionInfo(forceHash bool) (*VersionInfo, error) {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return nil, errors.New("failed to read build info")
	}

	var rv VersionInfo

	for _, setting := range info.Settings {
		if setting.Key == "vcs.revision" {
			rv.CommitHash = setting.Value
		}
		if setting.Key == "vcs.modified" {
			rv.DirtyCommit = setting.Value == "true"
		}
		if setting.Key == "vcs.time" {
			rv.CommitTime = setting.Value
		}
	}

	if rv.CommitHash == "" || rv.DirtyCommit || forceHash {
		execPath, err := os.Executable()
		if err != nil {
			return nil, err
		}

		file, err := os.Open(execPath)
		if err != nil {
			return nil, err
		}
		defer file.Close()

		h := sha256.New()

		if _, err := io.Copy(h, file); err != nil {
			return nil, err
		}

		hexdigest := fmt.Sprintf("%x", h.Sum(nil))
		rv.BinaryHash = hexdigest
	}

	return &rv, nil
}

func Main() error {
	ctx := context.Background()

	zapconfig := zap.NewDevelopmentConfig()
	zapconfig.EncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
	logger, err := zapconfig.Build()
	if err != nil {
		return err
	}
	defer logger.Sync()

	undo := zap.ReplaceGlobals(logger)
	defer undo()

	info, err := getVersionInfo(false)
	if err != nil {
		return err
	}

	zap.L().Info("Starting poindexter server",
		zap.String("version", info.VersionString()),
		zap.String("git_commit", info.CommitHash),
		zap.Bool("git_dirty", info.DirtyCommit),
		zap.String("binary_hash", info.BinaryHash),
		zap.String("build_time", info.CommitTime),
	)

	postgresCreds := poindexterdb.PostgresConfig{
		PostgresHost: os.Getenv("PGHOST"),
		PostgresUser: os.Getenv("PGUSER"),
		PostgresDB:   os.Getenv("PGDATABASE"),
		PostgresPass: os.Getenv("PGPASSWORD"),
	}

	traceAttributes := []attribute.KeyValue{
		semconv.ServiceNameKey.String("poindexter-server"),
		semconv.ServiceVersionKey.String(info.VersionString()),
		semconv.HostNameKey.String(os.Getenv("HOSTNAME")),
		attribute.String("database.name", postgresCreds.PostgresDB),
	}

	configValue := os.Getenv("POINDEXTER_CONFIG")
	if configValue == "" {
		return fmt.Errorf("POINDEXTER_CONFIG environment variable not set")
	}

	cfg, err := config.Load(configValue)
	if err != nil {
		return err
	}

	exporter, err := otlptrace.New(ctx, otlptracehttp.NewClient())
	if err != nil {
		log.Fatalf("Failed to create exporter: %v", err)
	}

	customExporter := NewCustomSpanExporter(exporter)

	tp := trace.NewTracerProvider(
		trace.WithBatcher(customExporter),
		trace.WithResource(resource.NewSchemaless(traceAttributes...)),
	)

	otel.SetTracerProvider(tp)

	defer func() {
		if err := tp.Shutdown(ctx); err != nil {
			log.Fatalf("Error shutting down tracer provider: %v", err)
		}
	}()

	serv, err := New(WithConfig(*cfg), WithPostgresCreds(postgresCreds))
	if err != nil {
		return err
	}

	return serv.Run()
}
