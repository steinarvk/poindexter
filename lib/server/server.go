package server

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/XSAM/otelsql"
	"github.com/gibson042/canonicaljson-go"
	"github.com/google/uuid"
	"github.com/gorilla/mux"
	"github.com/steinarvk/poindexter/lib/config"
	"github.com/steinarvk/poindexter/lib/dexapi"
	"github.com/steinarvk/poindexter/lib/dexerror"
	"github.com/steinarvk/poindexter/lib/logging"
	"github.com/steinarvk/poindexter/lib/poindexterdb"
	"github.com/steinarvk/poindexter/lib/version"
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
	r := mux.NewRouter()

	apiRouter := r.PathPrefix("/api").Subrouter()

	addHandler := func(method string, pattern string, handler VerifyingApiHandler) {
		h := s.middleware(handler)
		h = otelhttp.NewHandler(h, pattern)
		apiRouter.Handle(pattern, h).Methods(method)
	}

	addHandler("POST", "/query/{ns}/records/", queryApiHandler{s.readQueryRecordsHandler})
	addHandler("POST", "/query/{ns}/fields/", queryApiHandler{s.readQueryFieldsHandler})
	addHandler("POST", "/query/{ns}/values/{field}/", queryApiHandler{s.readQueryFieldValuesHandler})
	addHandler("GET", "/query/{ns}/records/by/{field}/{value}/", queryApiHandler{s.lookupRecordByField})
	addHandler("GET", "/query/{ns}/records/{id}/", queryApiHandler{s.lookupRecordByID})
	addHandler("GET", "/query/{ns}/entities/{id}/", queryApiHandler{s.lookupEntityByID})

	addHandler("POST", "/ingest/{ns}/record/", ingestApiHandler{s.writeSingleRecordHandler})
	addHandler("POST", "/ingest/{ns}/upsert/", ingestApiHandler{s.upsertEntityHandler})
	addHandler("POST", "/ingest/{ns}/jsonl/", ingestApiHandler{s.ingestJSONLHandler})
	addHandler("POST", "/ingest/{ns}/batches/check/", ingestApiHandler{s.ingestCheckBatchesHandler})
	addHandler("GET", "/ingest/{ns}/batches/{batch}/", ingestApiHandler{s.ingestCheckBatchHandler})
	addHandler("POST", "/ingest/{ns}/batches/{batch}/jsonl/", ingestApiHandler{s.ingestJSONLHandler})

	apiRouter.NotFoundHandler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.serveAPIError(w, r, s.make404Error(r))
	})

	s.httpServer.Handler = r

	zap.L().Sugar().Infof("Server is running on %s", s.httpServer.Addr)

	return s.httpServer.ListenAndServe()
}

func (s *Server) HTTPServer() *http.Server {
	return s.httpServer
}

type VerifyingApiHandler interface {
	CheckAndServeHTTP(access config.AccessLevel, namespace string, w http.ResponseWriter, r *http.Request) error
}

type queryApiHandler struct {
	handler apiHandlerFunc
}

func (v queryApiHandler) CheckAndServeHTTP(access config.AccessLevel, namespace string, w http.ResponseWriter, r *http.Request) error {
	if !access.QueryAccess {
		return errUnauthorized
	}

	return v.handler(namespace, w, r)
}

var (
	errUnauthorized = errors.New("unauthorized")
)

type ingestApiHandler struct {
	handler apiHandlerFunc
}

func (v ingestApiHandler) CheckAndServeHTTP(access config.AccessLevel, namespace string, w http.ResponseWriter, r *http.Request) error {
	if !access.IngestAccess {
		return errUnauthorized
	}

	return v.handler(namespace, w, r)
}

// middleware enforces basic auth and checks for a custom header, passing the validated username and namespace forward
func (s *Server) middleware(next VerifyingApiHandler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t0 := time.Now()

		userAgent := r.Header.Get("User-Agent")

		var authClient *config.Client

		username, password, ok := r.BasicAuth()
		if ok {
			client, ok := s.config.Clients[username]
			if ok && client.SharedSecret == password {
				authClient = &client
			}
		}

		pathVars := mux.Vars(r)
		namespace, ok := pathVars["ns"]
		if !ok {
			writeJSONError(w, "missing namespace", http.StatusBadRequest)
			return
		}

		debugHeader := r.Header.Get("X-Debug") == "true"

		requestLogger := zap.L().With(
			zap.String("method", r.Method),
			zap.String("path", r.URL.Path),
			zap.String("user_agent", userAgent),
			zap.String("username", username),
			zap.String("namespace", namespace),
		)

		requestLogger.Info("processing incoming request")

		r = r.WithContext(logging.NewContextWithLogger(r.Context(), requestLogger, debugHeader))

		serveUnauthorized := func() {
			zap.L().Sugar().Infof("rejecting access to user %q to namespace %q", username, namespace)

			leftUntilSecond := time.Until(t0.Add(time.Second))
			time.Sleep(leftUntilSecond)
			writeJSONError(w, "Unauthorized", http.StatusUnauthorized)
		}

		if authClient == nil {
			requestLogger.Info("rejecting access due to bad credentials")
			serveUnauthorized()
			return
		}

		if namespace == "" {
			requestLogger.Info("rejecting access due to missing namespace")
			serveUnauthorized()
			return
		}

		accessLevel, ok := authClient.Access[namespace]
		if !ok {
			requestLogger.Info("rejecting access due to unauthorized namespace")
			serveUnauthorized()
			return
		}

		requestLogger.Sugar().Debugf("checked access for user %q to namespace %q: %+v", username, namespace, accessLevel)

		if err := next.CheckAndServeHTTP(accessLevel, namespace, w, r); err != nil {
			if err == errUnauthorized {
				serveUnauthorized()
				return
			}

			s.serveAPIError(w, r, err)
		} else {
			duration := time.Since(t0)

			requestLogger.Info("request OK", zap.Duration("duration", duration))
		}
	})
}

func (s *Server) readQueryFieldsHandler(namespace string, w http.ResponseWriter, r *http.Request) error {
	ctx := r.Context()

	var queryReq dexapi.Query
	if err := s.decodeRequest(r, &queryReq); err != nil {
		return err
	}
	defer r.Body.Close()

	cq, err := s.db.CompileQuery(&queryReq)
	if err != nil {
		return fmt.Errorf("query error: %w", err)
	}

	resp, err := s.db.QueryFieldsList(ctx, namespace, cq)
	if err != nil {
		return fmt.Errorf("query error: %w", err)
	}

	w.Header().Set("Content-Type", "application/json")
	return json.NewEncoder(w).Encode(resp)
}

func (s *Server) readQueryFieldValuesHandler(namespace string, w http.ResponseWriter, r *http.Request) error {
	ctx := r.Context()

	fieldName := mux.Vars(r)["field"]
	if fieldName == "" {
		return dexerror.New(
			dexerror.WithHTTPCode(400),
			dexerror.WithErrorID("bad_request.missing_field_name"),
		)
	}

	var queryReq dexapi.Query
	if err := s.decodeRequest(r, &queryReq); err != nil {
		return err
	}
	defer r.Body.Close()

	cq, err := s.db.CompileQuery(&queryReq)
	if err != nil {
		return fmt.Errorf("query error: %w", err)
	}

	fieldNames := []string{
		fieldName,
	}

	resp, err := s.db.QueryValuesList(ctx, namespace, cq, fieldNames)
	if err != nil {
		return fmt.Errorf("query error: %w", err)
	}

	w.Header().Set("Content-Type", "application/json")
	return json.NewEncoder(w).Encode(resp)
}

func (s *Server) readQueryRecordsHandler(namespace string, w http.ResponseWriter, r *http.Request) error {
	ctx := r.Context()

	var queryReq dexapi.Query
	if err := s.decodeRequest(r, &queryReq); err != nil {
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

func (s *Server) ingestJSONLHandler(namespace string, w http.ResponseWriter, r *http.Request) error {
	const (
		maxLines      = 100000
		maxLineLength = 1024 * 1024
	)

	batchID := mux.Vars(r)["batch"]

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

	var response *dexapi.IngestionResponse
	var err error

	if batchID == "" {
		response, err = s.db.InsertFlattenedRecords(r.Context(), namespace, lines)
	} else {
		response, err = s.db.InsertFlattenedRecordsAsBatch(r.Context(), namespace, lines, batchID)
	}
	if err != nil {
		return err
	}

	w.Header().Set("Content-Type", "application/json")
	return json.NewEncoder(w).Encode(response)
}

func (s *Server) writeSingleRecordHandler(namespace string, w http.ResponseWriter, r *http.Request) error {
	ctx := r.Context()

	var req interface{}
	if err := s.decodeRequest(r, &req); err != nil {
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

	info, err := version.GetInfo()
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

func (s *Server) notImplementedHandler(namespace string, w http.ResponseWriter, r *http.Request) error {
	return dexerror.New(
		dexerror.WithHTTPCode(500),
		dexerror.WithErrorID("internal_error.endpoint_not_implemented"),
		dexerror.WithPublicMessage("endpoint not implemented yet"),
		dexerror.WithPublicData("path", r.URL.Path),
	)
}

func (s *Server) make404Error(r *http.Request) error {
	return dexerror.New(
		dexerror.WithHTTPCode(404),
		dexerror.WithErrorID("bad_request.no_such_endpoint"),
		dexerror.WithPublicMessage("No such API endpoint"),
		dexerror.WithPublicData("method", r.Method),
		dexerror.WithPublicData("path", r.URL.Path),
	)
}

func (s *Server) serveAPIError(w http.ResponseWriter, r *http.Request, err error) {
	logger := logging.FromContext(r.Context())

	apiErr := dexerror.AsPoindexterError(err)

	logger.Info(
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
		logger.Error("error marshalling error", zap.Error(oopsErr))
		w.Write([]byte(`{"error": {"message": "internal error"}}`))
	} else {
		w.Write(marshalled)
	}
}

func (s *Server) lookupRecordByID(namespace string, w http.ResponseWriter, r *http.Request) error {
	ctx := r.Context()

	recordID, err := uuid.Parse(mux.Vars(r)["id"])
	if err != nil {
		return dexerror.New(
			dexerror.WithHTTPCode(400),
			dexerror.WithErrorID("bad_request.bad_record_id"),
			dexerror.WithPublicMessage("Bad record ID"),
			dexerror.WithPublicData("record_id", mux.Vars(r)["id"]),
			dexerror.WithPublicData("error", err.Error()),
		)
	}

	recorditem, err := s.db.LookupObjectByRecordID(ctx, namespace, recordID)
	if err != nil {
		return err
	}

	response := dexapi.LookupRecordResponse{
		RecordItem: *recorditem,
	}

	return json.NewEncoder(w).Encode(response)
}

func (s *Server) lookupEntityByID(namespace string, w http.ResponseWriter, r *http.Request) error {
	ctx := r.Context()

	entityID, err := uuid.Parse(mux.Vars(r)["id"])
	if err != nil {
		return dexerror.New(
			dexerror.WithHTTPCode(400),
			dexerror.WithErrorID("bad_request.bad_entity_id"),
			dexerror.WithPublicMessage("Bad entity ID"),
			dexerror.WithPublicData("entity_id", mux.Vars(r)["id"]),
			dexerror.WithPublicData("error", err.Error()),
		)
	}

	recorditem, err := s.db.LookupObjectByEntityID(ctx, namespace, entityID)
	if err != nil {
		return err
	}

	response := dexapi.LookupRecordResponse{
		RecordItem: *recorditem,
	}

	return json.NewEncoder(w).Encode(response)
}

func (s *Server) ingestCheckBatchHandler(namespace string, w http.ResponseWriter, r *http.Request) error {
	ctx := r.Context()

	batchName := mux.Vars(r)["batch"]

	present, err := s.db.CheckBatch(ctx, namespace, batchName)
	if err != nil {
		return err
	}

	if !present {
		return dexerror.New(
			dexerror.WithHTTPCode(404),
			dexerror.WithUnremarkable(),
			dexerror.WithErrorID("not_found.batch_not_found"),
			dexerror.WithPublicMessage("Batch not found"),
			dexerror.WithPublicData("batch_name", batchName),
		)
	}

	response := dexapi.CheckBatchResponse{
		BatchStatus: dexapi.BatchStatus{
			Namespace: namespace,
			BatchName: batchName,
			Processed: true,
		},
	}

	return json.NewEncoder(w).Encode(response)
}

func (s *Server) lookupRecordByField(namespace string, w http.ResponseWriter, r *http.Request) error {
	ctx := r.Context()

	fieldName := mux.Vars(r)["field"]

	fieldValueString := mux.Vars(r)["value"]

	// How to canonicalize? TODO for now just assume it's a string.
	valueAsCanonicalBytes, err := canonicaljson.Marshal(fieldValueString)
	if err != nil {
		return err
	}

	recorditem, err := s.db.LookupObjectByField(ctx, namespace, fieldName, string(valueAsCanonicalBytes))
	if err != nil {
		return err
	}

	response := dexapi.LookupRecordResponse{
		RecordItem: *recorditem,
	}

	return json.NewEncoder(w).Encode(response)
}

func (s *Server) decodeRequest(r *http.Request, v interface{}) error {
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(v); err != nil {
		return dexerror.New(
			dexerror.WithHTTPCode(400),
			dexerror.WithErrorID("bad_request.invalid_request"),
			dexerror.WithPublicMessage("invalid request"),
			dexerror.WithPublicData("error", err.Error()),
		)
	}
	return nil
}

func (s *Server) ingestCheckBatchesHandler(namespace string, w http.ResponseWriter, r *http.Request) error {
	ctx := r.Context()

	var checkBatchesRequest dexapi.CheckBatchesRequest
	if err := s.decodeRequest(r, &checkBatchesRequest); err != nil {
		return err
	}

	if len(checkBatchesRequest.BatchNames) == 0 {
		return dexerror.New(
			dexerror.WithHTTPCode(400),
			dexerror.WithErrorID("bad_request.no_batches"),
			dexerror.WithPublicMessage("No batches specified"),
		)
	}

	statuses, err := s.db.CheckBatches(ctx, namespace, checkBatchesRequest.BatchNames)
	if err != nil {
		return err
	}

	response := dexapi.CheckBatchesResponse{
		Batches: statuses,
	}

	return json.NewEncoder(w).Encode(response)
}

func (s *Server) upsertEntityHandler(namespace string, w http.ResponseWriter, r *http.Request) error {
	ctx := r.Context()

	var req interface{}
	if err := s.decodeRequest(r, &req); err != nil {
		return err
	}

	// TODO -- theoretically record ID and timestamp are unnecessary here.
	// Could just autogenerate them if missing?

	response, err := s.db.UpsertEntity(ctx, namespace, req)
	if err != nil {
		return err
	}

	w.Header().Set("Content-Type", "application/json")
	if response.Updated {
		// The entity has been updated = a record has been created
		w.WriteHeader(http.StatusCreated)
	} else {
		w.WriteHeader(http.StatusOK)
	}
	return json.NewEncoder(w).Encode(response)
}
