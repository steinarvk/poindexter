// File: server.go
package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
)

// Option is the type for functional options that can return an error
type Option func(*Server) error

// Server holds the HTTP server configuration
type Server struct {
	host string
	port int
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

	return s, nil
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
type apiHandlerFunc func(username, namespace string, w http.ResponseWriter, r *http.Request) error

// Run starts the server
func (s *Server) Run() error {
	mux := http.NewServeMux()
	mux.Handle("/api/foo", s.middleware(s.apiHandler(s.fooHandler)))
	mux.Handle("/api/bar", s.middleware(s.apiHandler(s.barHandler)))

	address := fmt.Sprintf("%s:%d", s.host, s.port)
	fmt.Printf("Server is running on %s\n", address)
	return http.ListenAndServe(address, mux)
}

// middleware enforces basic auth and checks for a custom header, passing the validated username and namespace forward
func (s *Server) middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		username, password, ok := r.BasicAuth()
		if !ok || password != "pass" { // Simple password check, replace with real authentication
			writeJSONError(w, "Unauthorized", http.StatusUnauthorized)
			return
		}

		namespace := r.Header.Get("X-Namespace")
		if namespace == "" {
			writeJSONError(w, "Missing X-Namespace header", http.StatusBadRequest)
			return
		}

		// Store username and namespace in request context for downstream handlers
		ctx := r.Context()
		ctx = context.WithValue(ctx, "username", username)
		ctx = context.WithValue(ctx, "namespace", namespace)
		r = r.WithContext(ctx)

		next.ServeHTTP(w, r)
	})
}

// apiHandler adapts our custom handler type to http.Handler
func (s *Server) apiHandler(handler apiHandlerFunc) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Retrieve username and namespace from context
		username := r.Context().Value("username").(string)
		namespace := r.Context().Value("namespace").(string)

		err := handler(username, namespace, w, r)
		if err != nil {
			writeJSONError(w, err.Error(), http.StatusInternalServerError)
		}
	})
}

func (s *Server) fooHandler(username, namespace string, w http.ResponseWriter, r *http.Request) error {
	response := map[string]string{"message": "foo called", "username": username, "namespace": namespace}
	w.Header().Set("Content-Type", "application/json")
	return json.NewEncoder(w).Encode(response)
}

func (s *Server) barHandler(username, namespace string, w http.ResponseWriter, r *http.Request) error {
	response := map[string]string{"message": "bar called", "username": username, "namespace": namespace}
	w.Header().Set("Content-Type", "application/json")
	return json.NewEncoder(w).Encode(response)
}

// writeJSONError sends an error message in JSON format
func writeJSONError(w http.ResponseWriter, message string, statusCode int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(map[string]string{"error": message})
}
