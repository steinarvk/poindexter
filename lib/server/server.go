// File: server.go
package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/steinarvk/recdex/lib/config"
)

// Option is the type for functional options that can return an error
type Option func(*Server) error

// Server holds the HTTP server configuration
type Server struct {
	host   string
	port   int
	config config.Config
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
	mux.Handle("/api/read/", s.middleware(readApiHandler{s.fooHandler}))
	mux.Handle("/api/write/", s.middleware(writeApiHandler{s.barHandler}))

	address := fmt.Sprintf("%s:%d", s.host, s.port)
	fmt.Printf("Server is running on %s\n", address)
	return http.ListenAndServe(address, mux)
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
			log.Printf("rejecting access to user %q to namespace %q", username, namespace)

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

		log.Printf("checked access for user %q to namespace %q: %+v", username, namespace, accessLevel)

		if err := next.CheckAndServeHTTP(accessLevel, namespace, w, r); err != nil {
			if err == errUnauthorized {
				serveUnauthorized()
				return
			}
			writeJSONError(w, err.Error(), http.StatusInternalServerError)
		}
	})
}

func (s *Server) fooHandler(namespace string, w http.ResponseWriter, r *http.Request) error {
	response := map[string]string{"message": "foo called", "namespace": namespace}
	w.Header().Set("Content-Type", "application/json")
	return json.NewEncoder(w).Encode(response)
}

func (s *Server) barHandler(namespace string, w http.ResponseWriter, r *http.Request) error {
	response := map[string]string{"message": "bar called", "namespace": namespace}
	w.Header().Set("Content-Type", "application/json")
	return json.NewEncoder(w).Encode(response)
}

// writeJSONError sends an error message in JSON format
func writeJSONError(w http.ResponseWriter, message string, statusCode int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(map[string]string{"error": message})
}
