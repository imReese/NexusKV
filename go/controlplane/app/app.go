package app

import (
	"encoding/json"
	"net/http"
)

type ServerConfig struct {
	ListenAddress string
}

type Server struct {
	config ServerConfig
	mux    *http.ServeMux
	routes []string
}

func NewServer(cfg ServerConfig) *Server {
	if cfg.ListenAddress == "" {
		cfg.ListenAddress = "127.0.0.1:8081"
	}

	server := &Server{
		config: cfg,
		mux:    http.NewServeMux(),
	}

	server.register("/livez", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	server.register("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ready"))
	})
	server.register("/v1alpha/admin/status", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{
			"component": "controlplane",
			"status":    "starting",
		})
	})

	return server
}

func (s *Server) Handler() http.Handler {
	return s.mux
}

func (s *Server) Routes() []string {
	out := make([]string, len(s.routes))
	copy(out, s.routes)
	return out
}

func (s *Server) register(route string, handler http.HandlerFunc) {
	s.routes = append(s.routes, route)
	s.mux.HandleFunc(route, handler)
}
