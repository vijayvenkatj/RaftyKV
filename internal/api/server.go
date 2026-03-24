package api

import (
	"errors"
	"log"
	"net/http"

	"github.com/vijayvenkatj/kv-store/internal/server/store"
)

type Server struct {
	HttpServer *http.Server
}

type Config struct {
	Address  string
	NodeId   string
	IsLeader bool
	Peers    []string
	Path     string
}

func NewServer(config Config) *Server {

	storeInstance := store.New(config.Path, config.Peers, config.IsLeader)
	handler := NewHandler(storeInstance, config)
	router := NewRouter(handler)

	return &Server{
		HttpServer: &http.Server{
			Addr:    config.Address,
			Handler: corsMiddleware(router),
		},
	}
}

func (server *Server) ListenAndServe() {
	if err := server.HttpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("listen: %s\n", err)
	}
}
