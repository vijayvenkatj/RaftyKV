package api

import (
	"errors"
	"log"
	"net/http"
	"time"

	"github.com/vijayvenkatj/kv-store/internal/server/store"
)

type Server struct {
	HttpServer *http.Server
}

type Config struct {
	Address   string
	ElectionT time.Duration

	NodeID  uint32
	Peers   []uint32
	PeerMap map[uint32]string

	Path string
}

func NewServer(config Config) *Server {

	storeConfig := store.Config{
		NodeID:    config.NodeID,
		ElectionT: config.ElectionT,
		Peers:     config.Peers,
		PeerMap:   config.PeerMap,
		Path:      config.Path,
	}

	storeInstance := store.New(storeConfig)
	handler := NewHandler(storeInstance, config)
	router := NewRouter(handler)

	return &Server{
		HttpServer: &http.Server{
			Addr:    config.Address,
			Handler: corsMiddleware(router),
		},
	}
}

func (server *Server) ListenAndServe() error {
	if err := server.HttpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("listen: %s\n", err)
		return err
	}
	return nil
}
