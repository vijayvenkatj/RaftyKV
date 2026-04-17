package api

import (
	"errors"
	"log"
	"net"
	"net/http"
	"time"

	"github.com/vijayvenkatj/kv-store/internal/proto/raft"
	"google.golang.org/grpc"
)

type Server struct {
	HttpServer *http.Server
	GrpcServer *grpc.Server
}

type Config struct {
	Address     string
	GrpcAddress string
	ElectionT   time.Duration

	NodeID    uint32
	ShardList map[uint32][]Location

	Path string
}

func NewServer(config Config) *Server {

	shardManager := NewShardManager(config.ShardList, config.Address, config)
	handler := NewHandler(shardManager, config)
	router := NewRouter(handler)
	raftServer := grpc.NewServer()
	raft.RegisterRaftServiceServer(raftServer, NewRaftRPCServer(shardManager))

	lis, err := net.Listen("tcp", config.GrpcAddress)
	if err != nil {
		panic(err)
	}

	go func() {
		if err := raftServer.Serve(lis); err != nil {
			log.Fatalf("listen: %s\n", err)
		}
	}()

	return &Server{
		HttpServer: &http.Server{
			Addr:    config.Address,
			Handler: corsMiddleware(router),
		},
		GrpcServer: raftServer,
	}
}

func (server *Server) ListenAndServe() error {
	if err := server.HttpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("listen: %s\n", err)
		return err
	}
	return nil
}
