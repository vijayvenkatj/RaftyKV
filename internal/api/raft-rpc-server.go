package api

import (
	"context"

	"github.com/vijayvenkatj/kv-store/internal/proto/raft"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type RaftRPCServer struct {
	raft.UnimplementedRaftServiceServer
	shards *ShardManager
}

func NewRaftRPCServer(shards *ShardManager) *RaftRPCServer {
	return &RaftRPCServer{
		shards: shards,
	}
}

func (server *RaftRPCServer) AppendEntries(ctx context.Context, req *raft.AppendEntriesRequest) (*raft.AppendEntriesResponse, error) {
	store, err := server.shards.GetLocalShard(req.ShardId)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "shard %d not found on node", req.ShardId)
	}
	return store.HandleAppendEntries(ctx, req)
}

func (server *RaftRPCServer) RequestVote(ctx context.Context, req *raft.RequestVoteRequest) (*raft.RequestVoteResponse, error) {
	store, err := server.shards.GetLocalShard(req.ShardId)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "shard %d not found on node", req.ShardId)
	}
	return store.HandleRequestVote(ctx, req)
}
