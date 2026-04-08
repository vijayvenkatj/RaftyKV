package store

import (
	"errors"
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/vijayvenkatj/kv-store/internal/proto/raft"
	"github.com/vijayvenkatj/kv-store/internal/server/wal"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Config struct {
	NodeID      uint32
	Peers       []uint32
	PeerMap     map[uint32]string
	Path        string
	ElectionT   time.Duration
	GrpcAddress string
}

var (
	DataDoesNotExistErr = errors.New("data does not exist")
	IllegalOperationErr = errors.New("illegal operation")
)

type InstanceType int

const (
	Follower  InstanceType = iota
	Leader                 = 1
	Candidate              = 2
)

type Store struct {
	mu   sync.RWMutex
	data map[string]string

	cond *sync.Cond

	LeaderId    uint32
	NodeID      uint32
	CurrentTerm uint32
	VotedFor    uint32

	ElectionT time.Duration
	resetCh   chan struct{}

	LastApplied uint32
	CommitIndex uint32

	raft.UnimplementedRaftServiceServer

	followers   []uint32
	grpcClients map[uint32]raft.RaftServiceClient
	state       InstanceType

	NextIndex  map[uint32]uint32
	MatchIndex map[uint32]uint32

	wal  *wal.WAL
	snap *wal.Snapshot

	grpcServer *grpc.Server
}

func New(config Config) *Store {
	walInstance, err := wal.NewWAL(config.Path)
	if err != nil {
		panic(err)
	}

	snapInstance := wal.NewSnapshot(config.Path)

	state := Follower

	store := &Store{
		data: make(map[string]string),

		NodeID:    config.NodeID,
		LeaderId:  config.NodeID,
		ElectionT: config.ElectionT,
		resetCh:   make(chan struct{}),

		LastApplied: 0,
		CommitIndex: 0,

		followers:   config.Peers,
		grpcClients: make(map[uint32]raft.RaftServiceClient),
		state:       state,

		NextIndex:  make(map[uint32]uint32),
		MatchIndex: make(map[uint32]uint32),

		wal:  walInstance,
		snap: snapInstance,
	}

	store.cond = sync.NewCond(&store.mu)

	// Set up gRPC connections to peers
	for id, addr := range config.PeerMap {
		parts := strings.Split(addr, ":")
		host := parts[0]
		port, _ := strconv.Atoi(parts[1])
		grpcAddr := fmt.Sprintf("%s:%d", host, port+10000)

		conn, err := grpc.Dial(grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Printf("Failed to dial peer %d: %v", id, err)
			continue
		}
		store.grpcClients[id] = raft.NewRaftServiceClient(conn)
	}

	err = store.Restore()
	if err != nil {
		panic(err)
	}

	lastLog := store.wal.LastIndex
	for _, follower := range store.followers {
		store.NextIndex[follower] = lastLog + 1
	}

	store.grpcServer = grpc.NewServer()
	raft.RegisterRaftServiceServer(store.grpcServer, store)

	lis, err := net.Listen("tcp", config.GrpcAddress)
	if err != nil {
		panic(fmt.Sprintf("Failed to start gRPC listener: %v", err))
	}

	go func() {
		if err := store.grpcServer.Serve(lis); err != nil {
			log.Fatalf("Failed to serve gRPC: %v", err)
		}
	}()

	go store.runElectionTimer()
	go store.ApplyLoop()

	return store
}

func (s *Store) Apply(entry *wal.LogEntry) error {
	s.mu.Lock()

	if !(s.state == Leader) {
		s.mu.Unlock()
		return errors.New("not leader")
	}

	entry.LogIndex = s.wal.LastIndex + 1
	entry.Term = s.CurrentTerm

	err := s.wal.Append(entry)
	if err != nil {
		s.mu.Unlock()
		return err
	}

	index := entry.LogIndex

	if len(s.followers) == 0 {
		s.CommitIndex = index
		s.cond.Broadcast()
	}

	s.mu.Unlock()

	return s.waitForCommit(index)
}

func (s *Store) waitForCommit(index uint32) error {
	deadline := time.Now().Add(2 * time.Second)

	s.mu.Lock()
	defer s.mu.Unlock()

	log.Printf("Waiting for index: %d, current: %d", index, s.CommitIndex)

	for s.CommitIndex < index {
		if time.Now().After(deadline) {
			log.Printf("Timeout waiting for commit index: %d, current: %d", index, s.CommitIndex)
			return errors.New("timeout")
		}
		s.cond.Wait()
	}

	log.Printf("Commit index: %d, current: %d", index, s.CommitIndex)

	return nil
}

func (s *Store) Get(key string) (string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	val, ok := s.data[key]
	if !ok {
		return "", DataDoesNotExistErr
	}
	return val, nil
}

func (s *Store) Restore() error {

	entries, err := s.wal.ReadAll()
	if err != nil {
		return err
	}

	for _, entry := range entries {
		err := s.applyToMemory(entry)
		if err != nil {
			return err
		}
		s.LastApplied = entry.LogIndex
	}

	s.wal.LastIndex = s.LastApplied
	s.CommitIndex = s.LastApplied

	return nil
}

func (s *Store) IsLeader() bool {
	return s.state == Leader
}

func (s *Store) becomeFollower(term uint32) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.becomeFollowerLocked(term)
}

func (s *Store) becomeLeader() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.becomeLeaderLocked()
}

func (s *Store) becomeFollowerLocked(term uint32) {
	if term < s.CurrentTerm {
		return
	}
	s.state = Follower
	s.CurrentTerm = term
	s.VotedFor = 0
}

func (s *Store) becomeLeaderLocked() {
	if s.state == Leader {
		return
	}

	log.Println("BECAME LEADER")
	s.state = Leader
	s.LeaderId = s.NodeID

	last := s.wal.LastIndex
	for _, f := range s.followers {
		s.NextIndex[f] = last + 1
		s.MatchIndex[f] = 0
		go s.replicateWorker(f)
	}
}

func (s *Store) Close() error {
	return s.wal.Close()
}
