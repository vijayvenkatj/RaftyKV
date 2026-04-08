package store

import (
	"errors"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/vijayvenkatj/kv-store/internal/server/wal"
)

type Config struct {
	NodeID    uint32
	Peers     []uint32
	PeerMap   map[uint32]string
	IsLeader  bool
	Path      string
	ElectionT time.Duration
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

	// LeaderId		uint32 -> TODO: add in the future
	NodeID      uint32
	CurrentTerm uint32
	VotedFor    uint32

	ElectionT time.Duration
	resetCh   chan struct{}

	LastApplied uint32
	CommitIndex uint32

	followers   []uint32
	followerMap map[uint32]string
	state       InstanceType

	NextIndex  map[uint32]uint32
	MatchIndex map[uint32]uint32

	wal  *wal.WAL
	snap *wal.Snapshot

	httpClient *http.Client
}

func New(config Config) *Store {
	walInstance, err := wal.NewWAL(config.Path)
	if err != nil {
		panic(err)
	}

	snapInstance := wal.NewSnapshot(config.Path)
	var httpClient = &http.Client{
		Timeout: 2 * time.Second,
	}

	var state InstanceType
	if config.IsLeader {
		state = Leader
	} else {
		state = Follower
	}

	store := &Store{
		data: make(map[string]string),

		NodeID:    config.NodeID,
		ElectionT: config.ElectionT,
		resetCh:   make(chan struct{}),

		LastApplied: 0,
		CommitIndex: 0,

		followers:   config.Peers,
		followerMap: make(map[uint32]string),
		state:       state,

		NextIndex:  make(map[uint32]uint32),
		MatchIndex: make(map[uint32]uint32),

		wal:  walInstance,
		snap: snapInstance,

		httpClient: httpClient,
	}

	store.cond = sync.NewCond(&store.mu)
	store.followerMap = config.PeerMap

	err = store.Restore()
	if err != nil {
		panic(err)
	}

	lastLog := store.wal.LastIndex
	for _, follower := range store.followers {
		store.NextIndex[follower] = lastLog + 1
		if store.state == Leader {
			go store.replicateWorker(follower)
		}
	}

	// TODO: fix follower selection logic

	if store.state == Follower {
		go store.runElectionTimer()
	}

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

func (s *Store) Close() error {
	return s.wal.Close()
}
