package store

import (
	"errors"
	"fmt"
	"sync"

	"github.com/vijayvenkatj/kv-store/internal/server/wal"
)

var (
	DataDoesNotExistErr = errors.New("data does not exist")
	IllegalOperationErr = errors.New("illegal operation")
)

type Store struct {
	mu   sync.RWMutex
	data map[string]string

	lastAppliedIndex uint32
	commitIndex      uint32

	lastSnapIndex uint32
	threshold     uint32

	peers    []string
	isLeader bool

	wal  *wal.WAL
	snap *wal.Snapshot
}

func New(filePath string, peers []string, isLeader bool) *Store {
	walInstance, err := wal.NewWAL(filePath)
	if err != nil {
		panic(err)
	}

	snapInstance := wal.NewSnapshot(filePath)

	store := &Store{
		data:             make(map[string]string),
		threshold:        5,
		lastSnapIndex:    0,
		lastAppliedIndex: 0,
		wal:              walInstance,
		snap:             snapInstance,

		isLeader: isLeader,
		peers:    peers,
	}

	err = store.Restore()
	if err != nil {
		panic(err)
	}

	return store
}

func (s *Store) Apply(entry *wal.LogEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	entry.LogIndex = s.lastAppliedIndex + 1

	err := s.wal.Append(entry)
	if err != nil {
		return err
	}

	// Send to Peers and check for ACKs

	// If majority achieved -> go ahead

	switch entry.Operation {
	case "put":
		s.data[entry.Key] = entry.Value
		break
	case "delete":
		delete(s.data, entry.Key)
		break
	default:
		return IllegalOperationErr
	}

	s.lastAppliedIndex++

	if s.lastAppliedIndex-s.lastSnapIndex >= s.threshold {

		// Copy the data
		oldData := make(map[string]string)
		for k, v := range s.data {
			oldData[k] = v
		}
		lastIndex := s.lastAppliedIndex

		// Snapshot
		err = s.snap.Save(oldData, lastIndex)
		if err != nil {
			fmt.Println("Error writing snapshot:", err)
			return err
		}

		// Truncate WAL
		err = s.wal.Reset()
		if err != nil {
			fmt.Println("Error resetting wal:", err)
		}
		s.lastSnapIndex = lastIndex

		return nil
	}

	return nil
}

func (s *Store) applyToMemory(entry *wal.LogEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	switch entry.Operation {
	case "put":
		s.data[entry.Key] = entry.Value
	case "delete":
		delete(s.data, entry.Key)
	default:
		return IllegalOperationErr
	}

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

	snapData, err := s.snap.Read()
	if err != nil {
		return err
	}
	s.data = snapData.Data

	entries, err := s.wal.ReadAll()
	if err != nil {
		return err
	}

	for _, entry := range entries {
		if entry.LogIndex <= snapData.LastIndex {
			continue
		}
		err := s.applyToMemory(entry)
		if err != nil {
			return err
		}
		s.lastAppliedIndex = entry.LogIndex
	}

	return nil
}

func (s *Store) Close() error {
	return s.wal.Close()
}
