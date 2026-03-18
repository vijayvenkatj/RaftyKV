package store

import (
	"errors"
	"sync"

	"github.com/vijayvenkatj/kv-store/internal/server/wal"
)

type Store struct {
	mu   sync.RWMutex
	data map[string]string
	wal  *wal.WAL
}

func New() *Store {
	walInstance, err := wal.NewWAL("tmp/wal.txt")
	if err != nil {
		panic(err)
	}

	return &Store{
		data: make(map[string]string),
		wal:  walInstance,
	}
}

var (
	DataDoesNotExistErr = errors.New("data does not exist")
	IllegalOperationErr = errors.New("illegal operation")
)

func (s *Store) Apply(entry *wal.LogEntry) error {

	// Add the log entry to WAL
	err := s.wal.Append(entry)
	if err != nil {
		return err
	}

	switch entry.Operation {
	case "put":
		s.mu.Lock()
		defer s.mu.Unlock()

		s.data[entry.Key] = entry.Value
		break
	case "delete":
		s.mu.Lock()
		defer s.mu.Unlock()

		delete(s.data, entry.Key)
		break
	default:
		return IllegalOperationErr
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

	entries, err := s.wal.Restore()
	if err != nil {
		return err
	}

	for _, entry := range entries {
		err := s.applyToMemory(entry)
		if err != nil {
			return err
		}
	}
	return nil
}
