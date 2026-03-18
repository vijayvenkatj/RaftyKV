package wal

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sync"
)

type LogEntry struct {
	Operation string
	Key       string
	Value     string
}

type WAL struct {
	mu   sync.Mutex
	file *os.File
}

func NewWAL(filePath string) (*WAL, error) {
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	return &WAL{
		file: file,
	}, nil
}

func Close(wal *WAL) error {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	if wal.file != nil {
		return wal.file.Close()
	}

	return nil
}

func (wal *WAL) Append(entry *LogEntry) error {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	data := encode(entry)

	n, err := wal.file.Write(data)
	if err != nil {
		return err
	}
	if n != len(data) {
		return io.ErrShortWrite
	}

	if err := wal.file.Sync(); err != nil {
		return err
	}

	return nil
}

func (wal *WAL) Restore() ([]*LogEntry, error) {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	if wal.file == nil {
		return nil, nil
	}

	stat, _ := wal.file.Stat()
	fmt.Println("WAL size:", stat.Size())

	if _, err := wal.file.Seek(0, 0); err != nil {
		return nil, err
	}

	entries := make([]*LogEntry, 0)

	for {
		header := make([]byte, 12)

		_, err := io.ReadFull(wal.file, header)
		if err == io.EOF {
			break
		}
		if err != nil {
			break
		}

		opLen := binary.LittleEndian.Uint32(header[0:4])
		keyLen := binary.LittleEndian.Uint32(header[4:8])
		valLen := binary.LittleEndian.Uint32(header[8:12])

		totalBody := int(opLen + keyLen + valLen)

		body := make([]byte, totalBody)

		fmt.Println("Header:", header)
		fmt.Println("Lengths:", opLen, keyLen, valLen)

		_, err = io.ReadFull(wal.file, body)
		if err != nil {
			fmt.Println("Read Error:", err)
			break
		}

		data := make([]byte, 12+totalBody)
		copy(data[:12], header)
		copy(data[12:], body)

		entry, err := decode(data)
		if err != nil {
			return nil, err
		}

		fmt.Print(entry)

		entries = append(entries, entry)
	}

	return entries, nil
}
