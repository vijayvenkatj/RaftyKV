package main

import (
	"fmt"

	"github.com/vijayvenkatj/kv-store/internal/server/store"
	"github.com/vijayvenkatj/kv-store/internal/server/wal"
)

func main() {

	storeInstance := store.New("tmp/wal.log")

	for {
		var operation string
		fmt.Scan(&operation)

		if operation == "put" {
			var key, value string
			fmt.Scan(&key, &value)

			entry := &wal.LogEntry{
				Operation: operation,
				Key:       key,
				Value:     value,
			}
			err := storeInstance.Apply(entry)
			if err != nil {
				fmt.Println(err)
				return
			}
		}

		if operation == "delete" {
			var key string
			fmt.Scan(&key)

			entry := &wal.LogEntry{
				Operation: operation,
				Key:       key,
				Value:     "",
			}
			err := storeInstance.Apply(entry)
			if err != nil {
				fmt.Println(err)
				return
			}
		}

		if operation == "get" {
			var key string
			fmt.Scan(&key)
			fmt.Println(storeInstance.Get(key))
		}

		if operation == "stop" {
			break
		}

		if operation == "restore" {
			err := storeInstance.Restore()
			if err != nil {
				fmt.Println(err)
			}
		}
	}

	return
}
