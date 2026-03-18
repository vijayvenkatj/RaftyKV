package main

import (
	"fmt"

	"github.com/vijayvenkatj/kv-store/internal/server/store"
)

func main() {

	storeInstance := store.New()

	//ops := []*wal.LogEntry{{
	//	Operation: "put",
	//	Key:       "key1",
	//	Value:     "value1",
	//},
	//	{
	//		Operation: "put",
	//		Key:       "key2",
	//		Value:     "value2",
	//	}}
	//
	//for _, op := range ops {
	//	err := storeInstance.Apply(op)
	//	if err != nil {
	//		fmt.Println(err)
	//	}
	//}

	storeInstance.Restore()

	fmt.Println(storeInstance.Get("key1"))

	return
}
