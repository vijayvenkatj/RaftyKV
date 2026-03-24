package main

import (
	"flag"
	"fmt"
	"strings"

	"github.com/vijayvenkatj/kv-store/internal/api"
)

func main() {

	var nodeId = flag.String("id", "", "node id")
	var port = flag.String("port", "8080", "port to run server")
	var path = flag.String("path", "./tmp", "path to store")
	var peerData = flag.String("peers", "", "comma separated peer addresses")
	var isLeader = flag.Bool("leader", false, "node type")
	flag.Parse()

	var peers []string
	for _, peer := range strings.Split(*peerData, ",") {
		peers = append(peers, strings.TrimSpace(peer))
	}

	config := api.Config{
		NodeId:   *nodeId,
		IsLeader: *isLeader,
		Address:  fmt.Sprintf(":%s", *port),
		Path:     *path,
		Peers:    peers,
	}

	server := api.NewServer(config)
	server.ListenAndServe()

	return
}
