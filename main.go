package main

import (
	"flag"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/vijayvenkatj/kv-store/internal/api"
)

func main() {

	// Flags
	nodeID := flag.Uint("id", 1, "node id (must be unique)")
	port := flag.String("port", "8080", "port to run server")
	path := flag.String("path", "./tmp", "data directory")
	peerData := flag.String("peers", "", "comma separated peers in form id=host:port")
	flag.Parse()

	// Parse peers
	peerMap := make(map[uint32]string)
	var peers []uint32

	if *peerData != "" {
		for _, p := range strings.Split(*peerData, ",") {
			p = strings.TrimSpace(p)
			if p == "" {
				continue
			}

			parts := strings.Split(p, "=")
			if len(parts) != 2 {
				panic(fmt.Sprintf("invalid peer format: %s (expected id=host:port)", p))
			}

			id64, err := strconv.ParseUint(parts[0], 10, 32)
			if err != nil {
				panic(fmt.Sprintf("invalid peer id: %s", parts[0]))
			}

			id := uint32(id64)
			addr := parts[1]

			peerMap[id] = addr
			peers = append(peers, id)
		}
	}

	if _, exists := peerMap[uint32(*nodeID)]; exists {
		panic("peer list should not include self node")
	}

	// Build config
	config := api.Config{
		NodeID:    uint32(*nodeID),
		Address:   fmt.Sprintf(":%s", *port),
		Path:      fmt.Sprintf("%s/node-%d", *path, *nodeID),
		ElectionT: 5 * time.Second,
		Peers:     peers,
		PeerMap:   peerMap,
	}

	// Start server
	server := api.NewServer(config)

	fmt.Printf("Node %d starting on %s\n", config.NodeID, config.Address)
	fmt.Printf("Peers: %+v\n", config.PeerMap)

	if err := server.ListenAndServe(); err != nil {
		panic(err)
	}
}
