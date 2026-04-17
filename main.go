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
	shardData := flag.String("shardMap", "", "shard map in form shardID=nodeID@host:port,nodeID@host:port;shardID=nodeID@host:port")
	flag.Parse()

	shardList, err := parseShardMap(*shardData)
	if err != nil {
		panic(err)
	}

	nodeExists := false
	for _, locations := range shardList {
		for _, location := range locations {
			if location.NodeID == uint32(*nodeID) {
				nodeExists = true
				break
			}
		}
		if nodeExists {
			break
		}
	}
	if !nodeExists {
		panic(fmt.Sprintf("node %d is not present in shardMap", *nodeID))
	}

	portInt, err := strconv.Atoi(*port)
	if err != nil {
		panic(fmt.Sprintf("invalid port: %s", *port))
	}
	grpcPort := portInt + 10000

	// Build config
	config := api.Config{
		NodeID:      uint32(*nodeID),
		Address:     fmt.Sprintf(":%s", *port),
		GrpcAddress: fmt.Sprintf(":%d", grpcPort),
		Path:        fmt.Sprintf("%s/node-%d", *path, *nodeID),
		ElectionT:   5 * time.Second,

		ShardList: shardList,
	}

	// Start server
	server := api.NewServer(config)

	fmt.Printf("Node %d starting on %s\n", config.NodeID, config.Address)
	fmt.Printf("ShardMap: %+v\n", config.ShardList)

	if err := server.ListenAndServe(); err != nil {
		panic(err)
	}
}

func parseShardMap(raw string) (map[uint32][]api.Location, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, fmt.Errorf("shardMap is required")
	}

	shardMap := make(map[uint32][]api.Location)

	for _, shardEntry := range strings.Split(raw, ";") {
		shardEntry = strings.TrimSpace(shardEntry)
		if shardEntry == "" {
			continue
		}

		parts := strings.SplitN(shardEntry, "=", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid shard entry: %q (expected shardID=nodeID@host:port,...)", shardEntry)
		}

		shardID64, err := strconv.ParseUint(strings.TrimSpace(parts[0]), 10, 32)
		if err != nil {
			return nil, fmt.Errorf("invalid shard id %q", parts[0])
		}
		shardID := uint32(shardID64)

		if _, exists := shardMap[shardID]; exists {
			return nil, fmt.Errorf("duplicate shard id %d", shardID)
		}

		nodeEntries := strings.Split(parts[1], ",")
		locations := make([]api.Location, 0, len(nodeEntries))
		for _, nodeEntry := range nodeEntries {
			nodeEntry = strings.TrimSpace(nodeEntry)
			if nodeEntry == "" {
				continue
			}

			nodeParts := strings.SplitN(nodeEntry, "@", 2)
			if len(nodeParts) != 2 {
				return nil, fmt.Errorf("invalid node mapping %q (expected nodeID@host:port)", nodeEntry)
			}

			nodeID64, err := strconv.ParseUint(strings.TrimSpace(nodeParts[0]), 10, 32)
			if err != nil {
				return nil, fmt.Errorf("invalid node id %q", nodeParts[0])
			}

			address := strings.TrimSpace(nodeParts[1])
			if address == "" {
				return nil, fmt.Errorf("empty address for node %d", nodeID64)
			}

			locations = append(locations, api.Location{
				NodeID:  uint32(nodeID64),
				Address: address,
			})
		}

		if len(locations) == 0 {
			return nil, fmt.Errorf("shard %d has no nodes", shardID)
		}

		shardMap[shardID] = locations
	}

	if len(shardMap) == 0 {
		return nil, fmt.Errorf("shardMap has no valid shard entries")
	}

	return shardMap, nil
}
