package api

import (
	"errors"
	"fmt"
	"hash/fnv"
	"sort"

	"github.com/vijayvenkatj/kv-store/internal/server/store"
)

type Location struct {
	NodeID  uint32
	Address string
}
type ShardManager struct {
	LocalShards  map[uint32]*store.Store // ShardID -> Store
	GlobalShards map[uint32][]Location   // ShardID -> Location
	ShardIDs     []uint32
	LocalNodeID  uint32

	Nodes   []uint32
	NodeMap map[uint32]string // NodeID -> Address
}

func NewShardManager(shardList map[uint32][]Location, localAddr string, config Config) *ShardManager {

	var nodes []uint32
	nodeMap := make(map[uint32]string)

	// Make a list of shards to be in our Node
	var localShardList []uint32
	for shardID, list := range shardList {
		isLocalShard := false
		for _, location := range list {
			if location.NodeID == config.NodeID {
				isLocalShard = true
			}

			nodes = append(nodes, location.NodeID)
			nodeMap[location.NodeID] = location.Address
		}
		if isLocalShard {
			localShardList = append(localShardList, shardID)
		}
	}

	shardIDs := make([]uint32, 0, len(shardList))
	for shardID := range shardList {
		shardIDs = append(shardIDs, shardID)
	}
	sort.Slice(shardIDs, func(i, j int) bool { return shardIDs[i] < shardIDs[j] })

	// Create Stores for each Shard
	localShards := make(map[uint32]*store.Store)
	for _, shardID := range localShardList {

		var peers []uint32
		peerMap := make(map[uint32]string)

		for _, location := range shardList[shardID] {
			peers = append(peers, location.NodeID)
			peerMap[location.NodeID] = location.Address
		}

		storeConfig := store.Config{
			NodeID:    config.NodeID,
			ShardID:   shardID,
			ElectionT: config.ElectionT,
			Peers:     peers,
			PeerMap:   peerMap,
			Path:      fmt.Sprintf("%s/shard-%d", config.Path, shardID),
		}
		shardStore := store.New(storeConfig)
		localShards[shardID] = shardStore
	}

	return &ShardManager{
		LocalShards:  localShards,
		GlobalShards: shardList,
		ShardIDs:     shardIDs,
		LocalNodeID:  config.NodeID,

		Nodes:   nodes,
		NodeMap: nodeMap,
	}
}

func (sm *ShardManager) GetLocalShard(shardID uint32) (*store.Store, error) {
	localShard, exists := sm.LocalShards[shardID]
	if !exists {
		return nil, errors.New("shard not available locally")
	}
	return localShard, nil
}

func (sm *ShardManager) ShardLocation(shardID uint32) Location {
	shardLocations := sm.GlobalShards[shardID]
	return shardLocations[0]
}

/*
HELPERS
*/

func (sm *ShardManager) ownsShard(shardID uint32) bool {
	nodes := sm.GlobalShards[shardID]
	for _, location := range nodes {
		if location.NodeID == sm.LocalNodeID {
			return true
		}
	}
	return false
}

func hash(key string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(key))
	return h.Sum32()
}

func (sm *ShardManager) getShardID(key string) uint32 {
	if len(sm.ShardIDs) == 0 {
		return 0
	}
	idx := hash(key) % uint32(len(sm.ShardIDs))
	return sm.ShardIDs[idx]
}
