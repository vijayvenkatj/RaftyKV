package store

import (
	"context"
	"log"
	"math/rand"
	"time"

	"fmt"

	"github.com/vijayvenkatj/kv-store/internal/proto/raft"
	"github.com/vijayvenkatj/kv-store/internal/server/wal"
)

/*
replicateWorker works in the background for each follower, continuously trying to replicate logs until it succeeds or the leader steps down.
It handles backtracking in case of log mismatches and implements a simple backoff strategy to avoid overwhelming followers with requests.
*/
func (s *Store) replicateWorker(follower uint32) {
	backoff := 50 * time.Millisecond

	for {
		s.mu.RLock()
		if !(s.state == Leader) {
			s.mu.RUnlock()
			return
		}
		nextIdx := s.NextIndex[follower]
		s.mu.RUnlock()

		prevLogIdx := nextIdx - 1

		var prevLogTerm uint32
		if prevLogIdx != 0 {
			e, err := s.wal.Get(prevLogIdx)
			if err != nil {
				time.Sleep(backoff)
				continue
			}
			prevLogTerm = e.Term
		}

		entries, _ := s.wal.ReadSince(nextIdx)

		s.mu.RLock()
		commitIdx := s.CommitIndex
		s.mu.RUnlock()

		term, success, err := s.sendReplication(entries, commitIdx, follower, prevLogIdx, prevLogTerm)
		if success == false || err != nil {
			log.Printf("replicateWorker failed for follower %d: %v", follower, err)
		}

		s.mu.Lock()

		if !(s.state == Leader) {
			s.mu.Unlock()
			return
		}

		if term > s.CurrentTerm {
			s.becomeLeaderLocked()
			s.mu.Unlock()
			return
		}

		if err != nil {
			s.mu.Unlock()
			time.Sleep(backoff)
			backoff = min(backoff*2, 3*time.Second) + time.Duration(rand.Intn(150))*time.Millisecond
			continue
		}

		if success {
			match := prevLogIdx + uint32(len(entries))
			s.MatchIndex[follower] = match
			s.NextIndex[follower] = match + 1

			s.updateCommitIndex()

			backoff = 50 * time.Millisecond
			s.mu.Unlock()

			if len(entries) == 0 {
				time.Sleep(50 * time.Millisecond)
			}
			continue
		}

		if s.NextIndex[follower] > 1 {
			s.NextIndex[follower]--
			s.mu.Unlock()
			continue
		}

		s.mu.Unlock()

		time.Sleep(backoff)
		backoff = min(backoff*2, time.Second) + time.Duration(rand.Intn(150))*time.Millisecond
	}
}

/*
sendReplication sends a HTTP request for AppendEntries RPC to the specified follower and returns the response.
*/
func (s *Store) sendReplication(logs []*wal.LogEntry, leaderCommit, follower, prevLogIdx, prevLogTerm uint32) (uint32, bool, error) {

	client, ok := s.grpcClients[follower]
	if !ok {
		return 0, false, fmt.Errorf("no grpc client for follower %d", follower)
	}

	var protoEntries []*raft.LogEntry
	for _, l := range logs {
		protoEntries = append(protoEntries, &raft.LogEntry{
			Term:      l.Term,
			LogIndex:  l.LogIndex,
			Operation: l.Operation,
			Key:       l.Key,
			Value:     l.Value,
		})
	}

	req := &raft.AppendEntriesRequest{
		LeaderId:     s.NodeID,
		Term:         s.CurrentTerm,
		PrevLogIndex: prevLogIdx,
		PrevLogTerm:  prevLogTerm,
		Entries:      protoEntries,
		LeaderCommit: leaderCommit,
		ShardId:      s.ShardID,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	res, err := client.AppendEntries(ctx, req)
	if err != nil {
		return 0, false, err
	}

	return res.Term, res.Success, nil
}

/*
updateCommitIndex calculates the commit index based on the match indexes of the followers and updates it if a new majority is found.
It signals the condition variable to wake up any waiting apply loops.
*/
func (s *Store) updateCommitIndex() {
	for i := s.CommitIndex + 1; i <= s.wal.LastIndex; i++ {
		count := 1
		for _, match := range s.MatchIndex {
			if match >= i {
				count++
			}
		}
		if count > len(s.followers)/2 {
			entry, err := s.wal.Get(i)
			if err == nil && entry.Term == s.CurrentTerm {
				s.CommitIndex = i
			}
		}
	}

	s.cond.Broadcast()
}
