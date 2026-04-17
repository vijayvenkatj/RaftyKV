package store

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/vijayvenkatj/kv-store/internal/proto/raft"
	"github.com/vijayvenkatj/kv-store/internal/server/wal"
)

/*
AppendEntries truncates deviant data and replaces them with the leaders log. ( source of truth )
*/
func (s *Store) HandleAppendEntries(ctx context.Context, req *raft.AppendEntriesRequest) (*raft.AppendEntriesResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if req.Term < s.CurrentTerm {
		return &raft.AppendEntriesResponse{Term: s.CurrentTerm, Success: false}, nil
	} else if req.Term > s.CurrentTerm {
		s.becomeFollowerLocked(req.Term)
	}

	s.LeaderId = req.LeaderId

	// reset election timer
	select {
	case s.resetCh <- struct{}{}:
	default:
	}

	// log consistency check
	if req.PrevLogIndex > 0 {
		entry, err := s.wal.Get(req.PrevLogIndex)
		if err != nil || entry.Term != req.PrevLogTerm {
			return &raft.AppendEntriesResponse{Term: s.CurrentTerm, Success: false}, nil
		}
	}

	// truncate and overwrite
	for i, newEntryProto := range req.Entries {
		newEntry := &wal.LogEntry{
			Term:      newEntryProto.Term,
			LogIndex:  newEntryProto.LogIndex,
			Operation: newEntryProto.Operation,
			Key:       newEntryProto.Key,
			Value:     newEntryProto.Value,
		}
		idx := req.PrevLogIndex + 1 + uint32(i)

		existing, err := s.wal.Get(idx)

		if err == nil {
			if existing.Term != newEntry.Term {
				if err := s.wal.TruncateFrom(idx); err != nil {
					return &raft.AppendEntriesResponse{Term: s.CurrentTerm, Success: false}, nil
				}
				if err := s.wal.Append(newEntry); err != nil {
					return &raft.AppendEntriesResponse{Term: s.CurrentTerm, Success: false}, nil
				}
			}
		} else {
			if err := s.wal.Append(newEntry); err != nil {
				return &raft.AppendEntriesResponse{Term: s.CurrentTerm, Success: false}, nil
			}
		}
	}

	// commit update
	if req.LeaderCommit > s.CommitIndex {
		s.CommitIndex = min(req.LeaderCommit, s.wal.LastIndex)
		s.cond.Broadcast()
	}

	return &raft.AppendEntriesResponse{Term: s.CurrentTerm, Success: true}, nil
}

/*
RequestVote returns a vote if the candidate is at-least as updated as the follower.
*/
func (s *Store) HandleRequestVote(ctx context.Context, req *raft.RequestVoteRequest) (*raft.RequestVoteResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if req.Term < s.CurrentTerm {
		return &raft.RequestVoteResponse{Term: s.CurrentTerm, VoteGranted: false}, nil
	} else if req.Term > s.CurrentTerm {
		s.becomeFollowerLocked(req.Term)
	}

	if s.VotedFor != 0 && s.VotedFor != req.CandidateId {
		return &raft.RequestVoteResponse{Term: s.CurrentTerm, VoteGranted: false}, nil
	}

	lastIndex := s.wal.LastIndex
	lastTerm := uint32(0)

	if lastIndex > 0 {
		entry, _ := s.wal.Get(lastIndex)
		lastTerm = entry.Term
	}

	if req.LastLogTerm > lastTerm ||
		(req.LastLogTerm == lastTerm && req.LastLogIndex >= lastIndex) {

		s.VotedFor = req.CandidateId

		// reset timer on vote
		select {
		case s.resetCh <- struct{}{}:
		default:
		}

		return &raft.RequestVoteResponse{Term: s.CurrentTerm, VoteGranted: true}, nil
	}

	return &raft.RequestVoteResponse{Term: s.CurrentTerm, VoteGranted: false}, nil
}

/*
HELPER FUNCTIONS
*/

/*
startElection makes the current follower a Candidate, votes itself and asks others for their vote. If it reaches majority, then It becomes the leader.
*/
func (s *Store) startElection() {
	s.mu.Lock()

	s.state = Candidate
	s.CurrentTerm++
	term := s.CurrentTerm

	s.VotedFor = s.NodeID
	votes := 1

	lastIndex := s.wal.LastIndex
	lastTerm := uint32(0)

	if lastIndex > 0 {
		entry, _ := s.wal.Get(lastIndex)
		lastTerm = entry.Term
	}

	s.mu.Unlock()

	// reset timer
	select {
	case s.resetCh <- struct{}{}:
	default:
	}

	for _, follower := range s.followers {
		go func(f uint32) {
			t, granted, err := s.sendVoteRequest(f, term, lastIndex, lastTerm)

			s.mu.Lock()
			defer s.mu.Unlock()

			if s.state != Candidate || s.CurrentTerm != term {
				return
			}

			if err != nil {
				return
			}

			if t > s.CurrentTerm {
				s.becomeFollowerLocked(t)
				return
			}

			if granted {
				votes++
				if votes > len(s.followers)/2 {
					s.becomeLeaderLocked()
					return
				}
			}
		}(follower)
	}
}

func (s *Store) sendVoteRequest(follower uint32, term uint32, lastLogIndex uint32, lastLogTerm uint32) (uint32, bool, error) {

	client, ok := s.grpcClients[follower]
	if !ok {
		return 0, false, fmt.Errorf("no grpc client for follower %d", follower)
	}

	req := &raft.RequestVoteRequest{
		Term:         term,
		CandidateId:  s.NodeID,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
		ShardId:      s.ShardID,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	res, err := client.RequestVote(ctx, req)
	if err != nil {
		return 0, false, err
	}

	return res.Term, res.VoteGranted, nil
}

/*
runElectionTimer checks if the leader is being responsive. If the leader goes down (timer is not reset),then it calls for an election.
*/
func (s *Store) runElectionTimer() {
	timer := time.NewTimer(s.randomTimeout())
	defer timer.Stop()

	for {
		select {
		case <-timer.C:
			s.mu.RLock()
			isLeader := s.state == Leader
			s.mu.RUnlock()

			if !isLeader {
				s.startElection()
			}

			timer.Reset(s.randomTimeout())

		case <-s.resetCh:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(s.randomTimeout())
		}
	}
}

func (s *Store) randomTimeout() time.Duration {
	return s.ElectionT + time.Duration(rand.Intn(150))*time.Millisecond
}
