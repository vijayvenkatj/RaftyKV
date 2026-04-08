package store

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"time"

	"github.com/vijayvenkatj/kv-store/internal/server/wal"
)

type AppendEntriesRequest struct {
	Term     uint32 `json:"term"`
	LeaderId uint32 `json:"leader_id"`

	PrevLogIndex uint32 `json:"prev_log_index"`
	PrevLogTerm  uint32 `json:"prev_log_term"`

	Entries []*wal.LogEntry `json:"entries"`

	LeaderCommit uint32 `json:"leader_commit"`
}

type AppendEntriesResponse struct {
	Term    uint32 `json:"term"`
	Success bool   `json:"success"`
}

/*
AppendEntries truncates deviant data and replaces them with the leaders log. ( source of truth )
*/
func (s *Store) AppendEntries(req AppendEntriesRequest) AppendEntriesResponse {
	s.mu.Lock()
	defer s.mu.Unlock()

	if req.Term < s.CurrentTerm {
		return AppendEntriesResponse{Term: s.CurrentTerm, Success: false}
	} else if req.Term > s.CurrentTerm {
		s.becomeFollowerLocked(req.Term)
	}

	// reset election timer
	select {
	case s.resetCh <- struct{}{}:
	default:
	}

	// log consistency check
	if req.PrevLogIndex > 0 {
		entry, err := s.wal.Get(req.PrevLogIndex)
		if err != nil || entry.Term != req.PrevLogTerm {
			return AppendEntriesResponse{Term: s.CurrentTerm, Success: false}
		}
	}

	// truncate and overwrite
	for i, newEntry := range req.Entries {
		idx := req.PrevLogIndex + 1 + uint32(i)

		existing, err := s.wal.Get(idx)

		if err == nil {
			if existing.Term != newEntry.Term {
				if err := s.wal.TruncateFrom(idx); err != nil {
					return AppendEntriesResponse{Term: s.CurrentTerm, Success: false}
				}
				if err := s.wal.Append(newEntry); err != nil {
					return AppendEntriesResponse{Term: s.CurrentTerm, Success: false}
				}
			}
		} else {
			if err := s.wal.Append(newEntry); err != nil {
				return AppendEntriesResponse{Term: s.CurrentTerm, Success: false}
			}
		}
	}

	// commit update
	if req.LeaderCommit > s.CommitIndex {
		s.CommitIndex = min(req.LeaderCommit, s.wal.LastIndex)
		s.cond.Broadcast()
	}

	return AppendEntriesResponse{Term: s.CurrentTerm, Success: true}
}

type RequestVoteRequest struct {
	Term        uint32 `json:"term"`
	CandidateID uint32 `json:"candidate_id"`

	LastLogIndex uint32 `json:"last_log_index"`
	LastLogTerm  uint32 `json:"last_log_term"`
}

type RequestVoteResponse struct {
	Term        uint32 `json:"term"`
	VoteGranted bool   `json:"vote_granted"`
}

/*
RequestVote returns a vote if the candidate is at-least as updated as the follower.
*/
func (s *Store) RequestVote(req RequestVoteRequest) RequestVoteResponse {
	s.mu.Lock()
	defer s.mu.Unlock()

	if req.Term < s.CurrentTerm {
		return RequestVoteResponse{Term: s.CurrentTerm, VoteGranted: false}
	} else if req.Term > s.CurrentTerm {
		s.becomeFollowerLocked(req.Term)
	}

	if s.VotedFor != 0 && s.VotedFor != req.CandidateID {
		return RequestVoteResponse{Term: s.CurrentTerm, VoteGranted: false}
	}

	lastIndex := s.wal.LastIndex
	lastTerm := uint32(0)

	if lastIndex > 0 {
		entry, _ := s.wal.Get(lastIndex)
		lastTerm = entry.Term
	}

	if req.LastLogTerm > lastTerm ||
		(req.LastLogTerm == lastTerm && req.LastLogIndex >= lastIndex) {

		s.VotedFor = req.CandidateID

		// reset timer on vote
		select {
		case s.resetCh <- struct{}{}:
		default:
		}

		return RequestVoteResponse{Term: s.CurrentTerm, VoteGranted: true}
	}

	return RequestVoteResponse{Term: s.CurrentTerm, VoteGranted: false}
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

	reqBody := RequestVoteRequest{
		Term:         term,
		CandidateID:  s.NodeID,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}

	data, err := json.Marshal(reqBody)
	if err != nil {
		return 0, false, err
	}

	url := "http://" + s.followerMap[follower] + "/internal/vote"

	req, err := http.NewRequest("POST", url, bytes.NewBuffer(data))
	if err != nil {
		return 0, false, err
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return 0, false, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return 0, false, fmt.Errorf("bad status")
	}

	var res RequestVoteResponse
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
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
