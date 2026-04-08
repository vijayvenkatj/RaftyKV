package api

import (
	"encoding/json"
	"net/http"

	"github.com/vijayvenkatj/kv-store/internal/server/store"
	"github.com/vijayvenkatj/kv-store/internal/server/wal"
)

type Handler struct {
	NodeID   uint32
	Peers    []uint32
	IsLeader bool
	Store    *store.Store
}

func NewHandler(store *store.Store, config Config) *Handler {
	return &Handler{
		NodeID:   config.NodeID,
		Peers:    config.Peers,
		Store:    store,
		IsLeader: config.IsLeader,
	}
}

func (handler *Handler) GetValueHandler(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	if key == "" {
		writeJSON(w, http.StatusBadRequest, Response{Error: "missing key"})
		return
	}

	val, err := handler.Store.Get(key)
	if err != nil {
		writeJSON(w, http.StatusNotFound, Response{Error: "not found"})
		return
	}

	writeJSON(w, http.StatusOK, Response{
		Data: map[string]string{
			"key":   key,
			"value": val,
		},
	})
}

func (handler *Handler) PutValueHandler(w http.ResponseWriter, r *http.Request) {
	var req putRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Key == "" {
		writeJSON(w, http.StatusBadRequest, Response{Error: "invalid request"})
		return
	}

	if !handler.Store.IsLeader() {
		writeJSON(w, http.StatusBadRequest, Response{Error: "follower is not allowed to put value"})
		return
	}

	logEntry := &wal.LogEntry{
		Operation: "put",
		Key:       req.Key,
		Value:     req.Value,
	}
	if err := handler.Store.Apply(logEntry); err != nil {
		writeJSON(w, http.StatusInternalServerError, Response{Error: "internal error"})
		return
	}

	writeJSON(w, http.StatusOK, Response{
		Data: "ok",
	})
}

func (handler *Handler) DeleteValueHandler(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	if key == "" {
		writeJSON(w, http.StatusBadRequest, Response{Error: "missing key"})
		return
	}

	if !handler.Store.IsLeader() {
		writeJSON(w, http.StatusBadRequest, Response{Error: "follower is not allowed to delete value"})
		return
	}

	logEntry := &wal.LogEntry{
		Operation: "delete",
		Key:       key,
	}

	if err := handler.Store.Apply(logEntry); err != nil {
		writeJSON(w, http.StatusNotFound, Response{Error: "not found"})
		return
	}

	writeJSON(w, http.StatusOK, Response{
		Data: "deleted",
	})
}

func (handler *Handler) ReplicateHandler(w http.ResponseWriter, r *http.Request) {
	var request store.AppendEntriesRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		writeJSON(w, http.StatusBadRequest, store.AppendEntriesResponse{Success: false})
		return
	}

	resp := handler.Store.AppendEntries(request)
	writeJSON(w, http.StatusOK, resp)
}

func (handler *Handler) VoteHandler(w http.ResponseWriter, r *http.Request) {
	var request store.RequestVoteRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		writeJSON(w, http.StatusBadRequest, store.RequestVoteResponse{VoteGranted: false})
		return
	}

	resp := handler.Store.RequestVote(request)
	writeJSON(w, http.StatusOK, resp)
}
