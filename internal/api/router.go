package api

import (
	"net/http"
)

func NewRouter(handler *Handler) *http.ServeMux {

	router := http.NewServeMux()

	router.Handle("/api/v1/", http.StripPrefix("/api/v1", RouterV1(handler)))
	router.HandleFunc("POST /internal/replicate", handler.ReplicateHandler)
	router.HandleFunc("POST /internal/vote", handler.VoteHandler)

	return router
}

func RouterV1(handler *Handler) *http.ServeMux {
	router := http.NewServeMux()

	router.HandleFunc("GET /value", handler.GetValueHandler)
	router.HandleFunc("PUT /value", handler.PutValueHandler)
	router.HandleFunc("DELETE /value", handler.DeleteValueHandler)

	return router
}
