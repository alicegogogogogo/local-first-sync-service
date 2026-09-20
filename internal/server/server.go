package server

import (
	"encoding/json"
	"errors"
	"io"
	"log"
	"mime"
	"net/http"
	"strconv"

	"github.com/alicegogogogogo/local-first-sync-service/internal/store"
)

// maxBatchBytes caps an inbound POST body.
const maxBatchBytes = 10 << 20

// defaultListLimit is used when GET omits limit.
const defaultListLimit = 100

// maxListLimit is the largest page size GET accepts.
const maxListLimit = 1000

type changeIn struct {
	ID      string          `json:"id"`
	Payload json.RawMessage `json:"payload"`
}

type postRequest struct {
	DeviceID string     `json:"deviceId"`
	Changes  []changeIn `json:"changes"`
}

// NewHandler builds the public HTTP surface backed by s.
func NewHandler(s *store.Store) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})

	mux.HandleFunc("POST /v1/documents/{documentID}/changes", func(w http.ResponseWriter, r *http.Request) {
		handlePostChanges(s, w, r)
	})
	mux.HandleFunc("GET /v1/documents/{documentID}/changes", func(w http.ResponseWriter, r *http.Request) {
		handleListChanges(s, w, r)
	})

	return mux
}

// Handler exposes the HTTP surface over a private in-memory store. Use
// NewHandler with a durable store for real deployments.
func Handler() http.Handler {
	s, err := store.Open("")
	if err != nil {
		log.Fatalf("open in-memory store: %v", err)
	}
	return NewHandler(s)
}

func handlePostChanges(s *store.Store, w http.ResponseWriter, r *http.Request) {
	documentID := r.PathValue("documentID") // route pattern guarantees non-empty

	mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		writeError(w, http.StatusBadRequest, "Content-Type must be application/json")
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, maxBatchBytes)
	var req postRequest
	dec := json.NewDecoder(r.Body)
	if err := dec.Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
		return
	}
	// Reject trailing data after the JSON object.
	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		writeError(w, http.StatusBadRequest, "invalid JSON body: unexpected trailing content")
		return
	}

	if req.DeviceID == "" {
		writeError(w, http.StatusBadRequest, "deviceId must be a non-empty string")
		return
	}
	if len(req.Changes) == 0 {
		writeError(w, http.StatusBadRequest, "changes must be a non-empty array")
		return
	}

	changes := make([]store.Change, len(req.Changes))
	seen := make(map[string]struct{}, len(req.Changes))
	for i, c := range req.Changes {
		if c.ID == "" {
			writeError(w, http.StatusBadRequest, "each change must have a non-empty string id")
			return
		}
		if len(c.Payload) == 0 {
			writeError(w, http.StatusBadRequest, "each change must carry a JSON payload")
			return
		}
		if _, dup := seen[c.ID]; dup {
			writeError(w, http.StatusBadRequest, "duplicate change id within batch: "+c.ID)
			return
		}
		seen[c.ID] = struct{}{}
		changes[i] = store.Change{ID: c.ID, DeviceID: req.DeviceID, Payload: c.Payload}
	}

	results, err := s.PostChanges(documentID, changes)
	if err != nil {
		var conflict *store.ErrConflict
		if errors.As(err, &conflict) {
			writeError(w, http.StatusConflict, "change id already exists with different deviceId or payload: "+conflict.ID)
			return
		}
		writeError(w, http.StatusInternalServerError, "failed to commit changes")
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{"results": results})
}

func handleListChanges(s *store.Store, w http.ResponseWriter, r *http.Request) {
	documentID := r.PathValue("documentID")
	q := r.URL.Query()

	after := int64(0)
	if raw := q.Get("after"); raw != "" {
		v, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || v < 0 {
			writeError(w, http.StatusBadRequest, "after must be a non-negative integer")
			return
		}
		after = v
	}

	limit := int64(defaultListLimit)
	if raw := q.Get("limit"); raw != "" {
		v, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || v < 1 || v > maxListLimit {
			writeError(w, http.StatusBadRequest, "limit must be an integer between 1 and 1000")
			return
		}
		limit = v
	}

	changes, nextCursor, err := s.ListChanges(documentID, after, limit)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to list changes")
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"changes":    changes,
		"nextCursor": nextCursor,
	})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}
