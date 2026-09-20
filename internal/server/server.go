package server

import (
	"encoding/json"
	"errors"
	"mime"
	"net/http"
	"strconv"
)

// Handler exposes the public HTTP surface backed by an in-memory store.
func Handler() http.Handler {
	store, err := NewStore("")
	if err != nil {
		panic(err)
	}
	return NewHandler(store)
}

// NewHandler exposes the public HTTP surface backed by the given store.
func NewHandler(store *Store) http.Handler {
	h := &handler{store: store}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", h.health)
	mux.HandleFunc("POST /v1/documents/{documentID}/changes", h.postChanges)
	mux.HandleFunc("GET /v1/documents/{documentID}/changes", h.getChanges)
	return mux
}

type handler struct {
	store *Store
}

func (h *handler) health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

type postRequest struct {
	DeviceID string `json:"deviceId"`
	Changes  []struct {
		ID      string          `json:"id"`
		Payload json.RawMessage `json:"payload"`
	} `json:"changes"`
}

type postResponse struct {
	Results []ApplyResult `json:"results"`
}

func (h *handler) postChanges(w http.ResponseWriter, r *http.Request) {
	documentID := r.PathValue("documentID")

	mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		writeError(w, http.StatusBadRequest, "Content-Type must be application/json")
		return
	}

	var body postRequest
	dec := json.NewDecoder(r.Body)
	if err := dec.Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "request body must be a valid JSON object")
		return
	}
	if dec.More() {
		writeError(w, http.StatusBadRequest, "request body must contain a single JSON value")
		return
	}
	if documentID == "" {
		writeError(w, http.StatusBadRequest, "documentID must be a non-empty string")
		return
	}
	if body.DeviceID == "" {
		writeError(w, http.StatusBadRequest, "deviceId must be a non-empty string")
		return
	}
	if len(body.Changes) == 0 {
		writeError(w, http.StatusBadRequest, "changes must be a non-empty array")
		return
	}

	seen := make(map[string]struct{}, len(body.Changes))
	items := make([]ChangeInput, len(body.Changes))
	for i, ch := range body.Changes {
		if ch.ID == "" {
			writeError(w, http.StatusBadRequest, "change id must be a non-empty string")
			return
		}
		if ch.Payload == nil {
			writeError(w, http.StatusBadRequest, "change payload is required")
			return
		}
		if _, dup := seen[ch.ID]; dup {
			writeError(w, http.StatusBadRequest, "duplicate change id in batch: "+ch.ID)
			return
		}
		seen[ch.ID] = struct{}{}
		items[i] = ChangeInput{ID: ch.ID, Payload: ch.Payload}
	}

	results, err := h.store.Apply(documentID, body.DeviceID, items)
	if errors.Is(err, ErrConflict) {
		writeError(w, http.StatusConflict, "change conflicts with an existing record")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to persist changes")
		return
	}
	writeJSON(w, http.StatusOK, postResponse{Results: results})
}

type getResponse struct {
	Changes    []Change `json:"changes"`
	NextCursor int64    `json:"nextCursor"`
}

func (h *handler) getChanges(w http.ResponseWriter, r *http.Request) {
	documentID := r.PathValue("documentID")
	query := r.URL.Query()

	after, err := parseIntParam(query.Get("after"), 0)
	if err != nil || after < 0 {
		writeError(w, http.StatusBadRequest, "after must be a non-negative integer")
		return
	}
	limit, err := parseIntParam(query.Get("limit"), 100)
	if err != nil || limit < 1 || limit > 1000 {
		writeError(w, http.StatusBadRequest, "limit must be an integer between 1 and 1000")
		return
	}

	changes, next := h.store.List(documentID, after, int(limit))
	writeJSON(w, http.StatusOK, getResponse{Changes: changes, NextCursor: next})
}

func parseIntParam(raw string, fallback int64) (int64, error) {
	if raw == "" {
		return fallback, nil
	}
	return strconv.ParseInt(raw, 10, 64)
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}
