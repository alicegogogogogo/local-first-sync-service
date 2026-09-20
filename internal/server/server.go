package server

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"log"
	"mime"
	"net/http"
	"strconv"
	"strings"

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
	mux.HandleFunc("POST /v1/documents/{documentID}/merge", func(w http.ResponseWriter, r *http.Request) {
		handleMerge(s, w, r)
	})

	return &documentGuard{h: mux}
}

// documentGuard sits in front of the mux so a request whose documentID path
// segment is empty (for example "/v1/documents//changes") is answered with a
// 400 JSON error instead of the mux's default 307/404 HTML response. No
// request is ever rewritten or redirected.
type documentGuard struct {
	h http.Handler
}

func (g *documentGuard) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if p := r.URL.Path; strings.HasPrefix(p, "/v1/documents/") {
		rest := p[len("/v1/documents/"):]
		segment := rest
		if i := strings.IndexByte(rest, '/'); i >= 0 {
			segment = rest[:i]
		}
		if segment == "" {
			writeError(w, http.StatusBadRequest, "documentID must be a non-empty string")
			return
		}
	}
	g.h.ServeHTTP(w, r)
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

type mergeRequest struct {
	DeviceID   string          `json:"deviceId"`
	BaseCursor json.RawMessage `json:"baseCursor"`
	Change     *changeIn       `json:"change"`
}

func handleMerge(s *store.Store, w http.ResponseWriter, r *http.Request) {
	documentID := r.PathValue("documentID") // guarded non-empty

	mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		writeError(w, http.StatusBadRequest, "Content-Type must be application/json")
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, maxBatchBytes)
	raw, err := io.ReadAll(r.Body)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
		return
	}

	var req mergeRequest
	dec := json.NewDecoder(bytes.NewReader(raw))
	if err := dec.Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
		return
	}
	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		writeError(w, http.StatusBadRequest, "invalid JSON body: unexpected trailing content")
		return
	}

	if req.DeviceID == "" {
		writeError(w, http.StatusBadRequest, "deviceId must be a non-empty string")
		return
	}
	baseCursor, ok := nonNegativeInteger(req.BaseCursor)
	if !ok {
		writeError(w, http.StatusBadRequest, "baseCursor must be a non-negative integer")
		return
	}
	if req.Change == nil {
		writeError(w, http.StatusBadRequest, "change must be an object")
		return
	}
	if req.Change.ID == "" {
		writeError(w, http.StatusBadRequest, "change.id must be a non-empty string")
		return
	}
	if len(req.Change.Payload) == 0 || !isJSONObject(req.Change.Payload) {
		writeError(w, http.StatusBadRequest, "change.payload must be a JSON object")
		return
	}

	result, err := s.MergeChange(documentID, req.DeviceID, baseCursor, store.Change{
		ID:       req.Change.ID,
		DeviceID: req.DeviceID,
		Payload:  req.Change.Payload,
	})
	if err != nil {
		var badCursor *store.ErrInvalidCursor
		if errors.As(err, &badCursor) {
			writeError(w, http.StatusBadRequest, badCursor.Error())
			return
		}
		var conflict *store.ErrMergeConflict
		if errors.As(err, &conflict) {
			writeError(w, http.StatusConflict, conflict.Error())
			return
		}
		writeError(w, http.StatusInternalServerError, "failed to commit merge")
		return
	}

	writeJSON(w, http.StatusOK, result)
}

// nonNegativeInteger reports whether raw encodes a JSON number that is a
// non-negative integer with no fraction or exponent. JSON strings, booleans,
// null and floats are rejected even when they numerically look like integers.
func nonNegativeInteger(raw json.RawMessage) (int64, bool) {
	if len(raw) == 0 {
		return 0, false
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	tok, err := dec.Token()
	if err != nil {
		return 0, false
	}
	num, ok := tok.(json.Number)
	if !ok {
		return 0, false
	}
	v, err := strconv.ParseInt(num.String(), 10, 64)
	if err != nil || v < 0 {
		return 0, false
	}
	return v, true
}

// isJSONObject reports whether raw encodes a JSON object ({} included). Null,
// arrays and scalars are rejected.
func isJSONObject(raw json.RawMessage) bool {
	var m map[string]json.RawMessage
	if err := json.Unmarshal(raw, &m); err != nil || m == nil {
		return false
	}
	return true
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}
