package server

import (
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

type mergeChangeIn struct {
	ID      string          `json:"id"`
	Payload json.RawMessage `json:"payload"`
}

type mergeRequest struct {
	DeviceID   string          `json:"deviceId"`
	BaseCursor json.RawMessage `json:"baseCursor"`
	Change     *mergeChangeIn  `json:"change"`
}

type snapshotPostRequest struct {
	Cursor json.RawMessage `json:"cursor"`
	State  json.RawMessage `json:"state"`
}

type restoreRequest struct {
	DeviceID       string          `json:"deviceId"`
	ChangeID       string          `json:"changeId"`
	SnapshotCursor json.RawMessage `json:"snapshotCursor"`
}

type deviceRequest struct {
	DeviceID string `json:"deviceId"`
}

type sessionRequest struct {
	SessionID string `json:"sessionId"`
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
		handleMergeChange(s, w, r)
	})
	mux.HandleFunc("POST /v1/documents/{documentID}/snapshots", func(w http.ResponseWriter, r *http.Request) {
		handlePostSnapshot(s, w, r)
	})
	mux.HandleFunc("GET /v1/documents/{documentID}/snapshots/{cursor}", func(w http.ResponseWriter, r *http.Request) {
		handleGetSnapshot(s, w, r)
	})
	mux.HandleFunc("POST /v1/documents/{documentID}/restore", func(w http.ResponseWriter, r *http.Request) {
		handleRestore(s, w, r)
	})

	mux.HandleFunc("POST /v1/devices", func(w http.ResponseWriter, r *http.Request) {
		handleRegisterDevice(s, w, r)
	})
	mux.HandleFunc("POST /v1/devices/{deviceID}/sessions", func(w http.ResponseWriter, r *http.Request) {
		handleCreateSession(s, w, r)
	})
	mux.HandleFunc("DELETE /v1/devices/{deviceID}/sessions/{sessionID}", func(w http.ResponseWriter, r *http.Request) {
		handleDeleteSession(s, w, r)
	})
	mux.HandleFunc("GET /v1/sessions/{sessionID}/documents/{documentID}/changes", func(w http.ResponseWriter, r *http.Request) {
		handleSessionChanges(s, w, r)
	})

	// ServeMux treats any empty wildcard segment in paths like
	// /v1/documents//... or /v1/devices//sessions//... as an unclean path and
	// answers with a 307 HTML redirect (a 404 in a real client). The API
	// contract is a 400 JSON error, so intercept those paths before the mux
	// sees them.
	return emptyIDGuard(mux)
}

// emptyIDGuard rejects requests with an empty wildcard path segment with a 400
// JSON error instead of letting ServeMux emit its HTML redirect (or a plain
// text 404 for a trailing slash on an empty final segment).
func emptyIDGuard(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p := r.URL.Path
		switch {
		case strings.HasPrefix(p, "/v1/documents//"):
			writeError(w, http.StatusBadRequest, "documentID must be a non-empty string")
			return
		case strings.HasPrefix(p, "/v1/devices//"):
			// POST /v1/devices//sessions and DELETE
			// /v1/devices//sessions/{sessionID}: empty deviceId.
			writeError(w, http.StatusBadRequest, "deviceId must be a non-empty string")
			return
		case strings.HasPrefix(p, "/v1/sessions//"):
			// GET /v1/sessions//documents/{documentID}/changes: empty sessionId.
			writeError(w, http.StatusBadRequest, "sessionId must be a non-empty string")
			return
		case r.Method == http.MethodDelete && strings.HasPrefix(p, "/v1/devices/") &&
			(strings.Contains(p, "/sessions//") || strings.HasSuffix(p, "/sessions/")):
			// DELETE /v1/devices/{deviceId}/sessions/ or .../sessions//:
			// empty sessionId (ServeMux answers plain-text 404/307 otherwise).
			writeError(w, http.StatusBadRequest, "sessionId must be a non-empty string")
			return
		case strings.HasPrefix(p, "/v1/sessions/") && strings.Contains(p, "/documents//"):
			// GET /v1/sessions/{sessionId}/documents//changes: empty documentID.
			writeError(w, http.StatusBadRequest, "documentID must be a non-empty string")
			return
		}
		next.ServeHTTP(w, r)
	})
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

func handleMergeChange(s *store.Store, w http.ResponseWriter, r *http.Request) {
	documentID := r.PathValue("documentID") // route pattern guarantees non-empty

	mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		writeError(w, http.StatusBadRequest, "Content-Type must be application/json")
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, maxBatchBytes)
	var req mergeRequest
	dec := json.NewDecoder(r.Body)
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
	// baseCursor must be present and a non-negative integer (no fractions,
	// strings, booleans or null).
	if len(req.BaseCursor) == 0 {
		writeError(w, http.StatusBadRequest, "baseCursor must be a non-negative integer")
		return
	}
	baseCursor, ok := parseNonNegativeInt(req.BaseCursor)
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

	result, err := s.MergeChange(documentID, baseCursor, store.Change{
		ID:       req.Change.ID,
		DeviceID: req.DeviceID,
		Payload:  req.Change.Payload,
	})
	if err != nil {
		var conflict *store.ErrConflict
		switch {
		case errors.Is(err, store.ErrStaleCursor):
			writeError(w, http.StatusBadRequest, "baseCursor is unknown or greater than the current cursor")
			return
		case errors.As(err, &conflict):
			writeError(w, http.StatusConflict, "merge conflicts with existing changes: "+conflict.ID)
			return
		default:
			writeError(w, http.StatusInternalServerError, "failed to merge change")
			return
		}
	}

	writeJSON(w, http.StatusOK, result)
}

func handlePostSnapshot(s *store.Store, w http.ResponseWriter, r *http.Request) {
	documentID := r.PathValue("documentID") // route pattern guarantees non-empty

	mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		writeError(w, http.StatusBadRequest, "Content-Type must be application/json")
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, maxBatchBytes)
	var req snapshotPostRequest
	dec := json.NewDecoder(r.Body)
	if err := dec.Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
		return
	}
	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		writeError(w, http.StatusBadRequest, "invalid JSON body: unexpected trailing content")
		return
	}

	cursor, ok := parseNonNegativeInt(req.Cursor)
	if !ok {
		writeError(w, http.StatusBadRequest, "cursor must be a non-negative integer")
		return
	}
	if len(req.State) == 0 {
		writeError(w, http.StatusBadRequest, "state is required")
		return
	}

	created, err := s.PutSnapshot(documentID, cursor, req.State)
	if err != nil {
		var conflict *store.ErrSnapshotConflict
		switch {
		case errors.Is(err, store.ErrSnapshotBase):
			writeError(w, http.StatusBadRequest, "document is unknown or cursor is not an existing cursor")
			return
		case errors.As(err, &conflict):
			writeError(w, http.StatusConflict, "snapshot already exists with a different state")
			return
		default:
			writeError(w, http.StatusInternalServerError, "failed to store snapshot")
			return
		}
	}

	writeJSON(w, http.StatusOK, map[string]any{"cursor": cursor, "created": created})
}

func handleGetSnapshot(s *store.Store, w http.ResponseWriter, r *http.Request) {
	documentID := r.PathValue("documentID") // route pattern guarantees non-empty

	cursor, ok := parseCursorPath(r.PathValue("cursor"))
	if !ok {
		writeError(w, http.StatusBadRequest, "cursor must be a non-negative integer")
		return
	}

	state, err := s.GetSnapshot(documentID, cursor)
	if err != nil {
		if errors.Is(err, store.ErrSnapshotNotFound) {
			writeError(w, http.StatusNotFound, "snapshot not found")
			return
		}
		writeError(w, http.StatusInternalServerError, "failed to load snapshot")
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{"cursor": cursor, "state": state})
}

func handleRestore(s *store.Store, w http.ResponseWriter, r *http.Request) {
	documentID := r.PathValue("documentID") // route pattern guarantees non-empty

	mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		writeError(w, http.StatusBadRequest, "Content-Type must be application/json")
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, maxBatchBytes)
	var req restoreRequest
	dec := json.NewDecoder(r.Body)
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
	if req.ChangeID == "" {
		writeError(w, http.StatusBadRequest, "changeId must be a non-empty string")
		return
	}
	// snapshotCursor must be present and a positive integer (no fractions,
	// strings, booleans, null or zero).
	if len(req.SnapshotCursor) == 0 {
		writeError(w, http.StatusBadRequest, "snapshotCursor must be a positive integer")
		return
	}
	snapshotCursor, ok := parsePositiveInt(req.SnapshotCursor)
	if !ok {
		writeError(w, http.StatusBadRequest, "snapshotCursor must be a positive integer")
		return
	}

	result, err := s.RestoreSnapshot(documentID, req.DeviceID, req.ChangeID, snapshotCursor)
	if err != nil {
		var conflict *store.ErrRestoreConflict
		switch {
		case errors.Is(err, store.ErrSnapshotNotFound):
			writeError(w, http.StatusNotFound, "snapshot not found")
			return
		case errors.As(err, &conflict):
			writeError(w, http.StatusConflict, "change id already exists with a different deviceId, snapshotCursor or source state: "+req.ChangeID)
			return
		default:
			writeError(w, http.StatusInternalServerError, "failed to restore snapshot")
			return
		}
	}

	writeJSON(w, http.StatusOK, result)
}

func handleRegisterDevice(s *store.Store, w http.ResponseWriter, r *http.Request) {
	var req deviceRequest
	if !decodeStrictJSON(w, r, &req) {
		return
	}
	if req.DeviceID == "" {
		writeError(w, http.StatusBadRequest, "deviceId must be a non-empty string")
		return
	}

	created, err := s.RegisterDevice(req.DeviceID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to register device")
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{"deviceId": req.DeviceID, "created": created})
}

func handleCreateSession(s *store.Store, w http.ResponseWriter, r *http.Request) {
	deviceID := r.PathValue("deviceID") // route pattern guarantees non-empty

	var req sessionRequest
	if !decodeStrictJSON(w, r, &req) {
		return
	}
	if req.SessionID == "" {
		writeError(w, http.StatusBadRequest, "sessionId must be a non-empty string")
		return
	}

	created, err := s.CreateSession(deviceID, req.SessionID)
	if err != nil {
		var conflict *store.ErrSessionConflict
		switch {
		case errors.Is(err, store.ErrDeviceNotFound):
			writeError(w, http.StatusNotFound, "device not found")
			return
		case errors.As(err, &conflict):
			writeError(w, http.StatusConflict, "sessionId already belongs to another device: "+req.SessionID)
			return
		default:
			writeError(w, http.StatusInternalServerError, "failed to create session")
			return
		}
	}

	writeJSON(w, http.StatusOK, map[string]any{"sessionId": req.SessionID, "created": created})
}

func handleDeleteSession(s *store.Store, w http.ResponseWriter, r *http.Request) {
	deviceID := r.PathValue("deviceID")   // route pattern guarantees non-empty
	sessionID := r.PathValue("sessionID") // route pattern guarantees non-empty

	if err := s.DeleteSession(deviceID, sessionID); err != nil {
		if errors.Is(err, store.ErrSessionNotFound) {
			writeError(w, http.StatusNotFound, "session not found")
			return
		}
		writeError(w, http.StatusInternalServerError, "failed to delete session")
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{"deleted": true})
}

func handleSessionChanges(s *store.Store, w http.ResponseWriter, r *http.Request) {
	sessionID := r.PathValue("sessionID")   // route pattern guarantees non-empty
	documentID := r.PathValue("documentID") // route pattern guarantees non-empty

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

	// The session must currently exist; deleted and never-created sessions are
	// both 404.
	exists, err := s.SessionExists(sessionID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to look up session")
		return
	}
	if !exists {
		writeError(w, http.StatusNotFound, "session not found")
		return
	}

	// Beyond the session gate the query is the existing per-document changes
	// read, semantics unchanged.
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

// decodeStrictJSON enforces application/json, decodes exactly one JSON value
// into v and rejects any trailing content. On failure it writes a 400 JSON
// error and returns false; no caller writes after that.
func decodeStrictJSON(w http.ResponseWriter, r *http.Request, v any) bool {
	mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		writeError(w, http.StatusBadRequest, "Content-Type must be application/json")
		return false
	}

	r.Body = http.MaxBytesReader(w, r.Body, maxBatchBytes)
	dec := json.NewDecoder(r.Body)
	if err := dec.Decode(v); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
		return false
	}
	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		writeError(w, http.StatusBadRequest, "invalid JSON body: unexpected trailing content")
		return false
	}
	return true
}

// parseCursorPath reports whether raw is a decimal non-negative integer and
// returns its value. Signs, fractions and other adornments are rejected.
func parseCursorPath(raw string) (int64, bool) {
	if raw == "" {
		return 0, false
	}
	for _, c := range raw {
		if c < '0' || c > '9' {
			return 0, false
		}
	}
	n, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, false
	}
	return n, true
}

// parseNonNegativeInt reports whether raw is a JSON integer >= 0. Floats,// strings, booleans, null and negative numbers are rejected.
func parseNonNegativeInt(raw json.RawMessage) (int64, bool) {
	var n int64
	if err := json.Unmarshal(raw, &n); err != nil || n < 0 {
		return 0, false
	}
	// Guard against values like 1.0 that Unmarshal into int64 on some inputs;
	// require the literal to contain no fraction or exponent.
	trimmed := strings.TrimSpace(string(raw))
	if trimmed == "" || trimmed[0] == '-' {
		return 0, false
	}
	for _, c := range trimmed {
		if c < '0' || c > '9' {
			return 0, false
		}
	}
	return n, true
}

// parsePositiveInt reports whether raw is a JSON integer >= 1. Zero, floats,
// strings, booleans, null and negative numbers are rejected.
func parsePositiveInt(raw json.RawMessage) (int64, bool) {
	n, ok := parseNonNegativeInt(raw)
	if !ok || n < 1 {
		return 0, false
	}
	return n, true
}

// isJSONObject reports whether raw decodes to a JSON object (not null, an
// array or a scalar).
func isJSONObject(raw json.RawMessage) bool {
	var m map[string]json.RawMessage
	if err := json.Unmarshal(raw, &m); err != nil || m == nil {
		return false
	}
	return true
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
