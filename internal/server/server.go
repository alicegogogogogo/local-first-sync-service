package server

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"mime"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/alicegogogogogo/local-first-sync-service/internal/app"
	"github.com/alicegogogogogo/local-first-sync-service/internal/events"
	"github.com/alicegogogogogo/local-first-sync-service/internal/store"
)

// maxBatchBytes caps an inbound POST body.
const maxBatchBytes = 10 << 20

// defaultListLimit is used when GET omits limit.
const defaultListLimit = 100

// maxListLimit is the largest page size GET accepts.
const maxListLimit = 1000

// maxPollWaitMillis is the longest waitMs a long poll accepts.
const maxPollWaitMillis = 30000

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

type permissionRequest struct {
	DeviceID string `json:"deviceId"`
	Action   string `json:"action"`
}

type sessionRequest struct {
	SessionID string `json:"sessionId"`
}

type replayRequest struct {
	DeviceID   string     `json:"deviceId"`
	Operations []changeIn `json:"operations"`
}

// NewHandlerWithReadiness builds the same public HTTP surface as NewHandler but
// answers every request — including GET /healthz — with a 503 JSON error until
// ready reports that the database is open and the listener is accepting
// connections. Once ready, responses are byte-for-byte what NewHandler emits;
// readiness is one-way, so a later flip back is not part of the contract.
func NewHandlerWithReadiness(s *app.App, ready func() bool) http.Handler {
	h := NewHandler(s)
	if ready == nil {
		return h
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !ready() {
			writeError(w, http.StatusServiceUnavailable, "service is not ready")
			return
		}
		h.ServeHTTP(w, r)
	})
}

// NewHandler builds the public HTTP surface backed by s.
func NewHandler(s *app.App) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})

	mux.HandleFunc("POST /v1/devices", func(w http.ResponseWriter, r *http.Request) {
		handleRegisterDevice(s, w, r)
	})
	// Non-POST verbs on the exact collection path: the method-less pattern is
	// less specific than POST above, so it only catches the rest and answers a
	// JSON 404 instead of ServeMux's slash redirect.
	mux.HandleFunc("/v1/devices", func(w http.ResponseWriter, _ *http.Request) {
		writeError(w, http.StatusNotFound, "unknown device path")
	})
	mux.HandleFunc("DELETE /v1/devices/{deviceId}", func(w http.ResponseWriter, r *http.Request) {
		handleDeleteDevice(s, w, r)
	})
	// The device item path accepts only DELETE; every other verb gets a JSON
	// 400 rather than ServeMux's plain-text 405.
	mux.HandleFunc("/v1/devices/{deviceId}", func(w http.ResponseWriter, _ *http.Request) {
		writeError(w, http.StatusBadRequest, "method is not allowed on this path")
	})
	// A DELETE short of the device id is a malformed deregistration and
	// answers a JSON 400 instead of the collection 404. Extra segments past
	// the device id are handled by the subtree catch-all further below (a
	// JSON 400 for DELETE outside the session subtree); the more specific
	// session and attachment delete routes still win for their exact shapes.
	mux.HandleFunc("DELETE /v1/devices", func(w http.ResponseWriter, _ *http.Request) {
		writeError(w, http.StatusBadRequest, "device delete path is malformed")
	})
	mux.HandleFunc("/v1/sessions", func(w http.ResponseWriter, _ *http.Request) {
		writeError(w, http.StatusNotFound, "unknown session path")
	})
	mux.HandleFunc("POST /v1/devices/{deviceId}/sessions", func(w http.ResponseWriter, r *http.Request) {
		handleCreateSession(s, w, r)
	})
	mux.HandleFunc("DELETE /v1/devices/{deviceId}/sessions/{sessionId}", func(w http.ResponseWriter, r *http.Request) {
		handleDeleteSession(s, w, r)
	})
	mux.HandleFunc("POST /v1/devices/{deviceId}/attachments", func(w http.ResponseWriter, r *http.Request) {
		handleCreateAttachment(s, w, r)
	})
	mux.HandleFunc("GET /v1/devices/{deviceId}/attachments", func(w http.ResponseWriter, r *http.Request) {
		handleListAttachments(s, w, r)
	})
	// The attachment collection path accepts GET (list), POST (create) and
	// DELETE (the malformed-delete 400 below); every other verb gets a JSON
	// 400 rather than ServeMux's plain-text 405.
	mux.HandleFunc("/v1/devices/{deviceId}/attachments", func(w http.ResponseWriter, _ *http.Request) {
		writeError(w, http.StatusBadRequest, "method is not allowed on this path")
	})
	// A GET with extra segments past the collection or item path is a
	// malformed list/read and answers a JSON 400 instead of the subtree 404;
	// the more specific chunks route above still wins for its exact shape.
	mux.HandleFunc("GET /v1/devices/{deviceId}/attachments/{attachmentId}/{rest...}", func(w http.ResponseWriter, _ *http.Request) {
		writeError(w, http.StatusBadRequest, "attachment list path is malformed")
	})
	mux.HandleFunc("PUT /v1/devices/{deviceId}/attachments/{attachmentId}/chunks/{index}", func(w http.ResponseWriter, r *http.Request) {
		handlePutChunk(s, w, r)
	})
	mux.HandleFunc("POST /v1/devices/{deviceId}/attachments/{attachmentId}/complete", func(w http.ResponseWriter, r *http.Request) {
		handleCompleteAttachment(s, w, r)
	})
	mux.HandleFunc("POST /v1/devices/{deviceId}/attachments/{attachmentId}/access", func(w http.ResponseWriter, r *http.Request) {
		handleSetAttachmentAccess(s, w, r)
	})
	mux.HandleFunc("GET /v1/devices/{deviceId}/attachments/{attachmentId}/access", func(w http.ResponseWriter, r *http.Request) {
		handleListAttachmentAccess(s, w, r)
	})
	// The access subresource accepts GET (roster) and POST (grant/revoke);
	// every other verb gets a JSON 400, and extra segments past it are a
	// malformed path that also answers a JSON 400. A method-less pattern
	// cannot be used here — it would conflict with the GET extra-segment
	// wildcard above, which already answers GET .../access/{extra} — so the
	// rejected verbs (including POST with a stray segment) are named
	// explicitly.
	accessBad := func(message string) http.HandlerFunc {
		return func(w http.ResponseWriter, _ *http.Request) {
			writeError(w, http.StatusBadRequest, message)
		}
	}
	for _, method := range []string{http.MethodPost, http.MethodPut, http.MethodDelete, http.MethodPatch, http.MethodOptions} {
		mux.HandleFunc(method+" /v1/devices/{deviceId}/attachments/{attachmentId}/access/{rest...}",
			accessBad("attachment access path is malformed"))
	}
	for _, method := range []string{http.MethodPut, http.MethodDelete, http.MethodPatch, http.MethodOptions} {
		mux.HandleFunc(method+" /v1/devices/{deviceId}/attachments/{attachmentId}/access",
			accessBad("method is not allowed on this path"))
	}
	mux.HandleFunc("GET /v1/devices/{deviceId}/attachments/{attachmentId}", func(w http.ResponseWriter, r *http.Request) {
		handleGetAttachment(s, w, r)
	})
	mux.HandleFunc("DELETE /v1/devices/{deviceId}/attachments/{attachmentId}", func(w http.ResponseWriter, r *http.Request) {
		handleDeleteAttachment(s, w, r)
	})
	// The attachment item path accepts only GET and DELETE; every other verb
	// gets a JSON 400 rather than ServeMux's plain-text 405. A DELETE short of
	// the attachment id or with extra segments is a malformed delete and also
	// answers a JSON 400 instead of the subtree 404.
	mux.HandleFunc("/v1/devices/{deviceId}/attachments/{attachmentId}", func(w http.ResponseWriter, _ *http.Request) {
		writeError(w, http.StatusBadRequest, "method is not allowed on this path")
	})
	mux.HandleFunc("DELETE /v1/devices/{deviceId}/attachments", func(w http.ResponseWriter, _ *http.Request) {
		writeError(w, http.StatusBadRequest, "attachment delete path is malformed")
	})
	mux.HandleFunc("DELETE /v1/devices/{deviceId}/attachments/{attachmentId}/{rest...}", func(w http.ResponseWriter, _ *http.Request) {
		writeError(w, http.StatusBadRequest, "attachment delete path is malformed")
	})
	mux.HandleFunc("GET /v1/devices/{deviceId}/attachments/{attachmentId}/chunks/{index}", func(w http.ResponseWriter, r *http.Request) {
		handleGetChunk(s, w, r)
	})
	mux.HandleFunc("GET /v1/sessions/{sessionId}/documents/{documentId}/changes", func(w http.ResponseWriter, r *http.Request) {
		handleSessionChanges(s, w, r)
	})
	mux.HandleFunc("GET /v1/sessions/{sessionId}/documents/{documentId}/changes/subscribe", func(w http.ResponseWriter, r *http.Request) {
		handleSubscribe(s, w, r)
	})
	mux.HandleFunc("GET /v1/sessions/{sessionId}/documents/{documentId}/crdt/state", func(w http.ResponseWriter, r *http.Request) {
		handleSessionCRDTState(s, w, r)
	})
	mux.HandleFunc("GET /v1/sessions/{sessionId}/documents/{documentId}/crdt/state/subscribe", func(w http.ResponseWriter, r *http.Request) {
		handleCRDTStateSubscribe(s, w, r)
	})
	// Non-GET verbs on the session CRDT paths get a JSON 400 rather than
	// ServeMux's plain-text 405: the state read and the subscription are
	// GET-only.
	mux.HandleFunc("/v1/sessions/{sessionId}/documents/{documentId}/crdt/state", func(w http.ResponseWriter, _ *http.Request) {
		writeError(w, http.StatusBadRequest, "method is not allowed on this path")
	})
	mux.HandleFunc("/v1/sessions/{sessionId}/documents/{documentId}/changes/subscribe", func(w http.ResponseWriter, _ *http.Request) {
		writeError(w, http.StatusBadRequest, "method is not allowed on this path")
	})
	mux.HandleFunc("/v1/sessions/{sessionId}/documents/{documentId}/crdt/state/subscribe", func(w http.ResponseWriter, _ *http.Request) {
		writeError(w, http.StatusBadRequest, "method is not allowed on this path")
	})
	// Any other path under the new namespaces is a JSON 404 rather than
	// ServeMux's plain-text one: every failure of a new endpoint answers JSON.
	// Exact method-patterns above take precedence over these subtree patterns.
	// A DELETE that fell through to the device catch-all is a deregistration
	// carrying extra segments past the device id — a malformed delete, which
	// the delete entries answer with a JSON 400 (matching the attachment
	// delete shapes) — unless it targets the session subtree, whose malformed
	// delete shapes keep their JSON 404.
	mux.HandleFunc("/v1/devices/{rest...}", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete {
			rest := r.PathValue("rest")
			if segs := strings.SplitN(rest, "/", 3); len(segs) >= 2 && segs[1] == "sessions" {
				writeError(w, http.StatusNotFound, "unknown device/session path")
				return
			}
			writeError(w, http.StatusBadRequest, "device delete path is malformed")
			return
		}
		writeError(w, http.StatusNotFound, "unknown device/session path")
	})
	mux.HandleFunc("/v1/sessions/{rest...}", func(w http.ResponseWriter, _ *http.Request) {
		writeError(w, http.StatusNotFound, "unknown session path")
	})

	mux.HandleFunc("POST /v1/documents/{documentID}/changes", func(w http.ResponseWriter, r *http.Request) {
		handlePostChanges(s, w, r)
	})
	mux.HandleFunc("GET /v1/documents/{documentID}/changes", func(w http.ResponseWriter, r *http.Request) {
		handleListChanges(s, w, r)
	})
	mux.HandleFunc("GET /v1/documents/{documentID}/changes/poll", func(w http.ResponseWriter, r *http.Request) {
		handlePollChanges(s, w, r)
	})
	// Non-GET verbs on the poll path: the exact GET pattern above is more
	// specific, so only other verbs reach this method-less pattern and get a
	// JSON 400 instead of ServeMux's plain-text 405.
	mux.HandleFunc("/v1/documents/{documentID}/changes/poll", func(w http.ResponseWriter, _ *http.Request) {
		writeError(w, http.StatusBadRequest, "method is not allowed on this path")
	})
	mux.HandleFunc("POST /v1/documents/{documentID}/replay", func(w http.ResponseWriter, r *http.Request) {
		handleReplay(s, w, r)
	})
	mux.HandleFunc("/v1/documents/{documentID}/replay", func(w http.ResponseWriter, _ *http.Request) {
		writeError(w, http.StatusBadRequest, "method is not allowed on this path")
	})
	mux.HandleFunc("POST /v1/documents/{documentID}/merge", func(w http.ResponseWriter, r *http.Request) {
		handleMergeChange(s, w, r)
	})
	mux.HandleFunc("POST /v1/documents/{documentID}/snapshots", func(w http.ResponseWriter, r *http.Request) {
		handlePostSnapshot(s, w, r)
	})
	mux.HandleFunc("GET /v1/documents/{documentID}/snapshots", func(w http.ResponseWriter, r *http.Request) {
		handleExportSnapshots(s, w, r)
	})
	// Non-GET verbs on the snapshot collection path: the exact GET pattern
	// above is more specific, so only other verbs reach this method-less
	// pattern and get a JSON 400 instead of ServeMux's plain-text 405.
	mux.HandleFunc("/v1/documents/{documentID}/snapshots", func(w http.ResponseWriter, _ *http.Request) {
		writeError(w, http.StatusBadRequest, "method is not allowed on this path")
	})
	mux.HandleFunc("GET /v1/documents/{documentID}/snapshots/{cursor}", func(w http.ResponseWriter, r *http.Request) {
		handleGetSnapshot(s, w, r)
	})
	// Non-GET verbs on the single-snapshot path get a JSON 400 rather than
	// ServeMux's plain-text 405: the read is GET-only.
	mux.HandleFunc("/v1/documents/{documentID}/snapshots/{cursor}", func(w http.ResponseWriter, _ *http.Request) {
		writeError(w, http.StatusBadRequest, "method is not allowed on this path")
	})
	mux.HandleFunc("POST /v1/documents/{documentID}/restore", func(w http.ResponseWriter, r *http.Request) {
		handleRestore(s, w, r)
	})
	mux.HandleFunc("POST /v1/documents/{documentID}/permissions", func(w http.ResponseWriter, r *http.Request) {
		handleSetPermission(s, w, r)
	})
	mux.HandleFunc("POST /v1/documents/{documentID}/crdt/ops", func(w http.ResponseWriter, r *http.Request) {
		handleCRDTOps(s, w, r)
	})
	mux.HandleFunc("GET /v1/documents/{documentID}/crdt/state", func(w http.ResponseWriter, r *http.Request) {
		handleCRDTState(s, w, r)
	})
	mux.HandleFunc("POST /v1/documents/{documentID}/crdt/compact", func(w http.ResponseWriter, r *http.Request) {
		handleCRDTCompact(s, w, r)
	})
	mux.HandleFunc("GET /v1/documents/{documentID}/crdt/snapshot", func(w http.ResponseWriter, r *http.Request) {
		handleCRDTSnapshot(s, w, r)
	})
	// Other verbs on the CRDT endpoints get a JSON 400 (the ops and compact
	// endpoints only accept POST; the state and snapshot endpoints only accept
	// GET) rather than ServeMux's plain-text 405.
	mux.HandleFunc("/v1/documents/{documentID}/crdt/ops", func(w http.ResponseWriter, _ *http.Request) {
		writeError(w, http.StatusBadRequest, "method is not allowed on this path")
	})
	mux.HandleFunc("/v1/documents/{documentID}/crdt/state", func(w http.ResponseWriter, _ *http.Request) {
		writeError(w, http.StatusBadRequest, "method is not allowed on this path")
	})
	mux.HandleFunc("/v1/documents/{documentID}/crdt/compact", func(w http.ResponseWriter, _ *http.Request) {
		writeError(w, http.StatusBadRequest, "method is not allowed on this path")
	})
	mux.HandleFunc("/v1/documents/{documentID}/crdt/snapshot", func(w http.ResponseWriter, _ *http.Request) {
		writeError(w, http.StatusBadRequest, "method is not allowed on this path")
	})

	// ServeMux treats any empty path segment (the doubled slash in
	// /v1/documents//..., /v1/devices//sessions or
	// /v1/sessions//documents/...) as an unclean path and answers with a 307
	// HTML redirect (a 404 in a real client). The API contract is a 400 JSON
	// error for an empty identifier, so intercept those paths before the mux
	// sees them. A trailing slash empties the final id of the device/session
	// routes and is rejected there for the same reason; the document routes
	// keep their pre-existing ServeMux behavior.
	return emptyIDGuard(mux)
}

// emptyIDGuard rejects requests carrying an empty path identifier with a 400
// JSON error instead of letting ServeMux emit its HTML redirect or a
// plain-text 404.
//
// The document family keeps its original rule (an empty documentID, i.e. the
// prefix /v1/documents//); other odd paths there retain their old response so
// the pre-existing surface is unchanged. The new device/session families are
// stricter: any doubled slash (an empty deviceId, sessionId or documentId
// segment) or a trailing slash (an empty final id) is a 400 JSON error.
func emptyIDGuard(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p := r.URL.Path

		documentSegmentEmpty := strings.HasPrefix(p, "/v1/documents//")
		newFamilySegmentEmpty := false
		if strings.HasPrefix(p, "/v1/devices/") || strings.HasPrefix(p, "/v1/sessions/") {
			newFamilySegmentEmpty = strings.Contains(p, "//") || strings.HasSuffix(p, "/")
		}

		if documentSegmentEmpty || newFamilySegmentEmpty || malformedNewDocumentPath(p) || malformedSubscribePath(p) || malformedSessionCRDTPath(p) || malformedCRDTPath(p) || malformedSnapshotPath(p) {
			writeError(w, http.StatusBadRequest, "path identifiers must be non-empty strings")
			return
		}
		next.ServeHTTP(w, r)
	})
}

// malformedNewDocumentPath reports whether p targets one of the new
// long-poll/replay endpoints but is not that endpoint's exact location: a
// missing/empty segment, a trailing slash, extra segments, or a "poll"/"replay"
// segment in a position short of the registered shape. ServeMux would answer
// those with a 301 redirect or a plain-text 404/405; the new endpoints promise
// a JSON error and never a redirect, so every such path is a malformed 400.
//
// Keywords are matched only past the documentID position, so documents that
// happen to be named "poll" or "replay" keep their ordinary merge/snapshot/
// changes routes.
func malformedNewDocumentPath(p string) bool {
	rest, ok := strings.CutPrefix(p, "/v1/documents/")
	if !ok {
		return false
	}
	segs := strings.Split(rest, "/")

	// Poll endpoint: "poll" must be exactly the third segment, after a
	// non-empty documentID and "changes".
	for i, seg := range segs {
		if seg == "poll" && i > 0 {
			return !(len(segs) == 3 && segs[0] != "" && segs[1] == "changes")
		}
	}
	// Replay endpoint: "replay" must be exactly the second segment, after a
	// non-empty documentID.
	for i, seg := range segs {
		if seg == "replay" && i > 0 {
			return !(len(segs) == 2 && segs[0] != "")
		}
	}
	return false
}

// malformedSubscribePath reports whether p targets the WebSocket subscription
// endpoint but is not at its exact location
// (/v1/sessions/{sessionId}/documents/{documentId}/changes/subscribe): a
// missing "documents"/"changes" segment, an extra segment, or a "subscribe"
// segment short of the registered shape. ServeMux would answer those with a
// plain-text 404/405; every failure of this endpoint must be a JSON 400
// instead. Empty segments are already rejected by the guard itself.
//
// "subscribe" is treated as the endpoint keyword only in the endpoint's
// terminal segment position (the fifth segment, index 4); a session or
// document identifier literally named "subscribe" occupies an identifier
// position (index 0 or 2) and is therefore left to the ordinary
// changes/subscribe routes like any other id.
func malformedSubscribePath(p string) bool {
	rest, ok := strings.CutPrefix(p, "/v1/sessions/")
	if !ok {
		return false
	}
	segs := strings.Split(rest, "/")
	// The CRDT namespace ("crdt" immediately past the document identifier)
	// has its own guard; "subscribe" there is not the changes-subscribe
	// keyword.
	if len(segs) >= 4 && segs[1] == "documents" && segs[3] == "crdt" {
		return false
	}
	for i, seg := range segs {
		if seg != "subscribe" {
			continue
		}
		// Identifier positions: sessionId (0) and documentId (2). A value of
		// "subscribe" there is an ordinary identifier, not the endpoint word.
		if i == 0 || i == 2 {
			continue
		}
		// Endpoint keyword position: the fifth segment must be exactly
		// "subscribe" with the documents/changes scaffolding around it, and no
		// segment may follow. Any other occurrence is a malformed path.
		if i == 4 && len(segs) == 5 &&
			segs[0] != "" &&
			segs[1] == "documents" &&
			segs[2] != "" &&
			segs[3] == "changes" {
			return false
		}
		return true
	}
	return false
}

// malformedSessionCRDTPath reports whether p targets the session-scoped CRDT
// namespace but is not at one of its two exact locations:
//
//	GET /v1/sessions/{sessionId}/documents/{documentId}/crdt/state
//	GET /v1/sessions/{sessionId}/documents/{documentId}/crdt/state/subscribe
//
// a missing "state"/"subscribe" segment, an extra segment, or a "crdt" segment
// in the keyword position (immediately past the document identifier) short of
// one of the registered shapes. ServeMux would answer those with a
// plain-text 404/405; every failure of these endpoints must be a JSON 400
// instead. Empty segments are already rejected by the guard itself.
//
// "crdt" is treated as the endpoint keyword only in the fourth segment (index
// 3, right after the document identifier); a session or document identifier
// literally named "crdt" occupies an identifier position (index 0 or 2) and
// keeps its ordinary routes.
func malformedSessionCRDTPath(p string) bool {
	rest, ok := strings.CutPrefix(p, "/v1/sessions/")
	if !ok {
		return false
	}
	segs := strings.Split(rest, "/")
	if len(segs) < 4 || segs[1] != "documents" || segs[3] != "crdt" {
		return false
	}
	// Keyword position reached. The state read is exactly
	// {sessionId}/documents/{documentId}/crdt/state; the subscription adds
	// one trailing "subscribe" segment.
	validState := len(segs) == 5 &&
		segs[0] != "" &&
		segs[2] != "" &&
		segs[4] == "state"
	validSubscribe := len(segs) == 6 &&
		segs[0] != "" &&
		segs[2] != "" &&
		segs[4] == "state" &&
		segs[5] == "subscribe"
	return !(validState || validSubscribe)
}

// Handler exposes the HTTP surface over a private in-memory store. Use
// NewHandler with a durable store for real deployments.
func Handler() http.Handler {
	s, err := app.Open("")
	if err != nil {
		log.Fatalf("open in-memory app: %v", err)
	}
	return NewHandler(s)
}

// decodeJSONBody enforces the strict POST contract shared by every JSON
// endpoint: Content-Type must be application/json, the body is capped at
// maxBatchBytes, it must contain exactly one JSON value, and no trailing data
// may follow it. Any violation writes a 400 JSON error and returns false; the
// caller returns immediately, so no handler reaches the store with malformed
// input and rejected requests write nothing.
func decodeJSONBody(w http.ResponseWriter, r *http.Request, dst any) bool {
	mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		writeError(w, http.StatusBadRequest, "Content-Type must be application/json")
		return false
	}

	r.Body = http.MaxBytesReader(w, r.Body, maxBatchBytes)
	dec := json.NewDecoder(r.Body)
	if err := dec.Decode(dst); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
		return false
	}
	// Reject trailing data after the JSON value.
	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		writeError(w, http.StatusBadRequest, "invalid JSON body: unexpected trailing content")
		return false
	}
	return true
}

func handleRegisterDevice(s *app.App, w http.ResponseWriter, r *http.Request) {
	var req deviceRequest
	if !decodeJSONBody(w, r, &req) {
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

// deviceDeleteResponse is the success body of the deregistration endpoint:
// the device id and the deletion marker, in that key order, and nothing else.
type deviceDeleteResponse struct {
	DeviceID string `json:"deviceId"`
	Deleted  bool   `json:"deleted"`
}

// handleDeleteDevice deregisters the device named in the path. There is no
// request body and no new authentication: the path device id is the caller's
// identity. The removal cascades through the device's sessions, document
// permissions, attachments (chunks, completion state and grants, both given
// and received) and live subscriptions in one serialized transaction that
// commits synchronously; an unknown or already removed device is a 404 JSON
// error and writes nothing, so a repeat deregistration misses the same way.
func handleDeleteDevice(s *app.App, w http.ResponseWriter, r *http.Request) {
	deviceID := r.PathValue("deviceId") // route pattern + guard guarantee non-empty

	if err := s.DeleteDevice(deviceID); err != nil {
		if errors.Is(err, store.ErrDeviceNotFound) {
			writeError(w, http.StatusNotFound, "device not found")
			return
		}
		writeError(w, http.StatusInternalServerError, "failed to delete device")
		return
	}

	writeJSON(w, http.StatusOK, deviceDeleteResponse{DeviceID: deviceID, Deleted: true})
}

func handleCreateSession(s *app.App, w http.ResponseWriter, r *http.Request) {
	deviceID := r.PathValue("deviceId") // route pattern + guard guarantee non-empty

	var req sessionRequest
	if !decodeJSONBody(w, r, &req) {
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
		case errors.As(err, &conflict):
			writeError(w, http.StatusConflict, "sessionId already belongs to another device: "+conflict.ID)
		default:
			writeError(w, http.StatusInternalServerError, "failed to create session")
		}
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{"sessionId": req.SessionID, "created": created})
}

func handleDeleteSession(s *app.App, w http.ResponseWriter, r *http.Request) {
	deviceID := r.PathValue("deviceId")   // route pattern + guard guarantee non-empty
	sessionID := r.PathValue("sessionId") // route pattern + guard guarantee non-empty

	if err := s.DeleteSession(deviceID, sessionID); err != nil {
		if errors.Is(err, store.ErrSessionNotFound) {
			writeError(w, http.StatusNotFound, "session not found")
			return
		}
		writeError(w, http.StatusInternalServerError, "failed to delete session")
		return
	}

	writeJSON(w, http.StatusOK, map[string]bool{"deleted": true})
}

func handlePostChanges(s *app.App, w http.ResponseWriter, r *http.Request) {
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

	changes := make([]events.Change, len(req.Changes))
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
		changes[i] = events.Change{ID: c.ID, DeviceID: req.DeviceID, Payload: c.Payload}
	}

	results, err := s.PostChanges(documentID, changes)
	if err != nil {
		var conflict *events.ErrConflict
		if errors.As(err, &conflict) {
			writeError(w, http.StatusConflict, "change id already exists with different deviceId or payload: "+conflict.ID)
			return
		}
		writeError(w, http.StatusInternalServerError, "failed to commit changes")
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{"results": results})
}

func handleMergeChange(s *app.App, w http.ResponseWriter, r *http.Request) {
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

	result, err := s.MergeChange(documentID, baseCursor, events.Change{
		ID:       req.Change.ID,
		DeviceID: req.DeviceID,
		Payload:  req.Change.Payload,
	})
	if err != nil {
		var conflict *events.ErrConflict
		switch {
		case errors.Is(err, events.ErrStaleCursor):
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

func handlePostSnapshot(s *app.App, w http.ResponseWriter, r *http.Request) {
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
		var conflict *events.ErrSnapshotConflict
		switch {
		case errors.Is(err, events.ErrSnapshotBase):
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

func handleGetSnapshot(s *app.App, w http.ResponseWriter, r *http.Request) {
	documentID := r.PathValue("documentID") // route pattern guarantees non-empty

	cursor, ok := parseCursorPath(r.PathValue("cursor"))
	if !ok {
		writeError(w, http.StatusBadRequest, "cursor must be a non-negative integer")
		return
	}

	state, err := s.GetSnapshot(documentID, cursor)
	if err != nil {
		if errors.Is(err, events.ErrSnapshotNotFound) {
			writeError(w, http.StatusNotFound, "snapshot not found")
			return
		}
		writeError(w, http.StatusInternalServerError, "failed to load snapshot")
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{"cursor": cursor, "state": state})
}

func handleRestore(s *app.App, w http.ResponseWriter, r *http.Request) {
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
		var conflict *events.ErrRestoreConflict
		switch {
		case errors.Is(err, events.ErrSnapshotNotFound):
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

// handleSetPermission grants or revokes a registered device's access to one
// document. Devices start authorized, so the first revoke is the first write;
// repeating the current state is idempotent (changed=false). A rejected
// request (bad content type, malformed JSON, trailing content, empty or
// mistyped fields, unknown action) is a 400 JSON error and writes nothing; an
// unregistered device is a 404 JSON error.
func handleSetPermission(s *app.App, w http.ResponseWriter, r *http.Request) {
	documentID := r.PathValue("documentID") // route pattern guarantees non-empty

	var req permissionRequest
	if !decodeJSONBody(w, r, &req) {
		return
	}
	if req.DeviceID == "" {
		writeError(w, http.StatusBadRequest, "deviceId must be a non-empty string")
		return
	}
	var authorized bool
	switch req.Action {
	case "grant":
		authorized = true
	case "revoke":
		authorized = false
	default:
		writeError(w, http.StatusBadRequest, `action must be "grant" or "revoke"`)
		return
	}

	changed, err := s.SetDocumentPermission(documentID, req.DeviceID, authorized)
	if err != nil {
		if errors.Is(err, store.ErrDeviceNotFound) {
			writeError(w, http.StatusNotFound, "device not found")
			return
		}
		writeError(w, http.StatusInternalServerError, "failed to update permission")
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"deviceId":   req.DeviceID,
		"authorized": authorized,
		"changed":    changed,
	})
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

func handleListChanges(s *app.App, w http.ResponseWriter, r *http.Request) {
	documentID := r.PathValue("documentID")

	after, limit, ok := parseChangesQuery(w, r)
	if !ok {
		return
	}

	changes, nextCursor, err := s.ListChanges(documentID, after, limit)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to list changes")
		return
	}

	writeChangesPage(w, changes, nextCursor)
}

// handleSessionChanges is the session-scoped view of the existing change log:
// both path identifiers must be non-empty (the empty-segment guard rejects
// those before routing) and the session must currently exist, after which the
// query behaves exactly like GET /v1/documents/{documentID}/changes — unless
// the session's device has been revoked permission for the document, which is
// a 403 JSON error with no changes or nextCursor. Revocation only gates this
// read; the change log itself is untouched.
func handleSessionChanges(s *app.App, w http.ResponseWriter, r *http.Request) {
	sessionID := r.PathValue("sessionId")   // route pattern + guard guarantee non-empty
	documentID := r.PathValue("documentId") // route pattern + guard guarantee non-empty

	// Malformed query parameters are a request-shape error (400) checked
	// before the resource lookup (404), matching the snapshot GET ordering.
	after, limit, ok := parseChangesQuery(w, r)
	if !ok {
		return
	}

	deviceID, err := s.SessionDevice(sessionID)
	if err != nil {
		if errors.Is(err, store.ErrSessionNotFound) {
			writeError(w, http.StatusNotFound, "session not found")
			return
		}
		writeError(w, http.StatusInternalServerError, "failed to look up session")
		return
	}

	authorized, err := s.DocumentAuthorized(documentID, deviceID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to look up permission")
		return
	}
	if !authorized {
		writeError(w, http.StatusForbidden, "device permission for this document has been revoked")
		return
	}

	changes, nextCursor, err := s.ListChanges(documentID, after, limit)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to list changes")
		return
	}

	writeChangesPage(w, changes, nextCursor)
}

// parseChangesQuery parses the shared after/limit pagination parameters,
// writing the documented 400 on an illegal value. It is shared by the
// document-scoped and session-scoped change listings so their query semantics
// cannot drift apart.
func parseChangesQuery(w http.ResponseWriter, r *http.Request) (after, limit int64, ok bool) {
	q := r.URL.Query()

	after = 0
	if raw := q.Get("after"); raw != "" {
		v, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || v < 0 {
			writeError(w, http.StatusBadRequest, "after must be a non-negative integer")
			return 0, 0, false
		}
		after = v
	}

	limit = defaultListLimit
	if raw := q.Get("limit"); raw != "" {
		v, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || v < 1 || v > maxListLimit {
			writeError(w, http.StatusBadRequest, "limit must be an integer between 1 and 1000")
			return 0, 0, false
		}
		limit = v
	}

	return after, limit, true
}

// writeChangesPage renders a change listing page in the shared response shape.
func writeChangesPage(w http.ResponseWriter, changes []events.ListedChange, nextCursor int64) {
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

// handlePollChanges is the long-polling entry over the change log.
//
// Query params share the existing change-read constraints: after is a
// non-negative cursor (default 0) and limit is 1..1000 (default 100); waitMs
// is 0..30000 (default 0). Rows already past after return immediately; a known
// document caught up to after parks until the first new commit or the wait
// deadline; an unknown document returns an empty page with cursor 0 at once.
// The response shape is the ordinary page plus a timedOut flag; a deadline
// expiry echoes the caller's cursor and never advances it. A client
// disconnect simply stops the wait and writes nothing.
func handlePollChanges(s *app.App, w http.ResponseWriter, r *http.Request) {
	documentID := r.PathValue("documentID") // route pattern + guard guarantee non-empty

	after, limit, ok := parseChangesQuery(w, r)
	if !ok {
		return
	}
	q := r.URL.Query()
	var waitMs int64
	if raw := q.Get("waitMs"); raw != "" {
		v, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || v < 0 || v > maxPollWaitMillis {
			writeError(w, http.StatusBadRequest, "waitMs must be an integer between 0 and 30000")
			return
		}
		waitMs = v
	}

	changes, nextCursor, timedOut, err := s.WaitForChanges(
		r.Context(), documentID, after, limit, time.Duration(waitMs)*time.Millisecond,
	)
	switch {
	case errors.Is(err, events.ErrStoreClosing):
		writeError(w, http.StatusServiceUnavailable, "service is shutting down")
		return
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		// The client went away (or, with waitMs bounded, its deadline lapsed
		// at the transport): nothing more to write and nothing was stored.
		return
	case err != nil:
		writeError(w, http.StatusInternalServerError, "failed to poll changes")
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"changes":    changes,
		"nextCursor": nextCursor,
		"timedOut":   timedOut,
	})
}

// handleReplay retries an offline batch against the existing change log. The
// request and per-element semantics mirror POST .../changes — strict
// application/json body, non-empty ids, no in-batch duplicates, idempotent only
// when deviceId and decoded payload match, otherwise 409 reporting the
// conflicting id — with the registration/permission layer enforced first:
// 404 for an unregistered device and 403 for a revoked one, neither exposing
// change content. The whole batch commits in one serialized transaction that
// shares the document's contiguous cursor space with ordinary commits.
func handleReplay(s *app.App, w http.ResponseWriter, r *http.Request) {
	documentID := r.PathValue("documentID") // route pattern + guard guarantee non-empty

	var req replayRequest
	if !decodeJSONBody(w, r, &req) {
		return
	}
	if req.DeviceID == "" {
		writeError(w, http.StatusBadRequest, "deviceId must be a non-empty string")
		return
	}
	if req.Operations == nil {
		writeError(w, http.StatusBadRequest, "operations must be a non-empty array")
		return
	}
	if len(req.Operations) == 0 {
		writeError(w, http.StatusBadRequest, "operations must be a non-empty array")
		return
	}

	changes := make([]events.Change, len(req.Operations))
	seen := make(map[string]struct{}, len(req.Operations))
	for i, op := range req.Operations {
		if op.ID == "" {
			writeError(w, http.StatusBadRequest, "each operation must have a non-empty string id")
			return
		}
		if len(op.Payload) == 0 {
			writeError(w, http.StatusBadRequest, "each operation must carry a JSON payload")
			return
		}
		if _, dup := seen[op.ID]; dup {
			writeError(w, http.StatusBadRequest, "duplicate operation id within batch: "+op.ID)
			return
		}
		seen[op.ID] = struct{}{}
		changes[i] = events.Change{ID: op.ID, DeviceID: req.DeviceID, Payload: op.Payload}
	}

	results, err := s.ReplayChanges(documentID, changes)
	if err != nil {
		var conflict *events.ErrConflict
		switch {
		case errors.Is(err, store.ErrDeviceNotFound):
			writeError(w, http.StatusNotFound, "device not found")
		case errors.Is(err, store.ErrPermissionDenied):
			writeError(w, http.StatusForbidden, "device permission for this document has been revoked")
		case errors.As(err, &conflict):
			writeJSON(w, http.StatusConflict, map[string]string{
				"error":      "operation id already exists with different deviceId or payload",
				"conflictId": conflict.ID,
			})
		default:
			writeError(w, http.StatusInternalServerError, "failed to replay operations")
		}
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{"results": results})
}
