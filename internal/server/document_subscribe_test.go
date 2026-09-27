package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/alicegogogogogo/local-first-sync-service/internal/events"
)

// Document-level subscriptions identify the caller with the deviceId query
// parameter instead of a session; the device only has to be registered. These
// fixtures register the device directly, never creating a session, to prove
// clients that do not go through the session surface can still subscribe.

func docSubscribeURL(srv *httptest.Server, doc, cursor, device string) string {
	return srv.URL + "/v1/documents/" + doc + "/changes/subscribe?cursor=" + cursor + "&deviceId=" + device
}

func docCRDTSubscribeURL(srv *httptest.Server, doc, device string) string {
	return srv.URL + "/v1/documents/" + doc + "/crdt/state/subscribe?deviceId=" + device
}

// registerOnlyDevice registers a device without creating any session.
func registerOnlyDevice(t *testing.T, srv *httptest.Server, device string) {
	t.Helper()
	registerDeviceViaHTTP(t, srv, device)
}

// All document-level change-subscription pre-upgrade failures are ordinary
// JSON errors: no 101, no redirect and no HTML, in the fixed order
// cursor/handshake shape (400), device existence (404), permission (403).
func TestDocumentSubscribePreUpgradeValidation(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	w, _ := postJSON(t, h, "/v1/documents/doc/changes", map[string]any{
		"deviceId": "dev-1",
		"changes":  []any{map[string]any{"id": "c1", "payload": map[string]any{"n": 1}}},
	})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	w, _ = postJSON(t, h, "/v1/documents/doc2/permissions", map[string]any{"deviceId": "dev-1", "action": "revoke"})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}

	base := "/v1/documents/doc/changes/subscribe"
	cases := []struct {
		name       string
		method     string
		target     string
		upgrade    bool
		wantStatus int
	}{
		{"cursor negative", http.MethodGet, base + "?cursor=-1&deviceId=dev-1", true, http.StatusBadRequest},
		{"cursor fractional", http.MethodGet, base + "?cursor=1.5&deviceId=dev-1", true, http.StatusBadRequest},
		{"cursor non-numeric", http.MethodGet, base + "?cursor=abc&deviceId=dev-1", true, http.StatusBadRequest},
		{"cursor missing", http.MethodGet, base + "?deviceId=dev-1", true, http.StatusBadRequest},
		{"cursor scientific", http.MethodGet, base + "?cursor=1e3&deviceId=dev-1", true, http.StatusBadRequest},
		{"no upgrade headers", http.MethodGet, base + "?cursor=0&deviceId=dev-1", false, http.StatusBadRequest},
		{"wrong method", http.MethodPost, base + "?cursor=0&deviceId=dev-1", false, http.StatusBadRequest},
		{"empty document segment", http.MethodGet, "/v1/documents//changes/subscribe?cursor=0&deviceId=dev-1", true, http.StatusBadRequest},
		{"trailing slash", http.MethodGet, base + "/?cursor=0&deviceId=dev-1", true, http.StatusBadRequest},
		{"extra segment", http.MethodGet, base + "/extra?cursor=0&deviceId=dev-1", true, http.StatusBadRequest},
		{"device parameter missing", http.MethodGet, base + "?cursor=0", true, http.StatusNotFound},
		{"device parameter empty", http.MethodGet, base + "?cursor=0&deviceId=", true, http.StatusNotFound},
		{"device unregistered", http.MethodGet, base + "?cursor=0&deviceId=ghost", true, http.StatusNotFound},
		{"revoked permission", http.MethodGet, "/v1/documents/doc2/changes/subscribe?cursor=0&deviceId=dev-1", true, http.StatusForbidden},
		// Validation order: a bad cursor wins over a missing device.
		{"bad cursor before unknown device", http.MethodGet, base + "?cursor=x&deviceId=ghost", true, http.StatusBadRequest},
		// A bad handshake wins over a missing device.
		{"bad handshake before unknown device", http.MethodGet, base + "?cursor=0&deviceId=ghost", false, http.StatusBadRequest},
		// Device existence (404) wins over the permission verdict (403).
		{"unknown device before revoked permission", http.MethodGet, "/v1/documents/doc2/changes/subscribe?cursor=0&deviceId=ghost", true, http.StatusNotFound},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var r *http.Request
			if tc.upgrade {
				r = upgradeRequest(tc.target)
			} else {
				method := tc.method
				if method == "" {
					method = http.MethodGet
				}
				r = httptest.NewRequest(method, tc.target, nil)
			}
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, r)

			if rec.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d, body = %s", rec.Code, tc.wantStatus, rec.Body.String())
			}
			if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
				t.Fatalf("content-type = %q, want application/json", ct)
			}
			var body map[string]string
			if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil || body["error"] == "" {
				t.Fatalf("body = %q, want a JSON error", rec.Body.String())
			}
		})
	}
}

// A malformed document-level change-subscription handshake over a real
// connection is a 400 JSON response, never a 101.
func TestDocumentSubscribeBadHandshakeOverTCP(t *testing.T) {
	srv, _ := newWSTestServer(t)
	registerOnlyDevice(t, srv, "dev-1")
	target := "/v1/documents/doc/changes/subscribe?cursor=0&deviceId=dev-1"

	cases := []struct {
		name    string
		headers []string
	}{
		{"missing connection upgrade", []string{
			"Upgrade: websocket",
			"Sec-WebSocket-Key: " + newWSKey(),
			"Sec-WebSocket-Version: 13",
		}},
		{"wrong version", []string{
			"Connection: Upgrade",
			"Upgrade: websocket",
			"Sec-WebSocket-Key: " + newWSKey(),
			"Sec-WebSocket-Version: 8",
		}},
		{"bad key", []string{
			"Connection: Upgrade",
			"Upgrade: websocket",
			"Sec-WebSocket-Key: nope",
			"Sec-WebSocket-Version: 13",
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp, conn := dialWSRaw(t, srv.URL, target, tc.headers...)
			defer conn.Close()
			if resp.StatusCode != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400", resp.StatusCode)
			}
		})
	}

	// An undeclared device over a real connection is a 404 before the upgrade.
	conn404, resp404 := dialWS(t, srv.URL+"/v1/documents/doc/changes/subscribe?cursor=0&deviceId=ghost")
	if conn404 != nil {
		conn404.close()
		t.Fatal("a ghost device upgraded to a WebSocket")
	}
	if resp404.StatusCode != http.StatusNotFound {
		t.Fatalf("ghost device status = %d, want 404", resp404.StatusCode)
	}
}

// A registered device with no session upgrades and catches up the online log
// by cursor, then receives live commits pushed in cursor order.
func TestDocumentSubscribeCatchUpThenLive(t *testing.T) {
	srv, _ := newWSTestServer(t)
	registerOnlyDevice(t, srv, "dev-1")
	postDocChange(t, srv, "dev-1", "doc", "seed-1", map[string]any{"n": 1})
	postDocChange(t, srv, "dev-1", "doc", "seed-2", map[string]any{"n": 2})
	postDocChange(t, srv, "dev-1", "doc", "seed-3", map[string]any{"n": 3})

	conn, resp := dialWS(t, docSubscribeURL(srv, "doc", "1", "dev-1"))
	if conn == nil {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	defer conn.close()

	got := []events.ListedChange{conn.readChange(), conn.readChange()}
	if got[0].Cursor != 2 || got[0].ID != "seed-2" || got[1].Cursor != 3 || got[1].ID != "seed-3" {
		t.Fatalf("catch-up = %+v", got)
	}

	conn.setReadDeadline(300 * time.Millisecond)
	if _, _, _, ok := conn.readFrameMaybe(); ok {
		t.Fatal("a frame arrived before a live commit")
	}
	conn.clearReadDeadline()

	postDocChange(t, srv, "dev-1", "doc", "live-1", map[string]any{"n": 4})
	live := conn.readChange()
	if live.Cursor != 4 || live.ID != "live-1" || live.DeviceID != "dev-1" {
		t.Fatalf("live frame = %+v", live)
	}
}

// Pushed frames are the paginated read's record shape, and the document-level
// read and the subscription agree byte-for-byte.
func TestDocumentSubscribeFrameShapeEqualsReadShape(t *testing.T) {
	srv, _ := newWSTestServer(t)
	registerOnlyDevice(t, srv, "dev-1")
	postDocChange(t, srv, "dev-1", "doc", "c1", map[string]any{"n": 1})
	postDocChange(t, srv, "dev-1", "doc", "c2", map[string]any{"k": "v"})

	conn, resp := dialWS(t, docSubscribeURL(srv, "doc", "0", "dev-1"))
	if conn == nil {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	defer conn.close()

	status, body := httpGet(t, srv, "/v1/documents/doc/changes?limit=1000")
	if status != http.StatusOK {
		t.Fatalf("read status = %d", status)
	}
	rows := body["changes"].([]any)
	for _, rowAny := range rows {
		frame := conn.readChange()
		row := rowAny.(map[string]any)
		if frame.ID != row["id"] || frame.DeviceID != row["deviceId"] ||
			frame.Cursor != int64(row["cursor"].(float64)) {
			t.Fatalf("frame %+v != read row %v", frame, row)
		}
	}
}

// Every write path pushes to a document-level subscription immediately after
// commit; an idempotent repeat pushes nothing.
func TestDocumentSubscribeEveryWritePathPushes(t *testing.T) {
	srv, st := newWSTestServer(t)
	registerOnlyDevice(t, srv, "dev-1")
	postDocChange(t, srv, "dev-1", "doc", "seed-1", map[string]any{"n": 1})
	if _, err := st.PutSnapshot("doc", 1, json.RawMessage(`{"from":"snapshot"}`)); err != nil {
		t.Fatal(err)
	}

	conn, resp := dialWS(t, docSubscribeURL(srv, "doc", "1", "dev-1"))
	if conn == nil {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	defer conn.close()

	// Ordinary document-level commit.
	postDocChange(t, srv, "dev-1", "doc", "post-1", map[string]any{"k": "post"})
	if c := conn.readChange(); c.Cursor != 2 || c.ID != "post-1" {
		t.Fatalf("post frame = %+v", c)
	}

	// Merge.
	if code := postHTTP(t, srv, "/v1/documents/doc/merge", map[string]any{
		"deviceId":   "dev-1",
		"baseCursor": 2,
		"change":     map[string]any{"id": "merge-1", "payload": map[string]any{"m": 1}},
	}); code != http.StatusOK {
		t.Fatalf("merge = %d", code)
	}
	if c := conn.readChange(); c.Cursor != 3 || c.ID != "merge-1" {
		t.Fatalf("merge frame = %+v", c)
	}

	// Restore.
	if code := postHTTP(t, srv, "/v1/documents/doc/restore", map[string]any{
		"deviceId": "dev-1", "changeId": "restore-1", "snapshotCursor": 1,
	}); code != http.StatusOK {
		t.Fatalf("restore = %d", code)
	}
	if c := conn.readChange(); c.Cursor != 4 || c.ID != "restore-1" {
		t.Fatalf("restore frame = %+v", c)
	}

	// Replay.
	if code := postHTTP(t, srv, "/v1/documents/doc/replay", map[string]any{
		"deviceId": "dev-1",
		"operations": []any{
			map[string]any{"id": "replay-1", "payload": map[string]any{"r": 1}},
		},
	}); code != http.StatusOK {
		t.Fatalf("replay = %d", code)
	}
	if c := conn.readChange(); c.Cursor != 5 || c.ID != "replay-1" {
		t.Fatalf("replay frame = %+v", c)
	}

	// An idempotent repeat allocates no cursor and pushes nothing.
	if code := postHTTP(t, srv, "/v1/documents/doc/replay", map[string]any{
		"deviceId": "dev-1",
		"operations": []any{
			map[string]any{"id": "replay-1", "payload": map[string]any{"r": 1}},
		},
	}); code != http.StatusOK {
		t.Fatalf("idempotent replay = %d", code)
	}
	conn.setReadDeadline(300 * time.Millisecond)
	if _, _, _, ok := conn.readFrameMaybe(); ok {
		t.Fatal("an idempotent replay pushed a frame")
	}
	conn.clearReadDeadline()
}

// A revoke after the upgrade ends a document-level subscription with 4403,
// stickily; a grant revives only a fresh connection.
func TestDocumentSubscribeRevokedAfterUpgradeCloses4403(t *testing.T) {
	srv, _ := newWSTestServer(t)
	registerOnlyDevice(t, srv, "dev-1")
	postDocChange(t, srv, "dev-1", "doc", "seed-1", map[string]any{"n": 1})

	conn, resp := dialWS(t, docSubscribeURL(srv, "doc", "0", "dev-1"))
	if conn == nil {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	defer conn.close()
	if c := conn.readChange(); c.Cursor != 1 {
		t.Fatalf("seed frame = %+v", c)
	}

	if code := postHTTP(t, srv, "/v1/documents/doc/permissions", map[string]any{
		"deviceId": "dev-1", "action": "revoke",
	}); code != http.StatusOK {
		t.Fatalf("revoke = %d", code)
	}
	conn.setReadDeadline(2 * time.Second)
	if code := conn.readCloseCode(); code != 4403 {
		t.Fatalf("close code = %d, want 4403", code)
	}
	conn.clearReadDeadline()

	// Reconnect while revoked: 403 before the upgrade.
	conn2, resp2 := dialWS(t, docSubscribeURL(srv, "doc", "0", "dev-1"))
	if conn2 != nil || resp2.StatusCode != http.StatusForbidden {
		if conn2 != nil {
			conn2.close()
		}
		t.Fatalf("reconnect while revoked = %d, want 403", resp2.StatusCode)
	}

	// A grant does not revive the old connection; a fresh subscription streams.
	if code := postHTTP(t, srv, "/v1/documents/doc/permissions", map[string]any{
		"deviceId": "dev-1", "action": "grant",
	}); code != http.StatusOK {
		t.Fatalf("grant = %d", code)
	}
	conn3, resp3 := dialWS(t, docSubscribeURL(srv, "doc", "1", "dev-1"))
	if conn3 == nil {
		t.Fatalf("reconnect after grant = %d", resp3.StatusCode)
	}
	defer conn3.close()
	postDocChange(t, srv, "dev-1", "doc", "after-grant", map[string]any{"n": 2})
	if c := conn3.readChange(); c.ID != "after-grant" || c.Cursor != 2 {
		t.Fatalf("post-grant frame = %+v", c)
	}
}

// The termination signal ends a document-level subscription with 1001.
func TestDocumentSubscribeShutdownCloses1001(t *testing.T) {
	srv, st := newWSTestServer(t)
	registerOnlyDevice(t, srv, "dev-1")
	postDocChange(t, srv, "dev-1", "doc", "seed-1", map[string]any{"n": 1})

	conn, resp := dialWS(t, docSubscribeURL(srv, "doc", "1", "dev-1"))
	if conn == nil {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	defer conn.close()

	st.InterruptWaits()
	conn.setReadDeadline(2 * time.Second)
	if code := conn.readCloseCode(); code != 1001 {
		t.Fatalf("close code = %d, want 1001", code)
	}
	conn.clearReadDeadline()
}

// The document-level change connection is push-only: client data frames are
// read and discarded and never write to the log; ping gets a pong.
func TestDocumentSubscribePushOnly(t *testing.T) {
	srv, st := newWSTestServer(t)
	registerOnlyDevice(t, srv, "dev-1")
	postDocChange(t, srv, "dev-1", "doc", "seed-1", map[string]any{"n": 1})

	conn, resp := dialWS(t, docSubscribeURL(srv, "doc", "0", "dev-1"))
	if conn == nil {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	defer conn.close()
	if c := conn.readChange(); c.Cursor != 1 {
		t.Fatalf("seed = %+v", c)
	}

	conn.sendText(`{"id":"forged","payload":{"hack":true}}`)
	conn.sendPing()

	conn.setReadDeadline(time.Second)
	sawPong := false
	for {
		fin, opcode, _, ok := conn.readFrameMaybe()
		if !ok {
			break
		}
		if opcode == wsOpcodePong && fin {
			sawPong = true
		}
		if opcode == wsOpcodeText {
			t.Fatal("the server pushed after a discarded client frame")
		}
	}
	conn.clearReadDeadline()
	if !sawPong {
		t.Fatal("no pong for the ping")
	}

	postDocChange(t, srv, "dev-1", "doc", "real", map[string]any{"ok": true})
	if c := conn.readChange(); c.ID != "real" {
		t.Fatalf("frame = %+v", c)
	}
	rows, _, err := st.ListChanges("doc", 0, 1000)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range rows {
		if r.ID == "forged" {
			t.Fatal("a client frame was written to the change log")
		}
	}
}

// --- document-level CRDT state subscription ---------------------------------

// All document-level CRDT subscription pre-upgrade failures are JSON errors in
// the fixed order handshake shape (400), device existence (404), permission
// (403).
func TestDocumentCRDTSubscribePreUpgradeValidation(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	w, _ := postJSON(t, h, "/v1/documents/doc2/permissions", map[string]any{"deviceId": "dev-1", "action": "revoke"})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}

	base := "/v1/documents/doc/crdt/state/subscribe"
	cases := []struct {
		name       string
		method     string
		target     string
		upgrade    bool
		wantStatus int
	}{
		{"no upgrade headers", http.MethodGet, base + "?deviceId=dev-1", false, http.StatusBadRequest},
		{"wrong method", http.MethodPost, base + "?deviceId=dev-1", false, http.StatusBadRequest},
		{"empty document segment", http.MethodGet, "/v1/documents//crdt/state/subscribe?deviceId=dev-1", true, http.StatusBadRequest},
		{"trailing slash", http.MethodGet, base + "/?deviceId=dev-1", true, http.StatusBadRequest},
		{"extra segment", http.MethodGet, base + "/extra?deviceId=dev-1", true, http.StatusBadRequest},
		{"missing state segment", http.MethodGet, "/v1/documents/doc/crdt/subscribe?deviceId=dev-1", true, http.StatusBadRequest},
		{"extra segment past state", http.MethodGet, "/v1/documents/doc/crdt/state/extra?deviceId=dev-1", true, http.StatusBadRequest},
		{"bare crdt namespace", http.MethodGet, "/v1/documents/doc/crdt?deviceId=dev-1", true, http.StatusBadRequest},
		{"device parameter missing", http.MethodGet, base, true, http.StatusNotFound},
		{"device parameter empty", http.MethodGet, base + "?deviceId=", true, http.StatusNotFound},
		{"device unregistered", http.MethodGet, base + "?deviceId=ghost", true, http.StatusNotFound},
		{"revoked permission", http.MethodGet, "/v1/documents/doc2/crdt/state/subscribe?deviceId=dev-1", true, http.StatusForbidden},
		{"bad handshake before unknown device", http.MethodGet, base + "?deviceId=ghost", false, http.StatusBadRequest},
		{"unknown device before revoked permission", http.MethodGet, "/v1/documents/doc2/crdt/state/subscribe?deviceId=ghost", true, http.StatusNotFound},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var r *http.Request
			if tc.upgrade {
				r = upgradeRequest(tc.target)
			} else {
				method := tc.method
				if method == "" {
					method = http.MethodGet
				}
				r = httptest.NewRequest(method, tc.target, nil)
			}
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, r)

			if rec.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d, body = %s", rec.Code, tc.wantStatus, rec.Body.String())
			}
			if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
				t.Fatalf("content-type = %q, want application/json", ct)
			}
			var body map[string]string
			if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil || body["error"] == "" {
				t.Fatalf("body = %q, want a JSON error", rec.Body.String())
			}
		})
	}
}

// The first frame after the upgrade is the current merged state, byte-for-byte
// the state read endpoint's body; a document without operations stays silent
// until its first state.
func TestDocumentCRDTSubscribeInitialStateAndSilence(t *testing.T) {
	srv, _ := newWSTestServer(t)
	registerOnlyDevice(t, srv, "dev-1")

	// No CRDT operation yet: the subscription stays silent.
	conn, hs := dialWS(t, docCRDTSubscribeURL(srv, "doc", "dev-1"))
	if conn == nil {
		t.Fatalf("status = %d, want 101", hs.StatusCode)
	}
	defer conn.close()
	conn.setReadDeadline(300 * time.Millisecond)
	if _, _, _, ok := conn.readFrameMaybe(); ok {
		t.Fatal("a frame arrived before the first crdt state")
	}
	conn.clearReadDeadline()

	if code := postCRDTOps(t, srv, "doc", crdtCounterBody("dev-1", crdtCounterOp("a1", 5))); code != http.StatusOK {
		t.Fatalf("submit = %d", code)
	}
	state := conn.readCRDTState()
	if state.Type != "counter" || crdtCounterValue(t, state) != 5 {
		t.Fatalf("first state = %+v", state)
	}
	conn.close()

	// A fresh subscription's first frame equals the state read body exactly.
	stateBody := mustReadBody(t, srv, "/v1/documents/doc/crdt/state")
	conn2, hs2 := dialWS(t, docCRDTSubscribeURL(srv, "doc", "dev-1"))
	if conn2 == nil {
		t.Fatalf("re-handshake = %d", hs2.StatusCode)
	}
	defer conn2.close()
	if raw := conn2.readCRDTRaw(); string(raw) != stateBody {
		t.Fatalf("frame %q != state read body %q", raw, stateBody)
	}
}

// Only commits that truly change the merge push; idempotent repeats,
// equal-value new ops and rejected batches push nothing.
func TestDocumentCRDTSubscribePushesOnlyRealChanges(t *testing.T) {
	srv, _ := newWSTestServer(t)
	registerOnlyDevice(t, srv, "dev-1")

	conn, hs := dialWS(t, docCRDTSubscribeURL(srv, "doc", "dev-1"))
	if conn == nil {
		t.Fatalf("status = %d", hs.StatusCode)
	}
	defer conn.close()

	if code := postCRDTOps(t, srv, "doc", crdtCounterBody("dev-1", crdtCounterOp("a1", 5))); code != http.StatusOK {
		t.Fatalf("submit = %d", code)
	}
	if n := crdtCounterValue(t, conn.readCRDTState()); n != 5 {
		t.Fatalf("frame = %d, want 5", n)
	}

	// Idempotent repeat and an equal-value new op push nothing.
	if code := postCRDTOps(t, srv, "doc", crdtCounterBody("dev-1", crdtCounterOp("a1", 5))); code != http.StatusOK {
		t.Fatalf("idempotent resubmit = %d", code)
	}
	if code := postCRDTOps(t, srv, "doc", crdtCounterBody("dev-1", crdtCounterOp("a2", 5))); code != http.StatusOK {
		t.Fatalf("equal-value submit = %d", code)
	}
	// Rejected regression (409) and invalid batch (400) push nothing.
	if code := postCRDTOps(t, srv, "doc", crdtCounterBody("dev-1", crdtCounterOp("a3", 4))); code != http.StatusConflict {
		t.Fatalf("regression = %d, want 409", code)
	}
	if code := postCRDTOps(t, srv, "doc", map[string]any{"deviceId": "dev-1", "type": "counter", "ops": []any{}}); code != http.StatusBadRequest {
		t.Fatalf("empty batch = %d, want 400", code)
	}
	conn.setReadDeadline(300 * time.Millisecond)
	if _, _, _, ok := conn.readFrameMaybe(); ok {
		t.Fatal("an idempotent/equal/rejected commit pushed a frame")
	}
	conn.clearReadDeadline()

	if code := postCRDTOps(t, srv, "doc", crdtCounterBody("dev-1", crdtCounterOp("a4", 8))); code != http.StatusOK {
		t.Fatalf("advance = %d", code)
	}
	if n := crdtCounterValue(t, conn.readCRDTState()); n != 8 {
		t.Fatalf("frame = %d, want 8", n)
	}
}

// Revocation after the upgrade ends the document-level CRDT subscription with
// 4403; the termination signal ends it with 1001; inbound frames are
// discarded.
func TestDocumentCRDTSubscribeLifecycle(t *testing.T) {
	srv, st := newWSTestServer(t)
	registerOnlyDevice(t, srv, "dev-1")
	if code := postCRDTOps(t, srv, "doc", crdtCounterBody("dev-1", crdtCounterOp("a1", 1))); code != http.StatusOK {
		t.Fatalf("submit = %d", code)
	}

	conn, hs := dialWS(t, docCRDTSubscribeURL(srv, "doc", "dev-1"))
	if conn == nil {
		t.Fatalf("status = %d", hs.StatusCode)
	}
	defer conn.close()
	if n := crdtCounterValue(t, conn.readCRDTState()); n != 1 {
		t.Fatalf("initial = %d, want 1", n)
	}

	// A client data frame cannot submit an operation.
	conn.sendText(`{"deviceId":"dev-1","type":"counter","ops":[{"id":"forged","value":99}]}`)
	state, err := st.GetCRDTState("doc")
	if err != nil {
		t.Fatal(err)
	}
	if string(state.Value) != "1" {
		t.Fatalf("state = %s, want 1 (a client frame was applied)", state.Value)
	}

	if code := postHTTP(t, srv, "/v1/documents/doc/permissions", map[string]any{
		"deviceId": "dev-1", "action": "revoke",
	}); code != http.StatusOK {
		t.Fatalf("revoke = %d", code)
	}
	conn.setReadDeadline(2 * time.Second)
	if code := conn.readCloseCode(); code != 4403 {
		t.Fatalf("revoke close code = %d, want 4403", code)
	}
	conn.clearReadDeadline()

	// A second subscription on another document proves the shutdown close is
	// independent: revoke the permission back, reconnect, then interrupt.
	if code := postHTTP(t, srv, "/v1/documents/doc/permissions", map[string]any{
		"deviceId": "dev-1", "action": "grant",
	}); code != http.StatusOK {
		t.Fatalf("grant = %d", code)
	}
	conn2, hs2 := dialWS(t, docCRDTSubscribeURL(srv, "doc", "dev-1"))
	if conn2 == nil {
		t.Fatalf("reconnect = %d", hs2.StatusCode)
	}
	defer conn2.close()
	if n := crdtCounterValue(t, conn2.readCRDTState()); n != 1 {
		t.Fatalf("fresh state = %d, want 1", n)
	}

	st.InterruptWaits()
	conn2.setReadDeadline(2 * time.Second)
	if code := conn2.readCloseCode(); code != 1001 {
		t.Fatalf("shutdown close code = %d, want 1001", code)
	}
	conn2.clearReadDeadline()
}

// Device deregistration ends the device's document-level subscriptions with
// the same sticky permission close, and a reconnect fails at the door because
// the declared device no longer exists (404).
func TestDocumentSubscribeDeviceDeregisteredCloses4403(t *testing.T) {
	srv, _ := newWSTestServer(t)
	registerOnlyDevice(t, srv, "dev-1")
	postDocChange(t, srv, "dev-1", "doc", "seed-1", map[string]any{"n": 1})

	conn, resp := dialWS(t, docSubscribeURL(srv, "doc", "0", "dev-1"))
	if conn == nil {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	defer conn.close()
	if c := conn.readChange(); c.Cursor != 1 {
		t.Fatalf("seed = %+v", c)
	}

	// Deregister through the public HTTP surface: the committed cascade ends
	// the device's live subscriptions stickily, and the backing identity is
	// gone for any reconnect.
	req, err := http.NewRequest(http.MethodDelete, srv.URL+"/v1/devices/dev-1", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp2, err := srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp2.Body.Close()
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("deregister = %d", resp2.StatusCode)
	}

	conn.setReadDeadline(2 * time.Second)
	if code := conn.readCloseCode(); code != 4403 {
		t.Fatalf("close code = %d, want 4403", code)
	}
	conn.clearReadDeadline()

	// Re-handshake with the now-unknown device: 404 before the upgrade.
	conn3, resp3 := dialWS(t, docSubscribeURL(srv, "doc", "0", "dev-1"))
	if conn3 != nil || resp3.StatusCode != http.StatusNotFound {
		if conn3 != nil {
			conn3.close()
		}
		t.Fatalf("reconnect after deregister = %d, want 404", resp3.StatusCode)
	}
}
