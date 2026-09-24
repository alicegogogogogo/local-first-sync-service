package server

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/alicegogogogogo/local-first-sync-service/internal/store"
)

// upgradeRequest builds a GET with a valid WebSocket handshake for the
// in-process handler; negative tests strip or alter individual headers.
func upgradeRequest(target string) *http.Request {
	r := httptest.NewRequest(http.MethodGet, target, nil)
	r.Header.Set("Connection", "Upgrade")
	r.Header.Set("Upgrade", "websocket")
	r.Header.Set("Sec-WebSocket-Key", newWSKey())
	r.Header.Set("Sec-WebSocket-Version", "13")
	return r
}

// All pre-upgrade failures are ordinary JSON errors: no 101, no redirect and
// no HTML, and nothing about the session or document is leaked.
func TestSubscribePreUpgradeValidation(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	w, _ := postJSON(t, h, "/v1/devices/dev-1/sessions", map[string]any{"sessionId": "sess"})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	// Seed a change so permission revocation is meaningful.
	w, _ = postJSON(t, h, "/v1/documents/doc/changes", map[string]any{
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

	base := "/v1/sessions/sess/documents/doc/changes/subscribe"
	cases := []struct {
		name       string
		method     string
		target     string
		upgrade    bool
		wantStatus int
	}{
		{"cursor negative", http.MethodGet, base + "?cursor=-1", true, http.StatusBadRequest},
		{"cursor fractional", http.MethodGet, base + "?cursor=1.5", true, http.StatusBadRequest},
		{"cursor non-numeric", http.MethodGet, base + "?cursor=abc", true, http.StatusBadRequest},
		{"cursor missing", http.MethodGet, base, true, http.StatusBadRequest},
		{"cursor scientific", http.MethodGet, base + "?cursor=1e3", true, http.StatusBadRequest},
		{"no upgrade headers", http.MethodGet, base + "?cursor=0", false, http.StatusBadRequest},
		{"wrong method", http.MethodPost, base + "?cursor=0", false, http.StatusBadRequest},
		{"empty session segment", http.MethodGet, "/v1/sessions//documents/doc/changes/subscribe?cursor=0", true, http.StatusBadRequest},
		{"empty document segment", http.MethodGet, "/v1/sessions/sess/documents//changes/subscribe?cursor=0", true, http.StatusBadRequest},
		{"trailing slash", http.MethodGet, base + "/?cursor=0", true, http.StatusBadRequest},
		{"extra segment", http.MethodGet, base + "/extra?cursor=0", true, http.StatusBadRequest},
		{"missing documents segment", http.MethodGet, "/v1/sessions/sess/doc/changes/subscribe?cursor=0", true, http.StatusBadRequest},
		{"unknown session", http.MethodGet, "/v1/sessions/ghost/documents/doc/changes/subscribe?cursor=0", true, http.StatusNotFound},
		{"revoked permission", http.MethodGet, "/v1/sessions/sess/documents/doc2/changes/subscribe?cursor=0", true, http.StatusForbidden},
		// Validation order: a bad cursor wins over a missing session.
		{"bad cursor before unknown session", http.MethodGet, "/v1/sessions/ghost/documents/doc/changes/subscribe?cursor=x", true, http.StatusBadRequest},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var r *http.Request
			switch tc.name {
			case "no upgrade headers", "wrong method":
				method := http.MethodGet
				if tc.method != "" {
					method = tc.method
				}
				r = httptest.NewRequest(method, tc.target, nil)
			default:
				r = upgradeRequest(tc.target)
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

// A malformed handshake over a real connection (bad key, wrong version,
// missing upgrade token) is a 400 JSON response, never a 101.
func TestSubscribeBadHandshakeOverTCP(t *testing.T) {
	srv, _ := newWSTestServer(t)
	setupSession(t, srv, "dev-1", "sess", "doc", 0)
	target := "/v1/sessions/sess/documents/doc/changes/subscribe?cursor=0"

	cases := []struct {
		name    string
		headers []string
	}{
		{"missing connection upgrade", []string{
			"Upgrade: websocket",
			"Sec-WebSocket-Key: " + newWSKey(),
			"Sec-WebSocket-Version: 13",
		}},
		{"missing upgrade header", []string{
			"Connection: Upgrade",
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
			"Sec-WebSocket-Key: not-base64-16-bytes",
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
			if ct := resp.Header.Get("Content-Type"); ct != "application/json" {
				t.Fatalf("content-type = %q", ct)
			}
			data, _ := io.ReadAll(resp.Body)
			if !strings.Contains(string(data), `"error"`) {
				t.Fatalf("body = %q, want JSON error", data)
			}
		})
	}
}

// A valid handshake upgrades and the accept digest matches RFC 6455.
func TestSubscribeHandshakeSucceeds(t *testing.T) {
	srv, _ := newWSTestServer(t)
	setupSession(t, srv, "dev-1", "sess", "doc", 1)

	conn, resp := dialWS(t, subscribeURL(srv, "sess", "doc", "0"))
	if conn == nil {
		t.Fatalf("status = %d, want 101", resp.StatusCode)
	}
	defer conn.close()
	if !strings.Contains(strings.ToLower(resp.Header.Get("Connection")), "upgrade") ||
		!strings.EqualFold(resp.Header.Get("Upgrade"), "websocket") {
		t.Fatalf("upgrade headers = %v", resp.Header)
	}

	// The seeded change is the first frame, proving the stream started.
	ch := conn.readChange()
	if ch.Cursor != 1 || ch.ID != "seed-1" || ch.DeviceID != "dev-1" {
		t.Fatalf("first frame = %+v", ch)
	}
}

// Changes already past the cursor are replayed first in cursor order; new
// commits then continue the stream seamlessly.
func TestSubscribeCatchUpThenLive(t *testing.T) {
	srv, _ := newWSTestServer(t)
	setupSession(t, srv, "dev-1", "sess", "doc", 3)

	conn, resp := dialWS(t, subscribeURL(srv, "sess", "doc", "1"))
	if conn == nil {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	defer conn.close()

	got := []store.ListedChange{conn.readChange(), conn.readChange()}
	if got[0].Cursor != 2 || got[0].ID != "seed-2" || got[1].Cursor != 3 || got[1].ID != "seed-3" {
		t.Fatalf("catch-up = %+v", got)
	}
	for _, c := range got {
		if c.DeviceID != "dev-1" {
			t.Fatalf("deviceId = %q", c.DeviceID)
		}
		if n := c.Payload; string(n) == "" {
			t.Fatal("payload missing")
		}
	}

	// Caught up: no frame until a new commit lands.
	conn.setReadDeadline(300 * time.Millisecond)
	if _, _, _, ok := conn.readFrameMaybe(); ok {
		t.Fatal("a frame arrived before any commit")
	}
	conn.clearReadDeadline()

	postDocChange(t, srv, "dev-1", "doc", "live-1", map[string]any{"n": 4})
	live := conn.readChange()
	if live.Cursor != 4 || live.ID != "live-1" || live.DeviceID != "dev-1" {
		t.Fatalf("live frame = %+v", live)
	}
}

// A subscription from cursor 0 on a document with no rows parks until the
// first commit, then receives cursor 1.
func TestSubscribeUnknownDocumentParksThenStreams(t *testing.T) {
	srv, _ := newWSTestServer(t)
	setupSession(t, srv, "dev-1", "sess", "doc", 0)

	conn, resp := dialWS(t, subscribeURL(srv, "sess", "doc", "0"))
	if conn == nil {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	defer conn.close()

	conn.setReadDeadline(300 * time.Millisecond)
	if _, _, _, ok := conn.readFrameMaybe(); ok {
		t.Fatal("unknown document produced a frame")
	}
	conn.clearReadDeadline()

	postDocChange(t, srv, "dev-1", "doc", "first", map[string]any{"v": 1})
	first := conn.readChange()
	if first.Cursor != 1 || first.ID != "first" {
		t.Fatalf("first frame = %+v", first)
	}
}

// A reconnect with any cursor resumes exactly there and receives the same
// rows a paginated read would, in the same order and shape.
func TestSubscribeResumeMatchesPagedRead(t *testing.T) {
	srv, _ := newWSTestServer(t)
	setupSession(t, srv, "dev-1", "sess", "doc", 5)

	conn, resp := dialWS(t, subscribeURL(srv, "sess", "doc", "2"))
	if conn == nil {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	// Disconnect after the catch-up; reconnect from a later cursor.
	frame := conn.readChange()
	conn.sendClose(1000)
	conn.close()
	if frame.Cursor != 3 {
		t.Fatalf("first resumed frame cursor = %d, want 3", frame.Cursor)
	}

	conn2, resp2 := dialWS(t, subscribeURL(srv, "sess", "doc", "3"))
	if conn2 == nil {
		t.Fatalf("reconnect status = %d", resp2.StatusCode)
	}
	defer conn2.close()
	pushed := conn2.readChange()
	if pushed.Cursor != 4 || pushed.ID != "seed-4" {
		t.Fatalf("reconnected frame = %+v", pushed)
	}

	// The pushed frame and the equivalent read row are the same shape.
	status, body := httpGet(t, srv, "/v1/sessions/sess/documents/doc/changes?after=3&limit=1")
	if status != http.StatusOK {
		t.Fatalf("read status = %d", status)
	}
	row := body["changes"].([]any)[0].(map[string]any)
	if row["id"] != pushed.ID || row["deviceId"] != pushed.DeviceID ||
		int64(row["cursor"].(float64)) != pushed.Cursor {
		t.Fatalf("pushed %+v != read row %v", pushed, row)
	}
}

// Frames and the paginated read are byte-for-byte the same records in the same
// order.
func TestSubscribeFrameShapeEqualsReadShape(t *testing.T) {
	h, st := newTestHandler(t)
	srv := httptest.NewServer(h)
	defer srv.Close()
	setupSession(t, srv, "dev-1", "sess", "doc", 3)

	conn, resp := dialWS(t, subscribeURL(srv, "sess", "doc", "0"))
	if conn == nil {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	defer conn.close()

	w, body := doRequest(t, h, http.MethodGet, "/v1/documents/doc/changes?limit=1000")
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	rows := body["changes"].([]any)
	for i, rowAny := range rows {
		frame := conn.readChange()
		row := rowAny.(map[string]any)
		if frame.ID != row["id"] || frame.DeviceID != row["deviceId"] ||
			frame.Cursor != int64(row["cursor"].(float64)) {
			t.Fatalf("frame %d = %+v, row = %v", i, frame, row)
		}
		var framePayload, rowPayload any
		_ = json.Unmarshal(frame.Payload, &framePayload)
		_ = json.Unmarshal(mustJSON(row["payload"]), &rowPayload)
		if !deepEqualAny(framePayload, rowPayload) {
			t.Fatalf("payload %d: frame %v != row %v", i, framePayload, rowPayload)
		}
	}
	_ = st
}

// All four write paths push immediately after commit.
func TestSubscribeLivePushesFromEveryWritePath(t *testing.T) {
	srv, st := newWSTestServer(t)
	setupSession(t, srv, "dev-1", "sess", "doc", 1)
	// Snapshot at cursor 1 enables restore.
	if _, err := st.PutSnapshot("doc", 1, json.RawMessage(`{"from":"snapshot"}`)); err != nil {
		t.Fatal(err)
	}

	conn, resp := dialWS(t, subscribeURL(srv, "sess", "doc", "1"))
	if conn == nil {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	defer conn.close()

	// Ordinary commit.
	postDocChange(t, srv, "dev-1", "doc", "post-1", map[string]any{"k": "post"})
	if c := conn.readChange(); c.Cursor != 2 || c.ID != "post-1" {
		t.Fatalf("post frame = %+v", c)
	}

	// Merge.
	mergeResp := postHTTP(t, srv, "/v1/documents/doc/merge", map[string]any{
		"deviceId":   "dev-1",
		"baseCursor": 2,
		"change":     map[string]any{"id": "merge-1", "payload": map[string]any{"m": 1}},
	})
	if mergeResp != http.StatusOK {
		t.Fatalf("merge status = %d", mergeResp)
	}
	if c := conn.readChange(); c.Cursor != 3 || c.ID != "merge-1" {
		t.Fatalf("merge frame = %+v", c)
	}

	// Restore.
	if code := postHTTP(t, srv, "/v1/documents/doc/restore", map[string]any{
		"deviceId":       "dev-1",
		"changeId":       "restore-1",
		"snapshotCursor": 1,
	}); code != http.StatusOK {
		t.Fatalf("restore status = %d", code)
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
		t.Fatalf("replay status = %d", code)
	}
	if c := conn.readChange(); c.Cursor != 5 || c.ID != "replay-1" {
		t.Fatalf("replay frame = %+v", c)
	}
}

// Revocation after the upgrade ends the subscription with 4403, and no later
// change is delivered on the dead connection.
func TestSubscribeRevokedAfterUpgradeCloses4403(t *testing.T) {
	srv, _ := newWSTestServer(t)
	setupSession(t, srv, "dev-1", "sess", "doc", 1)

	conn, resp := dialWS(t, subscribeURL(srv, "sess", "doc", "0"))
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
		t.Fatalf("revoke status = %d", code)
	}

	conn.setReadDeadline(2 * time.Second)
	code := conn.readCloseCode()
	conn.clearReadDeadline()
	if code != 4403 {
		t.Fatalf("close code = %d, want 4403", code)
	}

	// A reconnect while revoked is a pre-upgrade 403; nothing arrives on the
	// old connection after a later commit.
	postDocChange(t, srv, "dev-1", "doc", "after-revoke", map[string]any{"x": 1})
	conn.setReadDeadline(300 * time.Millisecond)
	if _, _, _, ok := conn.readFrameMaybe(); ok {
		t.Fatal("a frame arrived after the 4403 close")
	}
	conn.clearReadDeadline()

	conn2, resp2 := dialWS(t, subscribeURL(srv, "sess", "doc", "0"))
	if conn2 != nil || resp2.StatusCode != http.StatusForbidden {
		if conn2 != nil {
			conn2.close()
		}
		t.Fatalf("reconnect while revoked = %d, want 403", resp2.StatusCode)
	}

	// Grant restores access; a fresh subscription streams again.
	if code := postHTTP(t, srv, "/v1/documents/doc/permissions", map[string]any{
		"deviceId": "dev-1", "action": "grant",
	}); code != http.StatusOK {
		t.Fatalf("grant status = %d", code)
	}
	conn3, resp3 := dialWS(t, subscribeURL(srv, "sess", "doc", "1"))
	if conn3 == nil {
		t.Fatalf("reconnect after grant = %d", resp3.StatusCode)
	}
	defer conn3.close()
	if c := conn3.readChange(); c.ID != "after-revoke" || c.Cursor != 2 {
		t.Fatalf("post-grant frame = %+v, want after-revoke/2", c)
	}
}

// Revocation is sticky on a live connection: even if a grant commits before
// the server reads the closing frame, the subscription still ends with 4403.
func TestSubscribeRevokeStickyDespiteGrant(t *testing.T) {
	srv, _ := newWSTestServer(t)
	setupSession(t, srv, "dev-1", "sess", "doc", 1)

	conn, resp := dialWS(t, subscribeURL(srv, "sess", "doc", "0"))
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
	// Immediately re-grant before draining the close frame.
	if code := postHTTP(t, srv, "/v1/documents/doc/permissions", map[string]any{
		"deviceId": "dev-1", "action": "grant",
	}); code != http.StatusOK {
		t.Fatalf("grant = %d", code)
	}

	conn.setReadDeadline(2 * time.Second)
	if code := conn.readCloseCode(); code != 4403 {
		t.Fatalf("close code = %d, want sticky 4403 despite the grant", code)
	}
	conn.clearReadDeadline()
}

// The termination signal ends every subscription with 1001.
func TestSubscribeShutdownCloses1001(t *testing.T) {
	srv, st := newWSTestServer(t)
	setupSession(t, srv, "dev-1", "sess", "doc", 1)

	conn, resp := dialWS(t, subscribeURL(srv, "sess", "doc", "1"))
	if conn == nil {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	defer conn.close()

	st.InterruptWaits()
	conn.setReadDeadline(2 * time.Second)
	code := conn.readCloseCode()
	conn.clearReadDeadline()
	if code != 1001 {
		t.Fatalf("close code = %d, want 1001", code)
	}

	// Committed data and cursors stay readable after the signal.
	rows, next, err := st.ListChanges("doc", 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || next != 1 {
		t.Fatalf("committed state after signal = %+v/%d", rows, next)
	}
}

// A client disconnect releases the subscription: later commits proceed, the
// store closes without waiting on the gone client, and a fresh subscription
// works.
func TestSubscribeClientDisconnectReleases(t *testing.T) {
	srv, st := newWSTestServer(t)
	setupSession(t, srv, "dev-1", "sess", "doc", 1)

	conn, resp := dialWS(t, subscribeURL(srv, "sess", "doc", "0"))
	if conn == nil {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	if c := conn.readChange(); c.Cursor != 1 {
		t.Fatalf("seed frame = %+v", c)
	}
	conn.close()

	// Give the read pump a moment to observe the EOF and unregister.
	time.Sleep(100 * time.Millisecond)

	if _, err := st.PostChanges("doc", []store.Change{
		{ID: "later", DeviceID: "dev-1", Payload: json.RawMessage(`{"n":2}`)},
	}); err != nil {
		t.Fatalf("commit after disconnect: %v", err)
	}

	// A new subscription from the same cursor catches the later change.
	conn2, resp2 := dialWS(t, subscribeURL(srv, "sess", "doc", "1"))
	if conn2 == nil {
		t.Fatalf("reconnect = %d", resp2.StatusCode)
	}
	defer conn2.close()
	if c := conn2.readChange(); c.ID != "later" || c.Cursor != 2 {
		t.Fatalf("frame after reconnect = %+v", c)
	}
}

// The connection is push-only: inbound data frames are accepted and ignored,
// never echoed and never written to the log; live pushes continue.
func TestSubscribePushOnly(t *testing.T) {
	srv, st := newWSTestServer(t)
	setupSession(t, srv, "dev-1", "sess", "doc", 1)

	conn, resp := dialWS(t, subscribeURL(srv, "sess", "doc", "0"))
	if conn == nil {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	defer conn.close()
	if c := conn.readChange(); c.Cursor != 1 {
		t.Fatalf("seed = %+v", c)
	}

	conn.sendText(`{"id":"forged","payload":{"hack":true}}`)

	// No echo frame arrives before the next real commit.
	conn.setReadDeadline(300 * time.Millisecond)
	if _, _, _, ok := conn.readFrameMaybe(); ok {
		t.Fatal("the server echoed a client frame")
	}
	conn.clearReadDeadline()

	postDocChange(t, srv, "dev-1", "doc", "real", map[string]any{"ok": true})
	if c := conn.readChange(); c.ID != "real" {
		t.Fatalf("frame = %+v", c)
	}

	// The forged id never reached the log: posting it now allocates a cursor.
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

// A client ping is answered with a pong and does not disturb the stream.
func TestSubscribePingPong(t *testing.T) {
	srv, _ := newWSTestServer(t)
	setupSession(t, srv, "dev-1", "sess", "doc", 1)

	conn, resp := dialWS(t, subscribeURL(srv, "sess", "doc", "0"))
	if conn == nil {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	defer conn.close()
	_ = conn.readChange()

	conn.sendPing()
	conn.setReadDeadline(2 * time.Second)
	sawPong := false
	for {
		fin, opcode, _, ok := conn.readFrameMaybe()
		if !ok {
			break
		}
		if opcode == wsOpcodePong && fin {
			sawPong = true
			break
		}
	}
	conn.clearReadDeadline()
	if !sawPong {
		t.Fatal("no pong received for the ping")
	}

	postDocChange(t, srv, "dev-1", "doc", "after-ping", map[string]any{"n": 2})
	if c := conn.readChange(); c.ID != "after-ping" {
		t.Fatalf("frame after ping = %+v", c)
	}
}

// An unmasked client frame violates RFC 6455 and ends the connection with the
// protocol-error close code 1002.
func TestSubscribeUnmaskedFrameCloses1002(t *testing.T) {
	srv, _ := newWSTestServer(t)
	setupSession(t, srv, "dev-1", "sess", "doc", 1)

	conn, resp := dialWS(t, subscribeURL(srv, "sess", "doc", "0"))
	if conn == nil {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	defer conn.close()
	_ = conn.readChange()

	// One unmasked single-byte text frame: FIN text, len 1, mask bit clear.
	if _, err := conn.conn.Write([]byte{0x81, 0x01, 'x'}); err != nil {
		t.Fatal(err)
	}
	conn.setReadDeadline(2 * time.Second)
	if code := conn.readCloseCode(); code != 1002 {
		t.Fatalf("close code = %d, want 1002", code)
	}
	conn.clearReadDeadline()
}

// A large existing backlog is paged out in cursor order without gaps.
func TestSubscribeLargeCatchUp(t *testing.T) {
	srv, st := newWSTestServer(t)
	setupSession(t, srv, "dev-1", "sess", "doc", 0)

	const total = 2500
	batch := make([]store.Change, total)
	for i := range batch {
		batch[i] = store.Change{
			ID:       "b-" + itoa(i+1),
			DeviceID: "dev-1",
			Payload:  json.RawMessage(`{"i":` + itoa(i+1) + `}`),
		}
	}
	if _, err := st.PostChanges("doc", batch); err != nil {
		t.Fatal(err)
	}

	conn, resp := dialWS(t, subscribeURL(srv, "sess", "doc", "0"))
	if conn == nil {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	defer conn.close()
	conn.setReadDeadline(10 * time.Second)
	for want := int64(1); want <= total; want++ {
		c := conn.readChange()
		if c.Cursor != want {
			t.Fatalf("cursor = %d, want %d (id=%s)", c.Cursor, want, c.ID)
		}
	}
}

// --- helpers ----------------------------------------------------------------

func postHTTP(t *testing.T, srv *httptest.Server, path string, body any) int {
	t.Helper()
	raw, _ := json.Marshal(body)
	resp, err := srv.Client().Post(srv.URL+path, "application/json", strings.NewReader(string(raw)))
	if err != nil {
		t.Fatalf("POST %s: %v", path, err)
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	return resp.StatusCode
}

func postDocChange(t *testing.T, srv *httptest.Server, device, doc, id string, payload any) {
	t.Helper()
	if code := postHTTP(t, srv, "/v1/documents/"+doc+"/changes", map[string]any{
		"deviceId": device,
		"changes":  []any{map[string]any{"id": id, "payload": payload}},
	}); code != http.StatusOK {
		t.Fatalf("post change %s = %d", id, code)
	}
}

func httpGet(t *testing.T, srv *httptest.Server, path string) (int, map[string]any) {
	t.Helper()
	resp, err := srv.Client().Get(srv.URL + path)
	if err != nil {
		t.Fatalf("GET %s: %v", path, err)
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	var body map[string]any
	_ = json.Unmarshal(data, &body)
	return resp.StatusCode, body
}

func mustJSON(v any) []byte {
	b, _ := json.Marshal(v)
	return b
}

func deepEqualAny(a, b any) bool {
	ab, _ := json.Marshal(a)
	bb, _ := json.Marshal(b)
	return string(ab) == string(bb)
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}
