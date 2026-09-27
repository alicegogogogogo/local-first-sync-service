package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/alicegogogogogo/local-first-sync-service/internal/app"
	"github.com/alicegogogogogo/local-first-sync-service/internal/events"
)

// docSubscribeURL builds the document-level change subscription URL. The
// caller is declared by the deviceId query parameter (no session) alongside the
// mandatory starting cursor.
func docSubscribeURL(srv *httptest.Server, doc, device, cursor string) string {
	return srv.URL + "/v1/documents/" + doc + "/changes/subscribe?deviceId=" + device + "&cursor=" + cursor
}

// registerAndSeedDevice registers device (no session) and seeds the document
// with seed changes seed-1..seed-N via the document-level commit.
func registerAndSeedDevice(t *testing.T, srv *httptest.Server, device, doc string, seed int) {
	t.Helper()
	registerDeviceViaHTTP(t, srv, device)
	for i := 0; i < seed; i++ {
		postDocChange(t, srv, device, doc, "seed-"+itoa(i+1), map[string]any{"n": i + 1})
	}
}

// All pre-upgrade failures are ordinary JSON errors, checked in the fixed order
// handshake/parameter shape (400) -> device existence (404) -> permission (403).
func TestDocSubscribePreUpgradeValidation(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	postDoc := func(doc, device string) {
		t.Helper()
		w, _ := postJSON(t, h, "/v1/documents/"+doc+"/changes", map[string]any{
			"deviceId": device,
			"changes":  []any{map[string]any{"id": "c1", "payload": map[string]any{"n": 1}}},
		})
		if w.Code != http.StatusOK {
			t.Fatal(w.Body.String())
		}
	}
	postDoc("doc", "dev-1")
	w, _ := postJSON(t, h, "/v1/documents/doc2/permissions", map[string]any{"deviceId": "dev-1", "action": "revoke"})
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
		{"cursor negative", http.MethodGet, base + "?deviceId=dev-1&cursor=-1", true, http.StatusBadRequest},
		{"cursor fractional", http.MethodGet, base + "?deviceId=dev-1&cursor=1.5", true, http.StatusBadRequest},
		{"cursor non-numeric", http.MethodGet, base + "?deviceId=dev-1&cursor=abc", true, http.StatusBadRequest},
		{"cursor missing", http.MethodGet, base + "?deviceId=dev-1", true, http.StatusBadRequest},
		{"no upgrade headers", http.MethodGet, base + "?deviceId=dev-1&cursor=0", false, http.StatusBadRequest},
		{"wrong method", http.MethodPost, base + "?deviceId=dev-1&cursor=0", false, http.StatusBadRequest},
		{"trailing slash", http.MethodGet, base + "/?deviceId=dev-1&cursor=0", true, http.StatusBadRequest},
		{"extra segment", http.MethodGet, base + "/extra?deviceId=dev-1&cursor=0", true, http.StatusBadRequest},
		{"missing changes segment", http.MethodGet, "/v1/documents/doc/subscribe?deviceId=dev-1&cursor=0", true, http.StatusBadRequest},
		{"missing deviceId param", http.MethodGet, base + "?cursor=0", true, http.StatusNotFound},
		{"empty deviceId param", http.MethodGet, base + "?deviceId=&cursor=0", true, http.StatusNotFound},
		{"unregistered device", http.MethodGet, base + "?deviceId=ghost&cursor=0", true, http.StatusNotFound},
		{"revoked permission", http.MethodGet, "/v1/documents/doc2/changes/subscribe?deviceId=dev-1&cursor=0", true, http.StatusForbidden},
		// Shape wins over a missing device: a bad cursor is a 400 first.
		{"bad cursor before device lookup", http.MethodGet, base + "?deviceId=ghost&cursor=x", true, http.StatusBadRequest},
		// Handshake shape wins over the device lookup.
		{"bad handshake before device lookup", http.MethodGet, base + "?deviceId=ghost&cursor=0", false, http.StatusBadRequest},
		// Device existence wins over permission.
		{"unknown device before revoked permission", http.MethodGet, "/v1/documents/doc2/changes/subscribe?deviceId=ghost&cursor=0", true, http.StatusNotFound},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var r *http.Request
			if tc.upgrade {
				r = upgradeRequest(tc.target)
			} else {
				r = httptest.NewRequest(tc.method, tc.target, nil)
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

// A malformed handshake over a real connection is a 400 JSON response.
func TestDocSubscribeBadHandshakeOverTCP(t *testing.T) {
	srv, _ := newWSTestServer(t)
	registerAndSeedDevice(t, srv, "dev-1", "doc", 0)
	target := "/v1/documents/doc/changes/subscribe?deviceId=dev-1&cursor=0"

	cases := []struct {
		name    string
		headers []string
	}{
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
}

// A valid handshake upgrades and replays seeded changes in cursor order.
func TestDocSubscribeHandshakeAndCatchUp(t *testing.T) {
	srv, _ := newWSTestServer(t)
	registerAndSeedDevice(t, srv, "dev-1", "doc", 3)

	conn, resp := dialWS(t, docSubscribeURL(srv, "doc", "dev-1", "1"))
	if conn == nil {
		t.Fatalf("status = %d, want 101", resp.StatusCode)
	}
	defer conn.close()

	got := []events.ListedChange{conn.readChange(), conn.readChange()}
	if got[0].Cursor != 2 || got[0].ID != "seed-2" || got[1].Cursor != 3 || got[1].ID != "seed-3" {
		t.Fatalf("catch-up = %+v", got)
	}
}

// After catch-up the connection seamlessly streams live commits from every
// write path, including the session batch commit.
func TestDocSubscribeLivePushFromEveryWritePath(t *testing.T) {
	srv, st := newWSTestServer(t)
	registerAndSeedDevice(t, srv, "dev-1", "doc", 1)
	if _, err := st.PutSnapshot("doc", 1, json.RawMessage(`{"from":"snapshot"}`)); err != nil {
		t.Fatal(err)
	}
	// A session owned by the same device drives the session batch path.
	if code := postHTTP(t, srv, "/v1/devices/dev-1/sessions", map[string]any{"sessionId": "sess"}); code != http.StatusOK {
		t.Fatalf("create session = %d", code)
	}

	conn, resp := dialWS(t, docSubscribeURL(srv, "doc", "dev-1", "1"))
	if conn == nil {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	defer conn.close()

	// Ordinary document commit.
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

	// Session batch commit reaches the document-level subscription too.
	raw, _ := json.Marshal(map[string]any{
		"changes": []any{map[string]any{"id": "sess-1", "payload": map[string]any{"s": 1}}},
	})
	sresp, err := srv.Client().Post(srv.URL+"/v1/sessions/sess/documents/doc/changes",
		"application/json", strings.NewReader(string(raw)))
	if err != nil {
		t.Fatalf("session batch: %v", err)
	}
	sresp.Body.Close()
	if sresp.StatusCode != http.StatusOK {
		t.Fatalf("session batch = %d", sresp.StatusCode)
	}
	if c := conn.readChange(); c.Cursor != 6 || c.ID != "sess-1" || c.DeviceID != "dev-1" {
		t.Fatalf("session batch frame = %+v", c)
	}
}

// An idempotent resubmission allocates no cursor and pushes no frame.
func TestDocSubscribeIdempotentCommitPushesNothing(t *testing.T) {
	srv, _ := newWSTestServer(t)
	registerAndSeedDevice(t, srv, "dev-1", "doc", 1)

	conn, resp := dialWS(t, docSubscribeURL(srv, "doc", "dev-1", "1"))
	if conn == nil {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	defer conn.close()

	postDocChange(t, srv, "dev-1", "doc", "dup", map[string]any{"v": 2})
	if c := conn.readChange(); c.ID != "dup" || c.Cursor != 2 {
		t.Fatalf("frame = %+v", c)
	}
	// Re-post the identical id+payload: idempotent, no frame.
	postDocChange(t, srv, "dev-1", "doc", "dup", map[string]any{"v": 2})
	conn.setReadDeadline(300 * time.Millisecond)
	if _, _, _, ok := conn.readFrameMaybe(); ok {
		t.Fatal("an idempotent commit pushed a frame")
	}
	conn.clearReadDeadline()
}

// Two distinct registered devices each hold an independent subscription;
// revoking one closes only that connection with 4403 and the other keeps
// receiving commits.
func TestDocSubscribePerDeviceRevoke(t *testing.T) {
	srv, _ := newWSTestServer(t)
	registerAndSeedDevice(t, srv, "dev-1", "doc", 0)
	registerDeviceViaHTTP(t, srv, "dev-2")

	c1, hs := dialWS(t, docSubscribeURL(srv, "doc", "dev-1", "0"))
	if c1 == nil {
		t.Fatalf("dev-1 upgrade = %d", hs.StatusCode)
	}
	defer c1.close()
	c2, hs := dialWS(t, docSubscribeURL(srv, "doc", "dev-2", "0"))
	if c2 == nil {
		t.Fatalf("dev-2 upgrade = %d", hs.StatusCode)
	}
	defer c2.close()

	if code := postHTTP(t, srv, "/v1/documents/doc/permissions", map[string]any{
		"deviceId": "dev-1", "action": "revoke",
	}); code != http.StatusOK {
		t.Fatalf("revoke = %d", code)
	}
	c1.setReadDeadline(2 * time.Second)
	if code := c1.readCloseCode(); code != 4403 {
		t.Fatalf("dev-1 close = %d, want 4403", code)
	}
	c1.clearReadDeadline()

	// dev-2 stays live and receives the next commit.
	postDocChange(t, srv, "dev-2", "doc", "live-2", map[string]any{"n": 1})
	if c := c2.readChange(); c.ID != "live-2" {
		t.Fatalf("dev-2 frame = %+v", c)
	}
	c1.setReadDeadline(300 * time.Millisecond)
	if _, _, _, ok := c1.readFrameMaybe(); ok {
		t.Fatal("a frame arrived on the revoked connection")
	}
	c1.clearReadDeadline()
}

// A revoke is sticky on the live connection even if a grant races in.
func TestDocSubscribeRevokeStickyDespiteGrant(t *testing.T) {
	srv, _ := newWSTestServer(t)
	registerAndSeedDevice(t, srv, "dev-1", "doc", 1)

	conn, resp := dialWS(t, docSubscribeURL(srv, "doc", "dev-1", "0"))
	if conn == nil {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	defer conn.close()
	if c := conn.readChange(); c.Cursor != 1 {
		t.Fatalf("seed = %+v", c)
	}

	if code := postHTTP(t, srv, "/v1/documents/doc/permissions", map[string]any{"deviceId": "dev-1", "action": "revoke"}); code != http.StatusOK {
		t.Fatalf("revoke = %d", code)
	}
	if code := postHTTP(t, srv, "/v1/documents/doc/permissions", map[string]any{"deviceId": "dev-1", "action": "grant"}); code != http.StatusOK {
		t.Fatalf("grant = %d", code)
	}
	conn.setReadDeadline(2 * time.Second)
	if code := conn.readCloseCode(); code != 4403 {
		t.Fatalf("close = %d, want sticky 4403", code)
	}
	conn.clearReadDeadline()
}

// The termination signal ends the subscription with 1001; committed data
// remains readable.
func TestDocSubscribeShutdownCloses1001(t *testing.T) {
	srv, st := newWSTestServer(t)
	registerAndSeedDevice(t, srv, "dev-1", "doc", 1)

	conn, resp := dialWS(t, docSubscribeURL(srv, "doc", "dev-1", "1"))
	if conn == nil {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	defer conn.close()

	st.InterruptWaits()
	conn.setReadDeadline(2 * time.Second)
	if code := conn.readCloseCode(); code != 1001 {
		t.Fatalf("close = %d, want 1001", code)
	}
	conn.clearReadDeadline()

	rows, next, err := st.ListChanges("doc", 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || next != 1 {
		t.Fatalf("committed state after signal = %+v/%d", rows, next)
	}
}

// The connection is push-only: inbound data frames are discarded and never
// written; live pushes continue.
func TestDocSubscribePushOnly(t *testing.T) {
	srv, st := newWSTestServer(t)
	registerAndSeedDevice(t, srv, "dev-1", "doc", 1)

	conn, resp := dialWS(t, docSubscribeURL(srv, "doc", "dev-1", "0"))
	if conn == nil {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	defer conn.close()
	if c := conn.readChange(); c.Cursor != 1 {
		t.Fatalf("seed = %+v", c)
	}

	conn.sendText(`{"id":"forged","payload":{"hack":true}}`)
	conn.sendPing()
	conn.setReadDeadline(300 * time.Millisecond)
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
			t.Fatal("server echoed a discarded client frame")
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

// A subscription does not advance the cursor and leaves no record: after
// connecting from cursor 0 on an empty document and disconnecting, the first
// real commit still takes cursor 1.
func TestDocSubscribeDoesNotConsumeCursor(t *testing.T) {
	srv, st := newWSTestServer(t)
	registerDeviceViaHTTP(t, srv, "dev-1")

	conn, resp := dialWS(t, docSubscribeURL(srv, "fresh", "dev-1", "0"))
	if conn == nil {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	conn.setReadDeadline(200 * time.Millisecond)
	if _, _, _, ok := conn.readFrameMaybe(); ok {
		t.Fatal("empty document produced a frame")
	}
	conn.clearReadDeadline()
	conn.sendClose(1000)
	conn.close()
	time.Sleep(100 * time.Millisecond)

	postDocChange(t, srv, "dev-1", "fresh", "first", map[string]any{"v": 1})
	rows, next, err := st.ListChanges("fresh", 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || next != 1 || rows[0].Cursor != 1 {
		t.Fatalf("subscription consumed a cursor: %+v next=%d", rows, next)
	}
}

// A document and a device literally named after endpoint keywords still
// subscribe normally.
func TestDocSubscribeKeywordIdentifiers(t *testing.T) {
	srv, _ := newWSTestServer(t)
	registerAndSeedDevice(t, srv, "subscribe", "subscribe", 1)

	conn, hs := dialWS(t, docSubscribeURL(srv, "subscribe", "subscribe", "0"))
	if conn == nil {
		t.Fatalf("upgrade = %d, want 101", hs.StatusCode)
	}
	defer conn.close()
	if c := conn.readChange(); c.ID != "seed-1" {
		t.Fatalf("frame = %+v", c)
	}
	postDocChange(t, srv, "subscribe", "subscribe", "live", map[string]any{"n": 2})
	if c := conn.readChange(); c.ID != "live" {
		t.Fatalf("live frame = %+v", c)
	}
}

// Subscription state is not persisted: after a restart a fresh changes
// subscription replays from any historical cursor and a fresh CRDT
// subscription immediately receives the consistent current merged state.
func TestDocSubscriptionsRestartResumable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "doc-subscribe-restart.db")

	first, err := app.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	srv1 := httptest.NewServer(NewHandler(first))
	registerDeviceViaHTTP(t, srv1, "dev-1")
	postDocChange(t, srv1, "dev-1", "doc", "c1", map[string]any{"n": 1})
	postDocChange(t, srv1, "dev-1", "doc", "c2", map[string]any{"n": 2})
	if code := postCRDTOps(t, srv1, "doc", crdtCounterBody("dev-1", crdtCounterOp("a1", 9))); code != http.StatusOK {
		t.Fatalf("crdt submit = %d", code)
	}
	srv1.Close()
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}

	second, err := app.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = second.Close() })
	srv2 := httptest.NewServer(NewHandler(second))
	t.Cleanup(srv2.Close)

	// Changes: a fresh subscription from a historical cursor replays durable
	// rows in cursor order.
	changeConn, hs := dialWS(t, docSubscribeURL(srv2, "doc", "dev-1", "0"))
	if changeConn == nil {
		t.Fatalf("changes upgrade after restart = %d", hs.StatusCode)
	}
	defer changeConn.close()
	if c := changeConn.readChange(); c.ID != "c1" || c.Cursor != 1 {
		t.Fatalf("first replayed = %+v", c)
	}
	if c := changeConn.readChange(); c.ID != "c2" || c.Cursor != 2 {
		t.Fatalf("second replayed = %+v", c)
	}
	changeConn.setReadDeadline(300 * time.Millisecond)
	if _, _, _, ok := changeConn.readFrameMaybe(); ok {
		t.Fatal("a frame arrived after catch-up with no new commit")
	}
	changeConn.clearReadDeadline()

	// CRDT: a fresh subscription immediately gets the consistent merge.
	crdtConn, hs := dialWS(t, docCRDTSubscribeURL(srv2, "doc", "dev-1"))
	if crdtConn == nil {
		t.Fatalf("crdt upgrade after restart = %d", hs.StatusCode)
	}
	defer crdtConn.close()
	if n := crdtCounterValue(t, crdtConn.readCRDTState()); n != 9 {
		t.Fatalf("crdt state after restart = %d, want 9", n)
	}
}

// Deregistering the subscribing device ends both of its document-level
// subscriptions with 4403 immediately after the cascade commits; a reconnect
// is a pre-upgrade 404 because the device is gone.
func TestDocSubscriptionsDeviceDeregistrationCloses4403(t *testing.T) {
	srv, _ := newWSTestServer(t)
	registerAndSeedDevice(t, srv, "dev-1", "doc", 1)
	if code := postCRDTOps(t, srv, "doc", crdtCounterBody("dev-1", crdtCounterOp("a1", 2))); code != http.StatusOK {
		t.Fatalf("crdt submit = %d", code)
	}

	changeConn, hs := dialWS(t, docSubscribeURL(srv, "doc", "dev-1", "0"))
	if changeConn == nil {
		t.Fatalf("changes upgrade = %d", hs.StatusCode)
	}
	defer changeConn.close()
	if c := changeConn.readChange(); c.Cursor != 1 {
		t.Fatalf("change seed = %+v", c)
	}

	crdtConn, hs := dialWS(t, docCRDTSubscribeURL(srv, "doc", "dev-1"))
	if crdtConn == nil {
		t.Fatalf("crdt upgrade = %d", hs.StatusCode)
	}
	defer crdtConn.close()
	if n := crdtCounterValue(t, crdtConn.readCRDTState()); n != 2 {
		t.Fatalf("crdt seed = %d", n)
	}

	req, _ := http.NewRequest(http.MethodDelete, srv.URL+"/v1/devices/dev-1", nil)
	dresp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatalf("deregister: %v", err)
	}
	dresp.Body.Close()
	if dresp.StatusCode != http.StatusOK {
		t.Fatalf("deregister = %d", dresp.StatusCode)
	}

	changeConn.setReadDeadline(2 * time.Second)
	if code := changeConn.readCloseCode(); code != 4403 {
		t.Fatalf("changes close = %d, want 4403", code)
	}
	changeConn.clearReadDeadline()
	crdtConn.setReadDeadline(2 * time.Second)
	if code := crdtConn.readCloseCode(); code != 4403 {
		t.Fatalf("crdt close = %d, want 4403", code)
	}
	crdtConn.clearReadDeadline()

	// The device no longer exists: a reconnect is a pre-upgrade 404.
	if bad, r := dialWS(t, docSubscribeURL(srv, "doc", "dev-1", "0")); bad != nil || r.StatusCode != http.StatusNotFound {
		if bad != nil {
			bad.close()
		}
		t.Fatalf("changes reconnect after deregister = %d, want 404", r.StatusCode)
	}
	if bad, r := dialWS(t, docCRDTSubscribeURL(srv, "doc", "dev-1")); bad != nil || r.StatusCode != http.StatusNotFound {
		if bad != nil {
			bad.close()
		}
		t.Fatalf("crdt reconnect after deregister = %d, want 404", r.StatusCode)
	}
}
