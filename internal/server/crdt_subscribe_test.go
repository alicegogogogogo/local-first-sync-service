package server

import (
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/alicegogogogogo/local-first-sync-service/internal/store"
)

func crdtSubscribeURL(srv *httptest.Server, session, doc string) string {
	return srv.URL + "/v1/sessions/" + session + "/documents/" + doc + "/crdt/state/subscribe"
}

// submitCounter posts one counter op and asserts 200.
func submitCounter(t *testing.T, srv *httptest.Server, doc, device, id string, value int) {
	t.Helper()
	if code := postHTTP(t, srv, "/v1/documents/"+doc+"/crdt/ops", map[string]any{
		"deviceId": device,
		"type":     "counter",
		"ops":      []any{map[string]any{"id": id, "value": value}},
	}); code != http.StatusOK {
		t.Fatalf("counter op %s=%d = %d", id, value, code)
	}
}

// submitCounterStatus posts one counter op and returns the HTTP status.
func submitCounterStatus(srv *httptest.Server, doc, device, id string, value int) int {
	raw, _ := json.Marshal(map[string]any{
		"deviceId": device,
		"type":     "counter",
		"ops":      []any{map[string]any{"id": id, "value": value}},
	})
	resp, err := srv.Client().Post(srv.URL+"/v1/documents/"+doc+"/crdt/ops",
		"application/json", strings.NewReader(string(raw)))
	if err != nil {
		return -1
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	return resp.StatusCode
}

// submitGSet posts one gset op and asserts 200.
func submitGSet(t *testing.T, srv *httptest.Server, doc, device, id string, elements ...string) {
	t.Helper()
	if code := postHTTP(t, srv, "/v1/documents/"+doc+"/crdt/ops", map[string]any{
		"deviceId": device,
		"type":     "gset",
		"ops":      []any{map[string]any{"id": id, "elements": elements}},
	}); code != http.StatusOK {
		t.Fatalf("gset op %s = %d", id, code)
	}
}

// readCRDTState reads the next text frame and decodes the CRDT state. Pongs
// are skipped; any other frame fails the test.
func (c *wsClient) readCRDTState() store.CRDTState {
	c.t.Helper()
	for {
		fin, opcode, payload := c.readFrame()
		switch opcode {
		case wsOpcodeText:
			if !fin {
				c.t.Fatal("server sent a fragmented text frame")
			}
			var state store.CRDTState
			if err := json.Unmarshal(payload, &state); err != nil {
				c.t.Fatalf("text frame is not a crdt state: %s (%v)", payload, err)
			}
			return state
		case wsOpcodePong:
			continue
		case wsOpcodeClose:
			c.t.Fatalf("unexpected close frame while reading crdt state: code=%d", closeCode(payload))
		default:
			c.t.Fatalf("unexpected opcode %d while reading crdt state", opcode)
		}
	}
}

// readCRDTStateRaw reads the next text frame bytes verbatim (including the
// trailing newline) for byte-for-byte comparisons.
func (c *wsClient) readCRDTStateRaw() []byte {
	c.t.Helper()
	for {
		fin, opcode, payload := c.readFrame()
		if opcode == wsOpcodePong {
			continue
		}
		if opcode != wsOpcodeText || !fin {
			c.t.Fatalf("frame opcode=%d fin=%v, want a single text frame", opcode, fin)
		}
		return payload
	}
}

// Validation runs entirely before the upgrade: handshake shape (400), then
// session existence (404), then permission (403).
func TestCRDTSubscribePreUpgradeOrdering(t *testing.T) {
	srv, _ := newWSTestServer(t)
	setupSession(t, srv, "dev-1", "sess", "doc", 0)
	submitCounter(t, srv, "locked", "dev-1", "a1", 1)
	if code := postHTTP(t, srv, "/v1/documents/locked/permissions", map[string]any{
		"deviceId": "dev-1", "action": "revoke",
	}); code != http.StatusOK {
		t.Fatalf("revoke = %d", code)
	}

	target := "/v1/sessions/sess/documents/doc/crdt/state/subscribe"
	cases := []struct {
		name       string
		target     string
		upgraded   bool
		wantStatus int
	}{
		{"no upgrade headers", target, false, http.StatusBadRequest},
		{"empty session segment", "/v1/sessions//documents/doc/crdt/state/subscribe", true, http.StatusBadRequest},
		{"empty document segment", "/v1/sessions/sess/documents//crdt/state/subscribe", true, http.StatusBadRequest},
		{"trailing slash", target + "/", true, http.StatusBadRequest},
		{"extra segment", target + "/extra", true, http.StatusBadRequest},
		{"missing state segment", "/v1/sessions/sess/documents/doc/crdt/other/subscribe", true, http.StatusBadRequest},
		{"unknown session", "/v1/sessions/ghost/documents/doc/crdt/state/subscribe", true, http.StatusNotFound},
		{"revoked permission", "/v1/sessions/sess/documents/locked/crdt/state/subscribe", true, http.StatusForbidden},
		// Validation order: a bad handshake wins over a missing session.
		{"bad handshake before unknown session", "/v1/sessions/ghost/documents/doc/crdt/state/subscribe", false, http.StatusBadRequest},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var resp *http.Response
			var c *wsClient
			var raw net.Conn
			if tc.upgraded {
				c, resp = dialWS(t, srv.URL+tc.target)
			} else {
				resp, raw = dialWSRaw(t, srv.URL, tc.target,
					"Sec-WebSocket-Version: 13",
					"Sec-WebSocket-Key: "+newWSKey())
			}
			if c != nil {
				defer c.close()
			} else if raw != nil {
				defer raw.Close()
			}
			if resp.StatusCode != tc.wantStatus {
				t.Fatalf("status = %d, want %d", resp.StatusCode, tc.wantStatus)
			}
			if ct := resp.Header.Get("Content-Type"); ct != "application/json" {
				t.Fatalf("content-type = %q, want application/json", ct)
			}
			data, _ := io.ReadAll(resp.Body)
			if !strings.Contains(string(data), `"error"`) {
				t.Fatalf("body = %q, want a JSON error", data)
			}
		})
	}

	// A non-GET verb on the exact path is a JSON 400, not a plain-text 405.
	req := httptest.NewRequest(http.MethodPost, target, nil)
	req.Header.Set("Connection", "Upgrade")
	req.Header.Set("Upgrade", "websocket")
	req.Header.Set("Sec-WebSocket-Key", newWSKey())
	req.Header.Set("Sec-WebSocket-Version", "13")
	rec := httptest.NewRecorder()
	newHandlerOnly(srv).ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), `"error"`) {
		t.Fatalf("POST on subscribe path = %d %s, want JSON 400", rec.Code, rec.Body.String())
	}
}

// newHandlerOnly exposes the test server's handler for in-process requests.
func newHandlerOnly(srv *httptest.Server) http.Handler { return srv.Config.Handler }

// A malformed handshake over a real connection is a 400 JSON response.
func TestCRDTSubscribeBadHandshakeOverTCP(t *testing.T) {
	srv, _ := newWSTestServer(t)
	setupSession(t, srv, "dev-1", "sess", "doc", 0)
	target := "/v1/sessions/sess/documents/doc/crdt/state/subscribe"

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
			data, _ := io.ReadAll(resp.Body)
			if !strings.Contains(string(data), `"error"`) {
				t.Fatalf("body = %q, want JSON error", data)
			}
		})
	}
}

// A valid handshake on a document that already has state pushes the current
// merged state first, as bytes identical to the state-read body.
func TestCRDTSubscribeFirstFrameIsCurrentState(t *testing.T) {
	srv, _ := newWSTestServer(t)
	setupSession(t, srv, "dev-1", "sess", "doc", 0)
	submitCounter(t, srv, "doc", "dev-1", "a1", 5)
	submitCounter(t, srv, "doc", "dev-1", "a2", 8)

	conn, resp := dialWS(t, crdtSubscribeURL(srv, "sess", "doc"))
	if conn == nil {
		t.Fatalf("status = %d, want 101", resp.StatusCode)
	}
	defer conn.close()

	frame := string(conn.readCRDTStateRaw())
	if frame != `{"type":"counter","value":8}`+"\n" {
		t.Fatalf("first frame = %q, want the current merged state with a newline", frame)
	}

	httpResp, err := srv.Client().Get(srv.URL + "/v1/documents/doc/crdt/state")
	if err != nil {
		t.Fatal(err)
	}
	defer httpResp.Body.Close()
	body, _ := io.ReadAll(httpResp.Body)
	if frame != string(body) {
		t.Fatalf("frame %q != state-read body %q", frame, body)
	}
}

// With no CRDT operations the connection waits silently; the first state to
// appear is its first frame.
func TestCRDTSubscribeSilentUntilFirstState(t *testing.T) {
	srv, _ := newWSTestServer(t)
	setupSession(t, srv, "dev-1", "sess", "doc", 0)

	conn, resp := dialWS(t, crdtSubscribeURL(srv, "sess", "doc"))
	if conn == nil {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	defer conn.close()

	conn.setReadDeadline(300 * time.Millisecond)
	if _, _, _, ok := conn.readFrameMaybe(); ok {
		t.Fatal("a frame arrived before any CRDT operation")
	}
	conn.clearReadDeadline()

	submitCounter(t, srv, "doc", "dev-1", "a1", 5)
	state := conn.readCRDTState()
	if state.Type != store.CRDTTypeCounter || string(state.Value) != "5" {
		t.Fatalf("first state = %+v, want counter/5", state)
	}
}

// Counter: only genuine merged-value changes push; equal contributions,
// idempotent reposts and rejected regressions push nothing.
func TestCRDTSubscribeCounterChangeNotification(t *testing.T) {
	srv, _ := newWSTestServer(t)
	setupSession(t, srv, "dev-1", "sess", "doc", 0)
	submitCounter(t, srv, "doc", "dev-1", "a1", 5)

	conn, resp := dialWS(t, crdtSubscribeURL(srv, "sess", "doc"))
	if conn == nil {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	defer conn.close()
	if first := conn.readCRDTState(); string(first.Value) != "5" {
		t.Fatalf("first frame = %+v, want 5", first)
	}

	assertNoFrame := func(what string) {
		t.Helper()
		conn.setReadDeadline(300 * time.Millisecond)
		if _, _, _, ok := conn.readFrameMaybe(); ok {
			t.Fatalf("a frame arrived for %s", what)
		}
		conn.clearReadDeadline()
	}

	// Equal contribution with a new id: accepted, no state change, no frame.
	submitCounter(t, srv, "doc", "dev-1", "a2", 5)
	assertNoFrame("equal contribution")

	// Idempotent repost: no frame.
	submitCounter(t, srv, "doc", "dev-1", "a1", 5)
	assertNoFrame("idempotent repost")

	// Rejected regression: 409 and no frame.
	if code := submitCounterStatus(srv, "doc", "dev-1", "a3", 4); code != http.StatusConflict {
		t.Fatalf("regression status = %d, want 409", code)
	}
	assertNoFrame("rejected regression")

	// Genuine advance: exactly one frame with the new sum.
	submitCounter(t, srv, "doc", "dev-1", "a4", 9)
	state := conn.readCRDTState()
	if state.Type != store.CRDTTypeCounter || string(state.Value) != "9" {
		t.Fatalf("advance frame = %+v, want counter/9", state)
	}
}

// GSet: states are sorted unions; adding an element already present pushes
// nothing; new elements push one sorted frame.
func TestCRDTSubscribeGSetSortedUnionAndNoop(t *testing.T) {
	srv, _ := newWSTestServer(t)
	setupSession(t, srv, "dev-1", "sess", "doc", 0)

	conn, resp := dialWS(t, crdtSubscribeURL(srv, "sess", "doc"))
	if conn == nil {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	defer conn.close()

	submitGSet(t, srv, "doc", "dev-1", "g1", "banana", "apple")
	if frame := string(conn.readCRDTStateRaw()); frame != `{"type":"gset","value":["apple","banana"]}`+"\n" {
		t.Fatalf("first gset frame = %q", frame)
	}

	submitGSet(t, srv, "doc", "dev-1", "g2", "apple")
	conn.setReadDeadline(300 * time.Millisecond)
	if _, _, _, ok := conn.readFrameMaybe(); ok {
		t.Fatal("re-adding an existing element pushed a frame")
	}
	conn.clearReadDeadline()

	submitGSet(t, srv, "doc", "dev-1", "g3", "cherry", "apple")
	state := conn.readCRDTState()
	if state.Type != store.CRDTTypeGSet || string(state.Value) != `["apple","banana","cherry"]` {
		t.Fatalf("grown gset frame = %+v", state)
	}
}

// Every live subscriber of the document receives each changed state, and
// rapid successive commits arrive once each in commit order.
func TestCRDTSubscribeFanOutAndCommitOrder(t *testing.T) {
	srv, _ := newWSTestServer(t)
	setupSession(t, srv, "dev-1", "sess-1", "doc", 0)
	w, _ := postJSON(t, newHandlerOnly(srv), "/v1/devices/dev-1/sessions", map[string]any{"sessionId": "sess-2"})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}

	c1, r1 := dialWS(t, crdtSubscribeURL(srv, "sess-1", "doc"))
	if c1 == nil {
		t.Fatalf("sub1 status = %d", r1.StatusCode)
	}
	defer c1.close()
	c2, r2 := dialWS(t, crdtSubscribeURL(srv, "sess-2", "doc"))
	if c2 == nil {
		t.Fatalf("sub2 status = %d", r2.StatusCode)
	}
	defer c2.close()

	for i, v := range []int{1, 2, 2, 3, 5} {
		submitCounter(t, srv, "doc", "dev-1", idFor(i), v)
	}
	want := []string{"1", "2", "3", "5"} // the equal 2 changes nothing
	for _, conn := range []*wsClient{c1, c2} {
		for i, wv := range want {
			state := conn.readCRDTState()
			if string(state.Value) != wv {
				t.Fatalf("subscriber frame %d = %s, want %s", i, state.Value, wv)
			}
		}
	}
}

func idFor(i int) string { return "op-" + string(rune('a'+i)) }

// Revocation after the upgrade ends the subscription with 4403; a grant that
// follows does not revive the connection, while a fresh subscription works.
func TestCRDTSubscribeRevokedAfterUpgradeCloses4403(t *testing.T) {
	srv, _ := newWSTestServer(t)
	setupSession(t, srv, "dev-1", "sess", "doc", 0)
	submitCounter(t, srv, "doc", "dev-1", "a1", 5)

	conn, resp := dialWS(t, crdtSubscribeURL(srv, "sess", "doc"))
	if conn == nil {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	defer conn.close()
	if first := conn.readCRDTState(); string(first.Value) != "5" {
		t.Fatalf("first frame = %+v", first)
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

	// Re-grant immediately: the closed connection stays dead.
	if code := postHTTP(t, srv, "/v1/documents/doc/permissions", map[string]any{
		"deviceId": "dev-1", "action": "grant",
	}); code != http.StatusOK {
		t.Fatalf("grant = %d", code)
	}
	conn.setReadDeadline(300 * time.Millisecond)
	if _, _, _, ok := conn.readFrameMaybe(); ok {
		t.Fatal("a frame arrived on the revoked connection after a grant")
	}
	conn.clearReadDeadline()

	// A fresh subscription after the grant receives the current state again.
	conn2, resp2 := dialWS(t, crdtSubscribeURL(srv, "sess", "doc"))
	if conn2 == nil {
		t.Fatalf("reconnect = %d, want 101", resp2.StatusCode)
	}
	defer conn2.close()
	if state := conn2.readCRDTState(); string(state.Value) != "5" {
		t.Fatalf("post-grant first frame = %+v, want counter/5", state)
	}
}

// The termination signal ends the subscription with 1001; committed CRDT
// state stays readable.
func TestCRDTSubscribeShutdownCloses1001(t *testing.T) {
	srv, st := newWSTestServer(t)
	setupSession(t, srv, "dev-1", "sess", "doc", 0)
	submitCounter(t, srv, "doc", "dev-1", "a1", 7)

	conn, resp := dialWS(t, crdtSubscribeURL(srv, "sess", "doc"))
	if conn == nil {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	defer conn.close()
	if first := conn.readCRDTState(); string(first.Value) != "7" {
		t.Fatalf("first frame = %+v", first)
	}

	st.InterruptWaits()
	conn.setReadDeadline(2 * time.Second)
	if code := conn.readCloseCode(); code != 1001 {
		t.Fatalf("close code = %d, want 1001", code)
	}
	conn.clearReadDeadline()

	state, err := st.GetCRDTState("doc")
	if err != nil || string(state.Value) != "7" {
		t.Fatalf("committed state after signal = %+v err=%v", state, err)
	}
}

// The connection is push-only: inbound data frames are read and discarded
// (the state is untouched), while a ping still gets a pong.
func TestCRDTSubscribePushOnlyAndPingPong(t *testing.T) {
	srv, _ := newWSTestServer(t)
	setupSession(t, srv, "dev-1", "sess", "doc", 0)
	submitCounter(t, srv, "doc", "dev-1", "a1", 5)

	conn, resp := dialWS(t, crdtSubscribeURL(srv, "sess", "doc"))
	if conn == nil {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	defer conn.close()
	if first := conn.readCRDTState(); string(first.Value) != "5" {
		t.Fatalf("first frame = %+v", first)
	}

	// Arbitrary client data must not mutate anything and must be discarded.
	conn.sendText(`{"id":"hacked","value":999}`)
	conn.sendText("not even json")

	// A ping is answered with a pong carrying the same payload.
	conn.sendMasked(wsOpcodePing, []byte("keepalive"))
	for {
		_, opcode, payload := conn.readFrame()
		if opcode != wsOpcodePong {
			t.Fatalf("after ping opcode = %d, want pong", opcode)
		}
		if string(payload) != "keepalive" {
			t.Fatalf("pong payload = %q, want echo", payload)
		}
		break
	}

	// The merged state is unchanged, and a genuine advance still pushes.
	status, body := httpGet(t, srv, "/v1/documents/doc/crdt/state")
	if status != http.StatusOK || body["value"].(float64) != 5 {
		t.Fatalf("state after client frames = %d %v", status, body)
	}
	submitCounter(t, srv, "doc", "dev-1", "a2", 6)
	if state := conn.readCRDTState(); string(state.Value) != "6" {
		t.Fatalf("advance frame = %+v, want counter/6", state)
	}
}

// Pushing consumes no document cursor and leaves no change record.
func TestCRDTSubscribeDoesNotTouchChangeLog(t *testing.T) {
	srv, _ := newWSTestServer(t)
	setupSession(t, srv, "dev-1", "sess", "doc", 0)
	submitCounter(t, srv, "doc", "dev-1", "a1", 5)

	conn, resp := dialWS(t, crdtSubscribeURL(srv, "sess", "doc"))
	if conn == nil {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	defer conn.close()
	if first := conn.readCRDTState(); string(first.Value) != "5" {
		t.Fatalf("first frame = %+v", first)
	}
	submitCounter(t, srv, "doc", "dev-1", "a2", 6)
	if state := conn.readCRDTState(); string(state.Value) != "6" {
		t.Fatalf("second frame = %+v", state)
	}

	status, body := httpGet(t, srv, "/v1/documents/doc/changes")
	if status != http.StatusOK {
		t.Fatalf("changes status = %d", status)
	}
	if rows := body["changes"].([]any); len(rows) != 0 {
		t.Fatalf("CRDT pushes produced change rows: %v", rows)
	}
	if next := body["nextCursor"].(float64); next != 0 {
		t.Fatalf("nextCursor = %v, want 0 (no change log activity)", next)
	}
}
