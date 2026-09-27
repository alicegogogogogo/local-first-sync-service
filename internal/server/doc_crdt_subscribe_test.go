package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/alicegogogogogo/local-first-sync-service/internal/crdt"
)

// docCRDTSubscribeURL builds the document-level CRDT state subscription URL.
// The caller is declared by deviceId (no session); there is no cursor.
func docCRDTSubscribeURL(srv *httptest.Server, doc, device string) string {
	return srv.URL + "/v1/documents/" + doc + "/crdt/state/subscribe?deviceId=" + device
}

// Pre-upgrade failures follow the fixed order: handshake/path shape (400) ->
// device existence (404) -> permission (403), all JSON and before the upgrade.
func TestDocCRDTSubscribePreUpgradeValidation(t *testing.T) {
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
		{"trailing slash", http.MethodGet, base + "/?deviceId=dev-1", true, http.StatusBadRequest},
		{"extra segment", http.MethodGet, base + "/extra?deviceId=dev-1", true, http.StatusBadRequest},
		{"missing state segment", http.MethodGet, "/v1/documents/doc/crdt/subscribe?deviceId=dev-1", true, http.StatusBadRequest},
		{"bare crdt namespace", http.MethodGet, "/v1/documents/doc/crdt?deviceId=dev-1", true, http.StatusBadRequest},
		{"missing deviceId param", http.MethodGet, base, true, http.StatusNotFound},
		{"empty deviceId param", http.MethodGet, base + "?deviceId=", true, http.StatusNotFound},
		{"unregistered device", http.MethodGet, base + "?deviceId=ghost", true, http.StatusNotFound},
		{"revoked permission", http.MethodGet, "/v1/documents/doc2/crdt/state/subscribe?deviceId=dev-1", true, http.StatusForbidden},
		// Handshake shape wins over the device lookup.
		{"bad handshake before device lookup", http.MethodGet, base + "?deviceId=ghost", false, http.StatusBadRequest},
		// Device existence wins over permission.
		{"unknown device before revoked permission", http.MethodGet, "/v1/documents/doc2/crdt/state/subscribe?deviceId=ghost", true, http.StatusNotFound},
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
func TestDocCRDTSubscribeBadHandshakeOverTCP(t *testing.T) {
	srv, _ := newWSTestServer(t)
	registerDeviceViaHTTP(t, srv, "dev-1")
	target := "/v1/documents/doc/crdt/state/subscribe?deviceId=dev-1"

	for _, tc := range []struct {
		name    string
		headers []string
	}{
		{"wrong version", []string{
			"Connection: Upgrade", "Upgrade: websocket",
			"Sec-WebSocket-Key: " + newWSKey(), "Sec-WebSocket-Version: 8",
		}},
		{"bad key", []string{
			"Connection: Upgrade", "Upgrade: websocket",
			"Sec-WebSocket-Key: nope", "Sec-WebSocket-Version: 13",
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resp, conn := dialWSRaw(t, srv.URL, target, tc.headers...)
			defer conn.Close()
			if resp.StatusCode != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400", resp.StatusCode)
			}
		})
	}
}

// After the upgrade the current merged state is pushed first, byte-for-byte the
// state read endpoint's body.
func TestDocCRDTSubscribeInitialState(t *testing.T) {
	srv, _ := newWSTestServer(t)
	registerDeviceViaHTTP(t, srv, "dev-1")
	if code := postCRDTOps(t, srv, "doc", crdtCounterBody("dev-1", crdtCounterOp("a1", 5))); code != http.StatusOK {
		t.Fatalf("submit = %d", code)
	}
	stateBody := mustReadBody(t, srv, "/v1/documents/doc/crdt/state")

	conn, hs := dialWS(t, docCRDTSubscribeURL(srv, "doc", "dev-1"))
	if conn == nil {
		t.Fatalf("status = %d, want 101", hs.StatusCode)
	}
	defer conn.close()

	if raw := string(conn.readCRDTRaw()); raw != stateBody {
		t.Fatalf("frame %q != state read body %q", raw, stateBody)
	}
}

// A document with a change log but no CRDT operation stays silent until its
// first state appears.
func TestDocCRDTSubscribeSilentUntilFirstState(t *testing.T) {
	srv, _ := newWSTestServer(t)
	registerAndSeedDevice(t, srv, "dev-1", "doc", 2)

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

	if code := postCRDTOps(t, srv, "doc", crdtGSetBody("dev-1", crdtGSetOp("g1", "apple"))); code != http.StatusOK {
		t.Fatalf("submit = %d", code)
	}
	state := conn.readCRDTState()
	if state.Type != "gset" {
		t.Fatalf("type = %q, want gset", state.Type)
	}
	if got := crdtSetElements(t, state); len(got) != 1 || got[0] != "apple" {
		t.Fatalf("elements = %v, want [apple]", got)
	}
}

// Only commits that really change the merge push; idempotent repeats,
// equal-value new ops, rejected regressions and invalid batches push nothing.
func TestDocCRDTSubscribePushesOnlyRealChanges(t *testing.T) {
	srv, _ := newWSTestServer(t)
	registerDeviceViaHTTP(t, srv, "dev-1")

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

	if code := postCRDTOps(t, srv, "doc", crdtCounterBody("dev-1", crdtCounterOp("a1", 5))); code != http.StatusOK {
		t.Fatalf("idempotent = %d", code)
	}
	if code := postCRDTOps(t, srv, "doc", crdtCounterBody("dev-1", crdtCounterOp("a2", 5))); code != http.StatusOK {
		t.Fatalf("equal-value = %d", code)
	}
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

// Session-scoped CRDT commits share the document subscription: a session batch
// that moves the merge is pushed to the document-level subscriber.
func TestDocCRDTSubscribeGetsSessionCommits(t *testing.T) {
	srv, _ := newWSTestServer(t)
	registerDeviceViaHTTP(t, srv, "dev-1")
	if code := postHTTP(t, srv, "/v1/devices/dev-1/sessions", map[string]any{"sessionId": "sess"}); code != http.StatusOK {
		t.Fatalf("create session = %d", code)
	}

	conn, hs := dialWS(t, docCRDTSubscribeURL(srv, "doc", "dev-1"))
	if conn == nil {
		t.Fatalf("status = %d", hs.StatusCode)
	}
	defer conn.close()

	body := `{"type":"counter","ops":[{"id":"s1","value":3}]}`
	resp, err := srv.Client().Post(srv.URL+"/v1/sessions/sess/documents/doc/crdt/ops",
		"application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("session crdt post: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("session crdt = %d", resp.StatusCode)
	}
	if n := crdtCounterValue(t, conn.readCRDTState()); n != 3 {
		t.Fatalf("frame = %d, want 3", n)
	}
}

// Revocation after upgrade closes only the affected device's subscription with
// 4403; another device's subscription keeps streaming. A grant does not revive
// the closed connection, but a fresh connection gets the current state.
func TestDocCRDTSubscribeRevoke4403(t *testing.T) {
	srv, _ := newWSTestServer(t)
	registerDeviceViaHTTP(t, srv, "dev-1")
	registerDeviceViaHTTP(t, srv, "dev-2")
	if code := postCRDTOps(t, srv, "doc", crdtCounterBody("dev-1", crdtCounterOp("a1", 1))); code != http.StatusOK {
		t.Fatalf("submit = %d", code)
	}

	c1, hs := dialWS(t, docCRDTSubscribeURL(srv, "doc", "dev-1"))
	if c1 == nil {
		t.Fatalf("dev-1 upgrade = %d", hs.StatusCode)
	}
	defer c1.close()
	c2, hs := dialWS(t, docCRDTSubscribeURL(srv, "doc", "dev-2"))
	if c2 == nil {
		t.Fatalf("dev-2 upgrade = %d", hs.StatusCode)
	}
	defer c2.close()
	if n := crdtCounterValue(t, c1.readCRDTState()); n != 1 {
		t.Fatalf("dev-1 initial = %d", n)
	}
	// dev-2 connected after the first op too, so its initial frame is also 1.
	if n := crdtCounterValue(t, c2.readCRDTState()); n != 1 {
		t.Fatalf("dev-2 initial = %d", n)
	}

	if code := postHTTP(t, srv, "/v1/documents/doc/permissions", map[string]any{"deviceId": "dev-1", "action": "revoke"}); code != http.StatusOK {
		t.Fatalf("revoke = %d", code)
	}
	c1.setReadDeadline(2 * time.Second)
	if code := c1.readCloseCode(); code != 4403 {
		t.Fatalf("dev-1 close = %d, want 4403", code)
	}
	c1.clearReadDeadline()

	// Reconnect while revoked is a pre-upgrade 403.
	if bad, r := dialWS(t, docCRDTSubscribeURL(srv, "doc", "dev-1")); bad != nil || r.StatusCode != http.StatusForbidden {
		if bad != nil {
			bad.close()
		}
		t.Fatalf("reconnect while revoked = %d, want 403", r.StatusCode)
	}

	// dev-2 keeps streaming.
	if code := postCRDTOps(t, srv, "doc", crdtCounterBody("dev-2", crdtCounterOp("b1", 4))); code != http.StatusOK {
		t.Fatalf("dev-2 submit = %d", code)
	}
	if n := crdtCounterValue(t, c2.readCRDTState()); n != 5 {
		t.Fatalf("dev-2 frame = %d, want 5", n)
	}

	// Grant then a fresh connection immediately gets the current merge.
	if code := postHTTP(t, srv, "/v1/documents/doc/permissions", map[string]any{"deviceId": "dev-1", "action": "grant"}); code != http.StatusOK {
		t.Fatalf("grant = %d", code)
	}
	c3, hs := dialWS(t, docCRDTSubscribeURL(srv, "doc", "dev-1"))
	if c3 == nil {
		t.Fatalf("reconnect after grant = %d", hs.StatusCode)
	}
	defer c3.close()
	if n := crdtCounterValue(t, c3.readCRDTState()); n != 5 {
		t.Fatalf("fresh state = %d, want 5", n)
	}
}

// A revoke is sticky even if a grant commits before the close frame is read.
func TestDocCRDTSubscribeRevokeStickyDespiteGrant(t *testing.T) {
	srv, _ := newWSTestServer(t)
	registerDeviceViaHTTP(t, srv, "dev-1")

	conn, hs := dialWS(t, docCRDTSubscribeURL(srv, "doc", "dev-1"))
	if conn == nil {
		t.Fatalf("status = %d", hs.StatusCode)
	}
	defer conn.close()

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

// The termination signal ends the subscription with 1001 and state stays
// readable.
func TestDocCRDTSubscribeShutdown1001(t *testing.T) {
	srv, st := newWSTestServer(t)
	registerDeviceViaHTTP(t, srv, "dev-1")
	if code := postCRDTOps(t, srv, "doc", crdtCounterBody("dev-1", crdtCounterOp("a1", 7))); code != http.StatusOK {
		t.Fatalf("submit = %d", code)
	}

	conn, hs := dialWS(t, docCRDTSubscribeURL(srv, "doc", "dev-1"))
	if conn == nil {
		t.Fatalf("status = %d", hs.StatusCode)
	}
	defer conn.close()
	if n := crdtCounterValue(t, conn.readCRDTState()); n != 7 {
		t.Fatalf("initial = %d", n)
	}

	st.InterruptWaits()
	conn.setReadDeadline(2 * time.Second)
	if code := conn.readCloseCode(); code != 1001 {
		t.Fatalf("close = %d, want 1001", code)
	}
	conn.clearReadDeadline()

	state, err := st.GetCRDTState("doc")
	if err != nil {
		t.Fatal(err)
	}
	if state.Type != crdt.TypeCounter || string(state.Value) != "7" {
		t.Fatalf("state after signal = %+v", state)
	}
}

// Push-only: inbound frames are discarded (no op written) and ping gets a pong.
func TestDocCRDTSubscribePushOnly(t *testing.T) {
	srv, st := newWSTestServer(t)
	registerDeviceViaHTTP(t, srv, "dev-1")
	if code := postCRDTOps(t, srv, "doc", crdtCounterBody("dev-1", crdtCounterOp("a1", 1))); code != http.StatusOK {
		t.Fatalf("submit = %d", code)
	}

	conn, hs := dialWS(t, docCRDTSubscribeURL(srv, "doc", "dev-1"))
	if conn == nil {
		t.Fatalf("status = %d", hs.StatusCode)
	}
	defer conn.close()
	if n := crdtCounterValue(t, conn.readCRDTState()); n != 1 {
		t.Fatalf("initial = %d", n)
	}

	conn.sendText(`{"deviceId":"dev-1","type":"counter","ops":[{"id":"forged","value":99}]}`)
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
		if opcode == wsOpcodeText {
			t.Fatal("server pushed after a discarded client frame")
		}
	}
	conn.clearReadDeadline()
	if !sawPong {
		t.Fatal("no pong for the ping")
	}

	state, err := st.GetCRDTState("doc")
	if err != nil {
		t.Fatal(err)
	}
	if string(state.Value) != "1" {
		t.Fatalf("state = %s, want 1 (client frame applied)", state.Value)
	}
	results, err := st.SubmitCRDTOps("doc", crdt.TypeCounter, []crdt.Op{
		{ID: "forged", DeviceID: "dev-1", Value: json.RawMessage(`99`)},
	})
	if err != nil {
		t.Fatalf("forged id should be free: %v", err)
	}
	if !results[0].Created {
		t.Fatal("forged id already existed: a client frame was written")
	}
	if n := crdtCounterValue(t, conn.readCRDTState()); n != 99 {
		t.Fatalf("frame after real submit = %d, want 99", n)
	}
}

// CRDT pushes stay independent of the change log at document scope too.
func TestDocCRDTSubscribeIndependentOfChangeLog(t *testing.T) {
	srv, _ := newWSTestServer(t)
	registerAndSeedDevice(t, srv, "dev-1", "doc", 1)

	crdtConn, hs := dialWS(t, docCRDTSubscribeURL(srv, "doc", "dev-1"))
	if crdtConn == nil {
		t.Fatalf("crdt upgrade = %d", hs.StatusCode)
	}
	defer crdtConn.close()
	changeConn, hs := dialWS(t, docSubscribeURL(srv, "doc", "dev-1", "1"))
	if changeConn == nil {
		t.Fatalf("changes upgrade = %d", hs.StatusCode)
	}
	defer changeConn.close()

	if code := postCRDTOps(t, srv, "doc", crdtCounterBody("dev-1", crdtCounterOp("a1", 2))); code != http.StatusOK {
		t.Fatalf("submit = %d", code)
	}
	if n := crdtCounterValue(t, crdtConn.readCRDTState()); n != 2 {
		t.Fatalf("crdt frame = %d, want 2", n)
	}
	changeConn.setReadDeadline(300 * time.Millisecond)
	if _, _, _, ok := changeConn.readFrameMaybe(); ok {
		t.Fatal("a crdt op woke the changes subscription")
	}
	changeConn.clearReadDeadline()

	postDocChange(t, srv, "dev-1", "doc", "c2", map[string]any{"n": 2})
	if c := changeConn.readChange(); c.ID != "c2" || c.Cursor != 2 {
		t.Fatalf("change frame = %+v", c)
	}
	crdtConn.setReadDeadline(300 * time.Millisecond)
	if _, _, _, ok := crdtConn.readFrameMaybe(); ok {
		t.Fatal("a change commit woke the crdt subscription")
	}
	crdtConn.clearReadDeadline()
}

// Documents literally named "crdt" or "state" keep ordinary CRDT subscription
// behavior; the path guard treats the keyword only at its endpoint position.
func TestDocCRDTSubscribeKeywordIdentifiers(t *testing.T) {
	srv, _ := newWSTestServer(t)
	registerDeviceViaHTTP(t, srv, "dev-1")
	if code := postCRDTOps(t, srv, "state", crdtCounterBody("dev-1", crdtCounterOp("a1", 3))); code != http.StatusOK {
		t.Fatalf("submit to doc state = %d", code)
	}
	conn, hs := dialWS(t, docCRDTSubscribeURL(srv, "state", "dev-1"))
	if conn == nil {
		t.Fatalf("subscribe to doc state = %d", hs.StatusCode)
	}
	defer conn.close()
	if n := crdtCounterValue(t, conn.readCRDTState()); n != 3 {
		t.Fatalf("frame = %d, want 3", n)
	}
}
