package server

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicegogogogogo/local-first-sync-service/internal/store"
)

// crdtSubscribeURL builds the CRDT state subscription URL.
func crdtSubscribeURL(srv *httptest.Server, session, doc string) string {
	return srv.URL + "/v1/sessions/" + session + "/documents/" + doc + "/crdt/state/subscribe"
}

// readCRDTState reads the next complete text frame, validates the wire format
// (compact single-line JSON terminated by one newline, keys type then value)
// and decodes the state. Pongs are skipped; a close frame fails the test.
func (c *wsClient) readCRDTState() store.CRDTState {
	c.t.Helper()
	for {
		fin, opcode, payload := c.readFrame()
		switch opcode {
		case wsOpcodeText:
			if !fin {
				c.t.Fatal("server sent a fragmented text frame")
			}
			if len(payload) == 0 || payload[len(payload)-1] != '\n' {
				c.t.Fatalf("state frame is not newline-terminated: %q", payload)
			}
			line := string(payload[:len(payload)-1])
			if strings.ContainsAny(line, "\r\n") {
				c.t.Fatalf("state frame is not a single line: %q", payload)
			}
			if strings.Contains(line, ", ") || strings.Contains(line, ": ") {
				c.t.Fatalf("state frame is not compact JSON: %q", payload)
			}
			var state store.CRDTState
			if err := json.Unmarshal(payload, &state); err != nil {
				c.t.Fatalf("text frame is not a crdt state: %s (%v)", payload, err)
			}
			return state
		case wsOpcodePong:
			continue
		case wsOpcodeClose:
			c.t.Fatalf("unexpected close frame while reading state: code=%d", closeCode(payload))
		default:
			c.t.Fatalf("unexpected opcode %d while reading state", opcode)
		}
	}
}

// readCRDTRaw reads the raw payload bytes of the next text frame (newline
// included), pongs skipped.
func (c *wsClient) readCRDTRaw() []byte {
	c.t.Helper()
	for {
		fin, opcode, payload := c.readFrame()
		if opcode == wsOpcodePong {
			continue
		}
		if opcode != wsOpcodeText || !fin {
			c.t.Fatalf("unexpected opcode=%d fin=%v", opcode, fin)
		}
		return payload
	}
}

func crdtCounterOp(id string, value int) map[string]any {
	return map[string]any{"id": id, "value": value}
}

func crdtGSetOp(id string, elements ...string) map[string]any {
	return map[string]any{"id": id, "elements": elements}
}

func postCRDTOps(t *testing.T, srv *httptest.Server, doc string, body map[string]any) int {
	t.Helper()
	return postHTTP(t, srv, "/v1/documents/"+doc+"/crdt/ops", body)
}

func crdtCounterValue(t *testing.T, state store.CRDTState) int64 {
	t.Helper()
	var n int64
	if err := json.Unmarshal(state.Value, &n); err != nil {
		t.Fatalf("counter value %s is not an integer: %v", state.Value, err)
	}
	return n
}

func crdtSetElements(t *testing.T, state store.CRDTState) []string {
	t.Helper()
	var elements []string
	if err := json.Unmarshal(state.Value, &elements); err != nil {
		t.Fatalf("gset value %s is not a string array: %v", state.Value, err)
	}
	return elements
}

// All pre-upgrade failures are ordinary JSON errors: no 101, no redirect and
// no HTML, and nothing about the session or state is leaked.
func TestCRDTSubscribePreUpgradeValidation(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	w, _ := postJSON(t, h, "/v1/devices/dev-1/sessions", map[string]any{"sessionId": "sess"})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	// Give doc2 a revoked permission so the 403 branch is reachable.
	w, _ = postJSON(t, h, "/v1/documents/doc2/permissions", map[string]any{"deviceId": "dev-1", "action": "revoke"})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}

	base := "/v1/sessions/sess/documents/doc/crdt/state/subscribe"
	cases := []struct {
		name       string
		method     string
		target     string
		upgrade    bool
		wantStatus int
	}{
		{"no upgrade headers", http.MethodGet, base, false, http.StatusBadRequest},
		{"wrong method", http.MethodPost, base, false, http.StatusBadRequest},
		{"empty session segment", http.MethodGet, "/v1/sessions//documents/doc/crdt/state/subscribe", true, http.StatusBadRequest},
		{"empty document segment", http.MethodGet, "/v1/sessions/sess/documents//crdt/state/subscribe", true, http.StatusBadRequest},
		{"trailing slash", http.MethodGet, base + "/", true, http.StatusBadRequest},
		{"extra segment", http.MethodGet, base + "/extra", true, http.StatusBadRequest},
		{"missing state segment", http.MethodGet, "/v1/sessions/sess/documents/doc/crdt/subscribe", true, http.StatusBadRequest},
		{"missing subscribe segment", http.MethodGet, "/v1/sessions/sess/documents/doc/crdt/state", true, http.StatusBadRequest},
		{"wrong terminal word", http.MethodGet, "/v1/sessions/sess/documents/doc/crdt/statex/subscribe", true, http.StatusBadRequest},
		{"bare crdt namespace", http.MethodGet, "/v1/sessions/sess/documents/doc/crdt", true, http.StatusBadRequest},
		{"unknown session", http.MethodGet, "/v1/sessions/ghost/documents/doc/crdt/state/subscribe", true, http.StatusNotFound},
		{"revoked permission", http.MethodGet, "/v1/sessions/sess/documents/doc2/crdt/state/subscribe", true, http.StatusForbidden},
		// Handshake shape is checked before the session lookup.
		{"bad handshake before unknown session", http.MethodGet, "/v1/sessions/ghost/documents/doc/crdt/state/subscribe", false, http.StatusBadRequest},
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
			data, _ := io.ReadAll(resp.Body)
			if !strings.Contains(string(data), `"error"`) {
				t.Fatalf("body = %q, want JSON error", data)
			}
		})
	}
}

// After the upgrade the current merged counter state is pushed first, matching
// the state read endpoint's body byte for byte.
func TestCRDTSubscribeInitialCounterState(t *testing.T) {
	srv, _ := newWSTestServer(t)
	setupSession(t, srv, "dev-1", "sess", "doc", 0)
	if code := postCRDTOps(t, srv, "doc", crdtCounterBody("dev-1", crdtCounterOp("a1", 5))); code != http.StatusOK {
		t.Fatalf("submit = %d", code)
	}

	resp, _ := httpGet(t, srv, "/v1/documents/doc/crdt/state")
	if resp != http.StatusOK {
		t.Fatalf("state read = %d", resp)
	}
	stateBody := mustReadBody(t, srv, "/v1/documents/doc/crdt/state")

	conn, hs := dialWS(t, crdtSubscribeURL(srv, "sess", "doc"))
	if conn == nil {
		t.Fatalf("status = %d, want 101", hs.StatusCode)
	}
	defer conn.close()

	raw := conn.readCRDTRaw()
	if string(raw) != stateBody {
		t.Fatalf("frame %q != state read body %q", raw, stateBody)
	}
	var state store.CRDTState
	_ = json.Unmarshal(raw, &state)
	if state.Type != "counter" || crdtCounterValue(t, state) != 5 {
		t.Fatalf("initial state = %+v", state)
	}
}

// A document with a change log but no CRDT operation stays silent until its
// first state appears; the state read endpoint answers 404 in the meantime.
func TestCRDTSubscribeSilentUntilFirstState(t *testing.T) {
	srv, _ := newWSTestServer(t)
	setupSession(t, srv, "dev-1", "sess", "doc", 2)

	if code, _ := httpGet(t, srv, "/v1/documents/doc/crdt/state"); code != http.StatusNotFound {
		t.Fatalf("state before ops = %d, want 404", code)
	}

	conn, hs := dialWS(t, crdtSubscribeURL(srv, "sess", "doc"))
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

// Counter commits that advance the per-device maximum push immediately in
// commit order; idempotent repeats, equal-value new ops and rejected
// regressions change no state and push nothing.
func TestCRDTSubscribeCounterPushesOnlyRealChanges(t *testing.T) {
	srv, _ := newWSTestServer(t)
	setupSession(t, srv, "dev-1", "sess", "doc", 0)

	conn, hs := dialWS(t, crdtSubscribeURL(srv, "sess", "doc"))
	if conn == nil {
		t.Fatalf("status = %d", hs.StatusCode)
	}
	defer conn.close()

	// First ever state: 5.
	if code := postCRDTOps(t, srv, "doc", crdtCounterBody("dev-1", crdtCounterOp("a1", 5))); code != http.StatusOK {
		t.Fatalf("submit = %d", code)
	}
	if n := crdtCounterValue(t, conn.readCRDTState()); n != 5 {
		t.Fatalf("frame = %d, want 5", n)
	}

	// Idempotent repeat of the same id/value pushes nothing.
	if code := postCRDTOps(t, srv, "doc", crdtCounterBody("dev-1", crdtCounterOp("a1", 5))); code != http.StatusOK {
		t.Fatalf("idempotent resubmit = %d", code)
	}
	// A new id carrying an equal contribution is accepted but moves no maximum.
	if code := postCRDTOps(t, srv, "doc", crdtCounterBody("dev-1", crdtCounterOp("a2", 5))); code != http.StatusOK {
		t.Fatalf("equal-value submit = %d", code)
	}
	conn.setReadDeadline(300 * time.Millisecond)
	if _, _, _, ok := conn.readFrameMaybe(); ok {
		t.Fatal("an idempotent/equal-value commit pushed a frame")
	}
	conn.clearReadDeadline()

	// A regression is a 409 and pushes nothing.
	if code := postCRDTOps(t, srv, "doc", crdtCounterBody("dev-1", crdtCounterOp("a3", 4))); code != http.StatusConflict {
		t.Fatalf("regression = %d, want 409", code)
	}
	// A wholly invalid batch is a 400 and pushes nothing.
	if code := postCRDTOps(t, srv, "doc", map[string]any{"deviceId": "dev-1", "type": "counter", "ops": []any{}}); code != http.StatusBadRequest {
		t.Fatalf("empty batch = %d, want 400", code)
	}
	conn.setReadDeadline(300 * time.Millisecond)
	if _, _, _, ok := conn.readFrameMaybe(); ok {
		t.Fatal("a rejected commit pushed a frame")
	}
	conn.clearReadDeadline()

	// An advancing contribution pushes the new sum.
	if code := postCRDTOps(t, srv, "doc", crdtCounterBody("dev-1", crdtCounterOp("a4", 8))); code != http.StatusOK {
		t.Fatalf("advance = %d", code)
	}
	if n := crdtCounterValue(t, conn.readCRDTState()); n != 8 {
		t.Fatalf("frame = %d, want 8", n)
	}

	// A second device adds its maximum to the sum.
	registerDeviceViaHTTP(t, srv, "dev-2")
	if code := postCRDTOps(t, srv, "doc", crdtCounterBody("dev-2", crdtCounterOp("b1", 3))); code != http.StatusOK {
		t.Fatalf("dev-2 submit = %d", code)
	}
	if n := crdtCounterValue(t, conn.readCRDTState()); n != 11 {
		t.Fatalf("frame = %d, want 11", n)
	}
}

// Re-adding an existing gset element creates the op but does not move the
// union, so no frame is sent; genuinely new elements push the sorted union.
func TestCRDTSubscribeGSetDuplicateElementIsSilent(t *testing.T) {
	srv, _ := newWSTestServer(t)
	setupSession(t, srv, "dev-1", "sess", "doc", 0)

	conn, hs := dialWS(t, crdtSubscribeURL(srv, "sess", "doc"))
	if conn == nil {
		t.Fatalf("status = %d", hs.StatusCode)
	}
	defer conn.close()

	if code := postCRDTOps(t, srv, "doc", crdtGSetBody("dev-1", crdtGSetOp("g1", "banana", "apple"))); code != http.StatusOK {
		t.Fatalf("submit = %d", code)
	}
	state := conn.readCRDTState()
	if got := crdtSetElements(t, state); len(got) != 2 || got[0] != "apple" || got[1] != "banana" {
		t.Fatalf("elements = %v, want [apple banana]", got)
	}

	// New op id, only already-present elements: created=true, no state change.
	if code := postCRDTOps(t, srv, "doc", crdtGSetBody("dev-1", crdtGSetOp("g2", "apple"))); code != http.StatusOK {
		t.Fatalf("duplicate-element submit = %d", code)
	}
	conn.setReadDeadline(300 * time.Millisecond)
	if _, _, _, ok := conn.readFrameMaybe(); ok {
		t.Fatal("re-adding an existing element pushed a frame")
	}
	conn.clearReadDeadline()

	// A mixed batch (one old, one new) pushes once with the grown union.
	if code := postCRDTOps(t, srv, "doc", crdtGSetBody("dev-1", crdtGSetOp("g3", "apple", "cherry"))); code != http.StatusOK {
		t.Fatalf("mixed submit = %d", code)
	}
	state = conn.readCRDTState()
	if got := crdtSetElements(t, state); len(got) != 3 || got[0] != "apple" || got[1] != "banana" || got[2] != "cherry" {
		t.Fatalf("elements = %v, want [apple banana cherry]", got)
	}
}

// Every live subscriber on the document gets each change in commit order, and
// the sequence is strictly increasing; subscribers on another document get
// nothing.
func TestCRDTSubscribeFanoutAndCommitOrder(t *testing.T) {
	srv, _ := newWSTestServer(t)
	setupSession(t, srv, "dev-1", "sess-1", "doc", 0)

	conns := make([]*wsClient, 3)
	for i := range conns {
		c, hs := dialWS(t, crdtSubscribeURL(srv, "sess-1", "doc"))
		if c == nil {
			t.Fatalf("subscriber %d status = %d", i, hs.StatusCode)
		}
		conns[i] = c
	}
	defer func() {
		for _, c := range conns {
			c.close()
		}
	}()

	// Many devices each advance their own maximum concurrently; each commit
	// moves the sum, so every subscriber observes a strictly increasing
	// sequence ending at the total.
	const devices = 8
	const perDevice = 5
	var wg sync.WaitGroup
	start := make(chan struct{})
	for d := 0; d < devices; d++ {
		device := "dev-" + itoa(d)
		// dev-1 is already registered (owns sess-1); register the rest.
		if device != "dev-1" {
			registerDeviceViaHTTP(t, srv, device)
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for v := 1; v <= perDevice; v++ {
				code := postCRDTOps(t, srv, "doc", crdtCounterBody(device, crdtCounterOp(device+"-"+itoa(v), v)))
				if code != http.StatusOK {
					t.Errorf("submit %s %d = %d", device, v, code)
					return
				}
			}
		}()
	}
	close(start)
	wg.Wait()

	wantTotal := int64(devices * perDevice)
	for i, c := range conns {
		var last int64
		count := 0
		c.setReadDeadline(5 * time.Second)
		for {
			state, ok := func() (store.CRDTState, bool) {
				fin, opcode, payload, ok := c.readFrameMaybe()
				if !ok {
					return store.CRDTState{}, false
				}
				if opcode != wsOpcodeText || !fin {
					t.Fatalf("subscriber %d unexpected opcode %d", i, opcode)
				}
				var st store.CRDTState
				if err := json.Unmarshal(payload, &st); err != nil {
					t.Fatalf("subscriber %d bad frame %s: %v", i, payload, err)
				}
				return st, true
			}()
			if !ok {
				break
			}
			n := crdtCounterValue(t, state)
			if n <= last {
				t.Fatalf("subscriber %d non-increasing sequence: %d after %d", i, n, last)
			}
			last = n
			count++
			if n == wantTotal {
				break
			}
		}
		c.clearReadDeadline()
		if last != wantTotal {
			t.Fatalf("subscriber %d ended at %d, want %d (frames=%d)", i, last, wantTotal, count)
		}
	}
}

// Revocation after the upgrade ends the subscription with 4403; a re-grant
// does not revive the dead connection, but a fresh subscription resumes with
// the current merged state.
func TestCRDTSubscribeRevokedAfterUpgradeCloses4403(t *testing.T) {
	srv, _ := newWSTestServer(t)
	setupSession(t, srv, "dev-1", "sess", "doc", 0)
	if code := postCRDTOps(t, srv, "doc", crdtCounterBody("dev-1", crdtCounterOp("a1", 1))); code != http.StatusOK {
		t.Fatalf("submit = %d", code)
	}

	conn, hs := dialWS(t, crdtSubscribeURL(srv, "sess", "doc"))
	if conn == nil {
		t.Fatalf("status = %d", hs.StatusCode)
	}
	defer conn.close()
	if n := crdtCounterValue(t, conn.readCRDTState()); n != 1 {
		t.Fatalf("initial = %d, want 1", n)
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

	// A reconnect while revoked is a pre-upgrade 403.
	conn2, resp2 := dialWS(t, crdtSubscribeURL(srv, "sess", "doc"))
	if conn2 != nil || resp2.StatusCode != http.StatusForbidden {
		if conn2 != nil {
			conn2.close()
		}
		t.Fatalf("reconnect while revoked = %d, want 403", resp2.StatusCode)
	}

	// Re-grant does not revive the old connection (it already ended), and a
	// new subscription immediately gets the current merged state.
	if code := postCRDTOps(t, srv, "doc", crdtCounterBody("dev-1", crdtCounterOp("a2", 4))); code == http.StatusOK {
		t.Fatal("a revoked device submitted a crdt op")
	}
	if code := postHTTP(t, srv, "/v1/documents/doc/permissions", map[string]any{
		"deviceId": "dev-1", "action": "grant",
	}); code != http.StatusOK {
		t.Fatalf("grant = %d", code)
	}
	if code := postCRDTOps(t, srv, "doc", crdtCounterBody("dev-1", crdtCounterOp("a2", 4))); code != http.StatusOK {
		t.Fatalf("post-grant submit = %d", code)
	}
	conn3, resp3 := dialWS(t, crdtSubscribeURL(srv, "sess", "doc"))
	if conn3 == nil {
		t.Fatalf("reconnect after grant = %d", resp3.StatusCode)
	}
	defer conn3.close()
	if n := crdtCounterValue(t, conn3.readCRDTState()); n != 4 {
		t.Fatalf("fresh state after grant = %d, want 4", n)
	}
}

// Revoke is sticky on a live subscription even if a grant races immediately.
func TestCRDTSubscribeRevokeStickyDespiteGrant(t *testing.T) {
	srv, _ := newWSTestServer(t)
	setupSession(t, srv, "dev-1", "sess", "doc", 0)

	conn, hs := dialWS(t, crdtSubscribeURL(srv, "sess", "doc"))
	if conn == nil {
		t.Fatalf("status = %d", hs.StatusCode)
	}
	defer conn.close()
	conn.setReadDeadline(300 * time.Millisecond)
	if _, _, _, ok := conn.readFrameMaybe(); ok {
		t.Fatal("unexpected frame before any state")
	}
	conn.clearReadDeadline()

	if code := postHTTP(t, srv, "/v1/documents/doc/permissions", map[string]any{
		"deviceId": "dev-1", "action": "revoke",
	}); code != http.StatusOK {
		t.Fatalf("revoke = %d", code)
	}
	if code := postHTTP(t, srv, "/v1/documents/doc/permissions", map[string]any{
		"deviceId": "dev-1", "action": "grant",
	}); code != http.StatusOK {
		t.Fatalf("grant = %d", code)
	}
	conn.setReadDeadline(2 * time.Second)
	if code := conn.readCloseCode(); code != 4403 {
		t.Fatalf("close code = %d, want sticky 4403 despite grant", code)
	}
	conn.clearReadDeadline()
}

// The termination signal ends the subscription with 1001 and committed state
// stays readable.
func TestCRDTSubscribeShutdownCloses1001(t *testing.T) {
	srv, st := newWSTestServer(t)
	setupSession(t, srv, "dev-1", "sess", "doc", 0)
	if code := postCRDTOps(t, srv, "doc", crdtCounterBody("dev-1", crdtCounterOp("a1", 7))); code != http.StatusOK {
		t.Fatalf("submit = %d", code)
	}

	conn, hs := dialWS(t, crdtSubscribeURL(srv, "sess", "doc"))
	if conn == nil {
		t.Fatalf("status = %d", hs.StatusCode)
	}
	defer conn.close()
	if n := crdtCounterValue(t, conn.readCRDTState()); n != 7 {
		t.Fatalf("initial = %d, want 7", n)
	}

	st.InterruptWaits()
	conn.setReadDeadline(2 * time.Second)
	if code := conn.readCloseCode(); code != 1001 {
		t.Fatalf("close code = %d, want 1001", code)
	}
	conn.clearReadDeadline()

	state, err := st.GetCRDTState("doc")
	if err != nil {
		t.Fatalf("state after signal: %v", err)
	}
	if state.Type != "counter" || string(state.Value) != "7" {
		t.Fatalf("committed state after signal = %+v", state)
	}
}

// The connection is push-only: inbound data frames are discarded and cannot
// submit CRDT operations; ping is answered with pong and the stream continues.
func TestCRDTSubscribePushOnlyAndPingPong(t *testing.T) {
	srv, st := newWSTestServer(t)
	setupSession(t, srv, "dev-1", "sess", "doc", 0)
	if code := postCRDTOps(t, srv, "doc", crdtCounterBody("dev-1", crdtCounterOp("a1", 1))); code != http.StatusOK {
		t.Fatalf("submit = %d", code)
	}

	conn, hs := dialWS(t, crdtSubscribeURL(srv, "sess", "doc"))
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
			t.Fatal("the server pushed a state after a discarded client frame")
		}
	}
	conn.clearReadDeadline()
	if !sawPong {
		t.Fatal("no pong received for the ping")
	}

	// The forged op never reached the CRDT tables: value still 1, op id free.
	state, err := st.GetCRDTState("doc")
	if err != nil {
		t.Fatal(err)
	}
	if string(state.Value) != "1" {
		t.Fatalf("state = %s, want 1 (client frame was applied)", state.Value)
	}
	results, err := st.SubmitCRDTOps("doc", store.CRDTTypeCounter, []store.CRDTOp{
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

// CRDT pushes use neither the change-log cursor nor the changes subscription,
// and a CRDT state subscription receives no change-log commits.
func TestCRDTSubscribeIndependentOfChangeLog(t *testing.T) {
	srv, _ := newWSTestServer(t)
	setupSession(t, srv, "dev-1", "sess", "doc", 1)

	crdtConn, hs := dialWS(t, crdtSubscribeURL(srv, "sess", "doc"))
	if crdtConn == nil {
		t.Fatalf("crdt subscribe status = %d", hs.StatusCode)
	}
	defer crdtConn.close()
	changeConn, hs2 := dialWS(t, subscribeURL(srv, "sess", "doc", "1"))
	if changeConn == nil {
		t.Fatalf("changes subscribe status = %d", hs2.StatusCode)
	}
	defer changeConn.close()

	// A CRDT op reaches only the state subscription.
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

	// A change commit reaches only the changes subscription.
	postDocChange(t, srv, "dev-1", "doc", "c2", map[string]any{"n": 2})
	if c := changeConn.readChange(); c.ID != "c2" || c.Cursor != 2 {
		t.Fatalf("change frame = %+v, want c2/2", c)
	}
	crdtConn.setReadDeadline(300 * time.Millisecond)
	if _, _, _, ok := crdtConn.readFrameMaybe(); ok {
		t.Fatal("a change commit woke the crdt state subscription")
	}
	crdtConn.clearReadDeadline()
}

// Identifiers literally named after the endpoint keywords keep ordinary
// behavior: a document named "crdt" and one named "state" still subscribe.
func TestCRDTSubscribeKeywordIdentifiers(t *testing.T) {
	srv, _ := newWSTestServer(t)
	setupSession(t, srv, "dev-1", "sess", "crdt", 0)
	if code := postCRDTOps(t, srv, "crdt", crdtCounterBody("dev-1", crdtCounterOp("a1", 3))); code != http.StatusOK {
		t.Fatalf("submit to doc crdt = %d", code)
	}
	conn, hs := dialWS(t, crdtSubscribeURL(srv, "sess", "crdt"))
	if conn == nil {
		t.Fatalf("subscribe to doc crdt status = %d", hs.StatusCode)
	}
	defer conn.close()
	if n := crdtCounterValue(t, conn.readCRDTState()); n != 3 {
		t.Fatalf("frame = %d, want 3", n)
	}
}

// mustReadBody performs a GET through the test server and returns the raw body.
func mustReadBody(t *testing.T, srv *httptest.Server, path string) string {
	t.Helper()
	resp, err := srv.Client().Get(srv.URL + path)
	if err != nil {
		t.Fatalf("GET %s: %v", path, err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

// registerDeviceViaHTTP registers a device through the public HTTP surface.
func registerDeviceViaHTTP(t *testing.T, srv *httptest.Server, device string) {
	t.Helper()
	if code := postHTTP(t, srv, "/v1/devices", map[string]any{"deviceId": device}); code != http.StatusOK {
		t.Fatalf("register %s = %d", device, code)
	}
}
