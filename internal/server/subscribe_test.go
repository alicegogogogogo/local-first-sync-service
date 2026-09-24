package server

import (
	"bufio"
	"crypto/rand"
	"crypto/sha1"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

// wsConn is a minimal RFC 6455 client used to exercise the subscription
// endpoint over a real TCP connection (so handshake status/headers and close
// codes are observable, which httptest's in-process recorder cannot do).
type wsConn struct {
	conn net.Conn
	br   *bufio.Reader
}

// dialWS performs the opening handshake against a ws/http URL. On a non-101
// response it returns a nil client together with the HTTP status and body, so
// the pre-upgrade 400/403/404 JSON contract can be asserted.
func dialWS(t *testing.T, rawURL string) (*wsConn, int, map[string]any) {
	t.Helper()
	u, err := url.Parse(rawURL)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := net.DialTimeout("tcp", u.Host, 2*time.Second)
	if err != nil {
		t.Fatalf("dial %s: %v", u.Host, err)
	}
	keyBuf := make([]byte, 16)
	if _, err := rand.Read(keyBuf); err != nil {
		t.Fatal(err)
	}
	key := base64.StdEncoding.EncodeToString(keyBuf)
	fmt.Fprintf(conn, "GET %s HTTP/1.1\r\nHost: %s\r\nUpgrade: websocket\r\nConnection: keep-alive, Upgrade\r\nSec-WebSocket-Key: %s\r\nSec-WebSocket-Version: 13\r\n\r\n",
		u.RequestURI(), u.Host, key)

	br := bufio.NewReader(conn)
	resp, err := http.ReadResponse(br, nil)
	if err != nil {
		_ = conn.Close()
		t.Fatalf("read upgrade response: %v", err)
	}
	if resp.StatusCode != http.StatusSwitchingProtocols {
		raw, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		_ = conn.Close()
		var body map[string]any
		_ = json.Unmarshal(raw, &body)
		return nil, resp.StatusCode, body
	}

	wantAccept := func() string {
		sum := sha1.Sum([]byte(key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"))
		return base64.StdEncoding.EncodeToString(sum[:])
	}()
	if got := resp.Header.Get("Sec-WebSocket-Accept"); got != wantAccept {
		_ = conn.Close()
		t.Fatalf("Sec-WebSocket-Accept = %q, want %q", got, wantAccept)
	}
	c := &wsConn{conn: conn, br: br}
	t.Cleanup(func() { _ = conn.Close() })
	return c, http.StatusSwitchingProtocols, nil
}

// setReadDeadline bounds the next read so a missing push fails the test
// instead of hanging it.
func (c *wsConn) setReadDeadline(d time.Duration) {
	_ = c.conn.SetReadDeadline(time.Now().Add(d))
}

// readFrame reads one unmasked server frame, returning opcode and payload.
func (c *wsConn) readFrame(t *testing.T) (byte, []byte) {
	t.Helper()
	op, payload, err := c.readFrameErr()
	if err != nil {
		t.Fatalf("read frame: %v", err)
	}
	return op, payload
}

// readFrameErr is the error-returning core of readFrame.
func (c *wsConn) readFrameErr() (byte, []byte, error) {
	var head [2]byte
	if _, err := io.ReadFull(c.br, head[:]); err != nil {
		return 0, nil, err
	}
	opcode := head[0] & 0x0F
	n := int64(head[1] & 0x7F)
	if head[1]&0x80 != 0 {
		return 0, nil, fmt.Errorf("server frame was masked")
	}
	switch n {
	case 126:
		var ext [2]byte
		if _, err := io.ReadFull(c.br, ext[:]); err != nil {
			return 0, nil, err
		}
		n = int64(binary.BigEndian.Uint16(ext[:]))
	case 127:
		var ext [8]byte
		if _, err := io.ReadFull(c.br, ext[:]); err != nil {
			return 0, nil, err
		}
		n = int64(binary.BigEndian.Uint64(ext[:]))
	}
	payload := make([]byte, n)
	if n > 0 {
		if _, err := io.ReadFull(c.br, payload); err != nil {
			return 0, nil, err
		}
	}
	return opcode, payload, nil
}

// readChange reads one text frame and decodes it as a change record.
func (c *wsConn) readChange(t *testing.T) map[string]any {
	t.Helper()
	op, payload := c.readFrame(t)
	if op != 0x1 {
		t.Fatalf("opcode = %d, want text (1); payload = %q", op, payload)
	}
	var rec map[string]any
	if err := json.Unmarshal(payload, &rec); err != nil {
		t.Fatalf("change frame is not JSON: %v (%q)", err, payload)
	}
	return rec
}

// readFrameOptional reads a frame if one arrives within the deadline set by
// setReadDeadline. It returns opcode 0 with nil payload when no frame is
// available (timeout/EOF), used to assert that no further push happens.
func (c *wsConn) readFrameOptional(t *testing.T) (byte, []byte) {
	t.Helper()
	op, payload, err := c.readFrameErr()
	if err != nil {
		return 0, nil
	}
	return op, payload
}

// readCloseCode reads a close frame and returns its code.
func (c *wsConn) readCloseCode(t *testing.T) uint16 {
	t.Helper()
	op, payload := c.readFrame(t)
	if op != 0x8 {
		t.Fatalf("opcode = %d, want close (8); payload = %q", op, payload)
	}
	if len(payload) < 2 {
		t.Fatal("close frame carried no code")
	}
	return binary.BigEndian.Uint16(payload[:2])
}

// sendMasked sends one final masked client frame.
func (c *wsConn) sendMasked(t *testing.T, opcode byte, payload []byte) {
	t.Helper()
	mask := make([]byte, 4)
	if _, err := rand.Read(mask); err != nil {
		t.Fatal(err)
	}
	masked := make([]byte, len(payload))
	for i := range payload {
		masked[i] = payload[i] ^ mask[i%4]
	}
	var buf []byte
	buf = append(buf, 0x80|opcode, 0x80)
	switch {
	case len(payload) <= 125:
		buf[1] |= byte(len(payload))
	case len(payload) <= 0xFFFF:
		buf[1] |= 126
		var ext [2]byte
		binary.BigEndian.PutUint16(ext[:], uint16(len(payload)))
		buf = append(buf, ext[:]...)
	default:
		buf[1] |= 127
		var ext [8]byte
		binary.BigEndian.PutUint64(ext[:], uint64(len(payload)))
		buf = append(buf, ext[:]...)
	}
	buf = append(buf, mask...)
	buf = append(buf, masked...)
	if _, err := c.conn.Write(buf); err != nil {
		t.Fatalf("write frame: %v", err)
	}
}

// setupSession registers deviceID and creates sessionID owned by it.
func setupSession(t *testing.T, h http.Handler, deviceID, sessionID string) {
	t.Helper()
	if w, _ := postJSON(t, h, "/v1/devices", map[string]string{"deviceId": deviceID}); w.Code != http.StatusOK {
		t.Fatalf("register device: %d %s", w.Code, w.Body.String())
	}
	if w, _ := postJSON(t, h, "/v1/devices/"+deviceID+"/sessions",
		map[string]string{"sessionId": sessionID}); w.Code != http.StatusOK {
		t.Fatalf("create session: %d %s", w.Code, w.Body.String())
	}
}

func subscribeURL(srv *httptest.Server, sessionID, documentID, after string) string {
	return strings.Replace(srv.URL, "http://", "http://", 1) +
		"/v1/sessions/" + sessionID + "/documents/" + documentID + "/subscribe?after=" + after
}

func TestSubscribeBackfillThenLive(t *testing.T) {
	h, s := newTestHandler(t)
	srv := httptest.NewServer(h)
	defer srv.Close()
	setupSession(t, h, "dev", "sess")
	seedDoc(t, h, "doc", 2)

	c, status, _ := dialWS(t, subscribeURL(srv, "sess", "doc", "0"))
	if status != http.StatusSwitchingProtocols {
		t.Fatalf("status = %d, want 101", status)
	}
	c.setReadDeadline(2 * time.Second)

	// Backfill: rows after cursor 0, ascending, same shape as GET.
	r1 := c.readChange(t)
	if r1["id"] != "c1" || r1["deviceId"] != "dev" || r1["cursor"].(float64) != 1 {
		t.Fatalf("r1 = %v", r1)
	}
	if r1["payload"].(map[string]any)["n"].(float64) != 1 {
		t.Fatalf("r1 payload = %v", r1["payload"])
	}
	r2 := c.readChange(t)
	if r2["id"] != "c2" || r2["cursor"].(float64) != 2 {
		t.Fatalf("r2 = %v", r2)
	}

	// Live push after backfill completes.
	postJSON(t, h, "/v1/documents/doc/changes", map[string]any{
		"deviceId": "dev",
		"changes":  []any{map[string]any{"id": "c3", "payload": map[string]any{"n": 3}}},
	})
	r3 := c.readChange(t)
	if r3["id"] != "c3" || r3["cursor"].(float64) != 3 || r3["deviceId"] != "dev" {
		t.Fatalf("r3 = %v", r3)
	}

	// Pushing created no change and moved no cursor of its own: the read log
	// still ends at 3.
	w, body := doRequest(t, h, http.MethodGet, "/v1/documents/doc/changes?after=2")
	if w.Code != 200 || body["nextCursor"].(float64) != 3 || len(body["changes"].([]any)) != 1 {
		t.Fatalf("push altered the log: %d %v", w.Code, body)
	}
	_ = s
}

func TestSubscribeStartsAtArbitraryCursor(t *testing.T) {
	h, _ := newTestHandler(t)
	srv := httptest.NewServer(h)
	defer srv.Close()
	setupSession(t, h, "dev", "sess")
	seedDoc(t, h, "doc", 3)

	c, status, _ := dialWS(t, subscribeURL(srv, "sess", "doc", "2"))
	if status != 101 {
		t.Fatalf("status = %d", status)
	}
	c.setReadDeadline(2 * time.Second)
	if r := c.readChange(t); r["id"] != "c3" || r["cursor"].(float64) != 3 {
		t.Fatalf("first = %v, want c3@3", r)
	}

	postJSON(t, h, "/v1/documents/doc/changes", map[string]any{
		"deviceId": "dev",
		"changes":  []any{map[string]any{"id": "c4", "payload": map[string]any{"n": 4}}},
	})
	if r := c.readChange(t); r["id"] != "c4" || r["cursor"].(float64) != 4 {
		t.Fatalf("live = %v, want c4@4", r)
	}
}

// Reconnecting at any observed cursor resumes exactly there with no gap or
// duplicate.
func TestSubscribeReconnectResumes(t *testing.T) {
	h, _ := newTestHandler(t)
	srv := httptest.NewServer(h)
	defer srv.Close()
	setupSession(t, h, "dev", "sess")
	seedDoc(t, h, "doc", 4)

	first, status, _ := dialWS(t, subscribeURL(srv, "sess", "doc", "0"))
	if status != 101 {
		t.Fatalf("status = %d", status)
	}
	first.setReadDeadline(2 * time.Second)
	first.readChange(t) // c1
	if r := first.readChange(t); r["cursor"].(float64) != 2 {
		t.Fatalf("second = %v", r)
	}

	// Reconnect from observed cursor 2: must see exactly c3, c4.
	second, status, _ := dialWS(t, subscribeURL(srv, "sess", "doc", "2"))
	if status != 101 {
		t.Fatalf("reconnect status = %d", status)
	}
	second.setReadDeadline(2 * time.Second)
	if r := second.readChange(t); r["id"] != "c3" || r["cursor"].(float64) != 3 {
		t.Fatalf("reconnect first = %v", r)
	}
	if r := second.readChange(t); r["id"] != "c4" || r["cursor"].(float64) != 4 {
		t.Fatalf("reconnect second = %v", r)
	}
}

// An unknown document still upgrades (it is not a session/permission error);
// the first commit to it is then pushed.
func TestSubscribeUnknownDocumentPushesFirstChange(t *testing.T) {
	h, _ := newTestHandler(t)
	srv := httptest.NewServer(h)
	defer srv.Close()
	setupSession(t, h, "dev", "sess")

	c, status, _ := dialWS(t, subscribeURL(srv, "sess", "ghost", "0"))
	if status != 101 {
		t.Fatalf("status = %d", status)
	}
	c.setReadDeadline(2 * time.Second)
	postJSON(t, h, "/v1/documents/ghost/changes", map[string]any{
		"deviceId": "dev",
		"changes":  []any{map[string]any{"id": "g1", "payload": map[string]any{"n": 1}}},
	})
	if r := c.readChange(t); r["id"] != "g1" || r["cursor"].(float64) != 1 {
		t.Fatalf("first ghost change = %v", r)
	}
}

// Every change-producing path (merge, restore, replay) pushes immediately.
func TestSubscribePushesMergeRestoreReplay(t *testing.T) {
	h, _ := newTestHandler(t)
	srv := httptest.NewServer(h)
	defer srv.Close()
	setupSession(t, h, "dev", "sess")

	c, status, _ := dialWS(t, subscribeURL(srv, "sess", "doc", "0"))
	if status != 101 {
		t.Fatalf("status = %d", status)
	}
	c.setReadDeadline(2 * time.Second)

	// merge: first change on an unknown doc with baseCursor 0 -> cursor 1.
	postJSON(t, h, "/v1/documents/doc/merge", map[string]any{
		"deviceId": "dev", "baseCursor": 0,
		"change": map[string]any{"id": "m1", "payload": map[string]any{"a": 1}},
	})
	if r := c.readChange(t); r["id"] != "m1" || r["cursor"].(float64) != 1 {
		t.Fatalf("merge push = %v", r)
	}

	// snapshot + restore -> cursor 2.
	postJSON(t, h, "/v1/documents/doc/snapshots", map[string]any{"cursor": 1, "state": map[string]any{"a": 1}})
	postJSON(t, h, "/v1/documents/doc/restore", map[string]any{
		"deviceId": "dev", "changeId": "rs1", "snapshotCursor": 1,
	})
	if r := c.readChange(t); r["id"] != "rs1" || r["cursor"].(float64) != 2 {
		t.Fatalf("restore push = %v", r)
	}

	// replay -> cursor 3.
	postJSON(t, h, "/v1/documents/doc/replay", map[string]any{
		"deviceId":   "dev",
		"operations": []any{map[string]any{"id": "rp1", "payload": map[string]any{"b": 2}}},
	})
	if r := c.readChange(t); r["id"] != "rp1" || r["cursor"].(float64) != 3 {
		t.Fatalf("replay push = %v", r)
	}
}

// A large backlog is delivered page after page in ascending order, matching
// paginated reads, before going live.
func TestSubscribeLargeBackfillInOrder(t *testing.T) {
	h, _ := newTestHandler(t)
	srv := httptest.NewServer(h)
	defer srv.Close()
	setupSession(t, h, "dev", "sess")

	const n = 2500
	batch := make([]any, n)
	for i := range batch {
		batch[i] = map[string]any{"id": fmt.Sprintf("c%04d", i+1), "payload": map[string]any{"i": i + 1}}
	}
	if w, _ := postJSON(t, h, "/v1/documents/doc/changes", map[string]any{"deviceId": "dev", "changes": batch}); w.Code != 200 {
		t.Fatalf("seed batch: %d %s", w.Code, w.Body.String())
	}

	c, status, _ := dialWS(t, subscribeURL(srv, "sess", "doc", "0"))
	if status != 101 {
		t.Fatalf("status = %d", status)
	}
	c.setReadDeadline(5 * time.Second)
	for want := int64(1); want <= n; want++ {
		r := c.readChange(t)
		if r["cursor"].(float64) != float64(want) {
			t.Fatalf("at want %d got %v (id=%v)", want, r["cursor"], r["id"])
		}
	}
}

func TestSubscribeRejectsBadCursorBeforeUpgrade(t *testing.T) {
	h, _ := newTestHandler(t)
	srv := httptest.NewServer(h)
	defer srv.Close()
	setupSession(t, h, "dev", "sess")
	seedDoc(t, h, "doc", 1)

	for _, after := range []string{"-1", "1.5", "abc", "1e2"} {
		u := srv.URL + "/v1/sessions/sess/documents/doc/subscribe?after=" + after
		c, code, body := dialWS(t, u)
		if c != nil {
			t.Fatalf("after=%s upgraded", after)
		}
		if code != http.StatusBadRequest {
			t.Fatalf("after=%s code = %d, want 400", after, code)
		}
		if body["error"] == nil {
			t.Fatalf("after=%s body = %v, want JSON error", after, body)
		}
	}

	// A missing after defaults to 0 and is valid (upgrades); an empty after=
	// likewise defaults to 0.
	if c, code, _ := dialWS(t, srv.URL+"/v1/sessions/sess/documents/doc/subscribe"); c == nil || code != 101 {
		t.Fatalf("missing after: code=%d", code)
	}
}

func TestSubscribeRejectsMissingHandshake(t *testing.T) {
	h, _ := newTestHandler(t)
	srv := httptest.NewServer(h)
	defer srv.Close()
	setupSession(t, h, "dev", "sess")

	// A plain GET with no Upgrade headers is a 400 JSON error, not a 101 or
	// any HTML/redirect.
	resp, err := http.Get(srv.URL + "/v1/sessions/sess/documents/doc/subscribe?after=0")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "application/json" {
		t.Fatalf("content-type = %q", ct)
	}
	raw, _ := io.ReadAll(resp.Body)
	var body map[string]any
	if err := json.Unmarshal(raw, &body); err != nil || body["error"] == nil {
		t.Fatalf("body = %q", raw)
	}
}

func TestSubscribeMethodAndPathShape(t *testing.T) {
	h, _ := newTestHandler(t)
	srv := httptest.NewServer(h)
	defer srv.Close()
	setupSession(t, h, "dev", "sess")
	seedDoc(t, h, "doc", 1)

	cases := []struct {
		name   string
		method string
		path   string
	}{
		{"POST method", http.MethodPost, "/v1/sessions/sess/documents/doc/subscribe"},
		{"PUT method", http.MethodPut, "/v1/sessions/sess/documents/doc/subscribe"},
		{"empty session", http.MethodGet, "/v1/sessions//documents/doc/subscribe"},
		{"empty document", http.MethodGet, "/v1/sessions/sess/documents//subscribe"},
		{"trailing slash", http.MethodGet, "/v1/sessions/sess/documents/doc/subscribe/"},
		{"extra segment", http.MethodGet, "/v1/sessions/sess/documents/doc/subscribe/extra"},
		{"missing documents segment", http.MethodGet, "/v1/sessions/sess/doc/subscribe"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req, err := http.NewRequest(tc.method, srv.URL+tc.path, nil)
			if err != nil {
				t.Fatal(err)
			}
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400", resp.StatusCode)
			}
			if ct := resp.Header.Get("Content-Type"); ct != "application/json" {
				t.Fatalf("content-type = %q", ct)
			}
			raw, _ := io.ReadAll(resp.Body)
			var body map[string]any
			if err := json.Unmarshal(raw, &body); err != nil || body["error"] == nil {
				t.Fatalf("body = %q", raw)
			}
		})
	}
}

// A session or document literally named "subscribe" keeps its ordinary changes
// route rather than being mistaken for the endpoint keyword.
func TestSubscribeIdentifierNamedSubscribeKeepsChangesRoute(t *testing.T) {
	h, _ := newTestHandler(t)
	setupSession(t, h, "dev", "subscribe")
	w, body := doRequest(t, h, http.MethodGet, "/v1/sessions/subscribe/documents/doc/changes")
	if w.Code != 200 || body["nextCursor"].(float64) != 0 {
		t.Fatalf("session named subscribe: %d %v", w.Code, body)
	}

	setupSession(t, h, "dev2", "sess2")
	postJSON(t, h, "/v1/documents/subscribe/changes", map[string]any{
		"deviceId": "dev2",
		"changes":  []any{map[string]any{"id": "x", "payload": map[string]any{"n": 1}}},
	})
	w, body = doRequest(t, h, http.MethodGet, "/v1/sessions/sess2/documents/subscribe/changes")
	if w.Code != 200 || body["nextCursor"].(float64) != 1 {
		t.Fatalf("document named subscribe: %d %v", w.Code, body)
	}
}

func TestSubscribeUnknownSession404(t *testing.T) {
	h, _ := newTestHandler(t)
	srv := httptest.NewServer(h)
	defer srv.Close()

	c, code, body := dialWS(t, subscribeURL(srv, "ghost", "doc", "0"))
	if c != nil {
		t.Fatal("unknown session upgraded")
	}
	if code != http.StatusNotFound {
		t.Fatalf("code = %d, want 404", code)
	}
	if body["error"] == nil {
		t.Fatalf("body = %v", body)
	}
}

func TestSubscribeDeletedSession404(t *testing.T) {
	h, _ := newTestHandler(t)
	srv := httptest.NewServer(h)
	defer srv.Close()
	setupSession(t, h, "dev", "sess")
	if w, _ := doRequestWithDelete(t, h, "/v1/devices/dev/sessions/sess"); w.Code != 200 {
		t.Fatalf("delete session: %d %s", w.Code, w.Body.String())
	}
	c, code, _ := dialWS(t, subscribeURL(srv, "sess", "doc", "0"))
	if c != nil {
		t.Fatal("deleted session upgraded")
	}
	if code != http.StatusNotFound {
		t.Fatalf("code = %d, want 404", code)
	}
}

func doRequestWithDelete(t *testing.T, h http.Handler, url string) (*httptest.ResponseRecorder, map[string]any) {
	t.Helper()
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodDelete, url, nil))
	var decoded map[string]any
	if w.Body.Len() > 0 {
		_ = json.Unmarshal(w.Body.Bytes(), &decoded)
	}
	return w, decoded
}

func TestSubscribeRevokedBeforeConnect403(t *testing.T) {
	h, s := newTestHandler(t)
	srv := httptest.NewServer(h)
	defer srv.Close()
	setupSession(t, h, "dev", "sess")
	seedDoc(t, h, "doc", 1)
	if _, err := s.SetDocumentPermission("doc", "dev", false); err != nil {
		t.Fatal(err)
	}
	c, code, body := dialWS(t, subscribeURL(srv, "sess", "doc", "0"))
	if c != nil {
		t.Fatal("revoked session upgraded")
	}
	if code != http.StatusForbidden {
		t.Fatalf("code = %d, want 403", code)
	}
	if body["error"] == nil {
		t.Fatalf("body = %v", body)
	}
}

// Revocation after the upgrade ends the subscription with 4403 and stops every
// subsequent push.
func TestSubscribeRevokedAfterConnectCloses4403(t *testing.T) {
	h, s := newTestHandler(t)
	srv := httptest.NewServer(h)
	defer srv.Close()
	setupSession(t, h, "dev", "sess")
	seedDoc(t, h, "doc", 1)

	c, status, _ := dialWS(t, subscribeURL(srv, "sess", "doc", "0"))
	if status != 101 {
		t.Fatalf("status = %d", status)
	}
	c.setReadDeadline(2 * time.Second)
	if r := c.readChange(t); r["id"] != "c1" {
		t.Fatalf("backfill = %v", r)
	}

	// Parked; revoke must close with 4403.
	if _, err := s.SetDocumentPermission("doc", "dev", false); err != nil {
		t.Fatal(err)
	}
	if code := c.readCloseCode(t); code != 4403 {
		t.Fatalf("close code = %d, want 4403", code)
	}

	// A later commit is not delivered: the read must time out (EOF/deadline),
	// not return a text frame.
	postJSON(t, h, "/v1/documents/doc/changes", map[string]any{
		"deviceId": "dev2",
		"changes":  []any{map[string]any{"id": "after", "payload": map[string]any{"n": 9}}},
	})
	c.setReadDeadline(300 * time.Millisecond)
	if op, payload := c.readFrameOptional(t); op != 0 {
		t.Fatalf("received a frame after revocation: op=%d payload=%q", op, payload)
	}
}

// Closing the store (the termination path) ends a parked subscription with
// 1001.
func TestSubscribeStoreCloseCloses1001(t *testing.T) {
	h, s := newTestHandler(t)
	srv := httptest.NewServer(h)
	defer srv.Close()
	setupSession(t, h, "dev", "sess")
	seedDoc(t, h, "doc", 1)

	c, status, _ := dialWS(t, subscribeURL(srv, "sess", "doc", "1"))
	if status != 101 {
		t.Fatalf("status = %d", status)
	}
	c.setReadDeadline(2 * time.Second)

	closed := make(chan error, 1)
	go func() { closed <- s.Close() }()
	if code := c.readCloseCode(t); code != 1001 {
		t.Fatalf("close code = %d, want 1001", code)
	}
	select {
	case err := <-closed:
		if err != nil {
			t.Fatalf("store close: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("store.Close waited for the subscription instead of closing it")
	}
}

// A client disconnect releases the subscription promptly and leaves no record;
// the store can then close without waiting.
func TestSubscribeClientDisconnectReleases(t *testing.T) {
	h, s := newTestHandler(t)
	srv := httptest.NewServer(h)
	defer srv.Close()
	setupSession(t, h, "dev", "sess")
	seedDoc(t, h, "doc", 1)

	c, status, _ := dialWS(t, subscribeURL(srv, "sess", "doc", "1"))
	if status != 101 {
		t.Fatalf("status = %d", status)
	}
	if err := c.conn.Close(); err != nil {
		t.Fatal(err)
	}

	// The dropped, parked subscription created no change or cursor: once the
	// server observes the disconnect the log is still the single seeded row.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if rows, next, err := s.ListChanges("doc", 0, 100); err == nil && len(rows) == 1 && next == 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	rows, next, err := s.ListChanges("doc", 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || next != 1 {
		t.Fatalf("subscription left records: rows=%d next=%d", len(rows), next)
	}

	// With the client gone, the store closes without waiting on the
	// subscription.
	done := make(chan error, 1)
	go func() { done <- s.Close() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("store close after disconnect: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("store.Close hung after the client disconnected")
	}
}

// The channel is push-only: inbound data frames are ignored and pushes keep
// flowing.
func TestSubscribeIgnoresInboundData(t *testing.T) {
	h, _ := newTestHandler(t)
	srv := httptest.NewServer(h)
	defer srv.Close()
	setupSession(t, h, "dev", "sess")

	c, status, _ := dialWS(t, subscribeURL(srv, "sess", "doc", "0"))
	if status != 101 {
		t.Fatalf("status = %d", status)
	}
	c.setReadDeadline(2 * time.Second)

	c.sendMasked(t, 0x1, []byte(`{"unauthorized":"write"}`))
	c.sendMasked(t, 0x1, []byte("anything"))

	postJSON(t, h, "/v1/documents/doc/changes", map[string]any{
		"deviceId": "dev",
		"changes":  []any{map[string]any{"id": "c1", "payload": map[string]any{"n": 1}}},
	})
	if r := c.readChange(t); r["id"] != "c1" {
		t.Fatalf("push after inbound data = %v", r)
	}
}
