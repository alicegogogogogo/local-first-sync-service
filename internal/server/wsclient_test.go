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
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/alicegogogogogo/local-first-sync-service/internal/store"
)

// wsClient is a minimal raw RFC 6455 client used to drive the subscription
// endpoint over a real TCP connection (HTTP hijacking does not work with
// httptest.ResponseRecorder).
type wsClient struct {
	t    *testing.T
	conn net.Conn
	br   *bufio.Reader
}

func newWSKey() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return base64.StdEncoding.EncodeToString(b)
}

// dialWS performs the opening handshake against an httptest.Server URL. A
// non-101 response is returned as (nil, response) so tests can assert the
// 400/403/404 JSON failures.
func dialWS(t *testing.T, serverURL string) (*wsClient, *http.Response) {
	t.Helper()
	u, err := url.Parse(serverURL)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := net.DialTimeout("tcp", u.Host, 2*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	key := newWSKey()
	req := fmt.Sprintf(
		"GET %s HTTP/1.1\r\nHost: %s\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"+
			"Sec-WebSocket-Key: %s\r\nSec-WebSocket-Version: 13\r\n\r\n",
		u.RequestURI(), u.Host, key)
	if _, err := io.WriteString(conn, req); err != nil {
		_ = conn.Close()
		t.Fatalf("write handshake: %v", err)
	}
	br := bufio.NewReader(conn)
	resp, err := http.ReadResponse(br, nil)
	if err != nil {
		_ = conn.Close()
		t.Fatalf("read handshake: %v", err)
	}
	if resp.StatusCode != http.StatusSwitchingProtocols {
		_ = conn.Close()
		return nil, resp
	}
	sum := sha1.Sum([]byte(key + wsGUID))
	if got := resp.Header.Get("Sec-WebSocket-Accept"); got != base64.StdEncoding.EncodeToString(sum[:]) {
		_ = conn.Close()
		t.Fatalf("accept = %q, want the RFC 6455 digest", got)
	}
	return &wsClient{t: t, conn: conn, br: br}, resp
}

// dialWSRaw sends a caller-built handshake request, for negative tests that
// omit or corrupt a header.
func dialWSRaw(t *testing.T, serverURL, requestTarget string, headers ...string) (*http.Response, net.Conn) {
	t.Helper()
	u, err := url.Parse(serverURL)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := net.DialTimeout("tcp", u.Host, 2*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	var b strings.Builder
	b.WriteString("GET " + requestTarget + " HTTP/1.1\r\n")
	b.WriteString("Host: " + u.Host + "\r\n")
	for _, h := range headers {
		b.WriteString(h + "\r\n")
	}
	b.WriteString("\r\n")
	if _, err := io.WriteString(conn, b.String()); err != nil {
		_ = conn.Close()
		t.Fatalf("write handshake: %v", err)
	}
	br := bufio.NewReader(conn)
	resp, err := http.ReadResponse(br, nil)
	if err != nil {
		_ = conn.Close()
		t.Fatalf("read handshake: %v", err)
	}
	return resp, conn
}

// readFrame reads one server (unmasked) frame.
func (c *wsClient) readFrame() (fin bool, opcode byte, payload []byte) {
	c.t.Helper()
	header := make([]byte, 2)
	if _, err := io.ReadFull(c.br, header); err != nil {
		c.t.Fatalf("read frame header: %v", err)
	}
	fin = header[0]&0x80 != 0
	opcode = header[0] & 0x0F
	length := int64(header[1] & 0x7F)
	switch length {
	case 126:
		var ext [2]byte
		if _, err := io.ReadFull(c.br, ext[:]); err != nil {
			c.t.Fatalf("read ext len: %v", err)
		}
		length = int64(binary.BigEndian.Uint16(ext[:]))
	case 127:
		var ext [8]byte
		if _, err := io.ReadFull(c.br, ext[:]); err != nil {
			c.t.Fatalf("read ext len: %v", err)
		}
		length = int64(binary.BigEndian.Uint64(ext[:]))
	}
	payload = make([]byte, length)
	if length > 0 {
		if _, err := io.ReadFull(c.br, payload); err != nil {
			c.t.Fatalf("read payload: %v", err)
		}
	}
	return fin, opcode, payload
}

func closeCode(payload []byte) int {
	if len(payload) < 2 {
		return -1
	}
	return int(binary.BigEndian.Uint16(payload[:2]))
}

// readFrameMaybe is the non-fatal variant used by negative assertions: with a
// read deadline already set it reports ok=false on a timeout/EOF instead of
// failing the test.
func (c *wsClient) readFrameMaybe() (fin bool, opcode byte, payload []byte, ok bool) {
	header := make([]byte, 2)
	if _, err := io.ReadFull(c.br, header); err != nil {
		return false, 0, nil, false
	}
	fin = header[0]&0x80 != 0
	opcode = header[0] & 0x0F
	length := int64(header[1] & 0x7F)
	switch length {
	case 126:
		var ext [2]byte
		if _, err := io.ReadFull(c.br, ext[:]); err != nil {
			return false, 0, nil, false
		}
		length = int64(binary.BigEndian.Uint16(ext[:]))
	case 127:
		var ext [8]byte
		if _, err := io.ReadFull(c.br, ext[:]); err != nil {
			return false, 0, nil, false
		}
		length = int64(binary.BigEndian.Uint64(ext[:]))
	}
	payload = make([]byte, length)
	if length > 0 {
		if _, err := io.ReadFull(c.br, payload); err != nil {
			return false, 0, nil, false
		}
	}
	return fin, opcode, payload, true
}

// readChange reads the next complete text message and decodes one change row.
// The server only ever sends whole (FIN) text frames; pongs are skipped.
func (c *wsClient) readChange() store.ListedChange {
	c.t.Helper()
	for {
		fin, opcode, payload := c.readFrame()
		switch opcode {
		case wsOpcodeText:
			if !fin {
				c.t.Fatal("server sent a fragmented text frame")
			}
			var change store.ListedChange
			if err := json.Unmarshal(payload, &change); err != nil {
				c.t.Fatalf("text frame is not a change row: %s (%v)", payload, err)
			}
			return change
		case wsOpcodePong:
			continue
		case wsOpcodeClose:
			c.t.Fatalf("unexpected close frame while reading change: code=%d", closeCode(payload))
		default:
			c.t.Fatalf("unexpected opcode %d while reading change", opcode)
		}
	}
}

// readCloseCode reads frames until the close frame and returns its code.
func (c *wsClient) readCloseCode() int {
	c.t.Helper()
	for {
		_, opcode, payload := c.readFrame()
		if opcode == wsOpcodeClose {
			return closeCode(payload)
		}
	}
}

// sendMasked writes one masked client frame.
func (c *wsClient) sendMasked(opcode byte, payload []byte) {
	c.t.Helper()
	mask := make([]byte, 4)
	_, _ = rand.Read(mask)
	masked := append([]byte(nil), payload...)
	for i := range masked {
		masked[i] ^= mask[i%4]
	}
	var header []byte
	b0 := byte(0x80) | opcode
	switch {
	case len(payload) <= 125:
		header = append(header, b0, 0x80|byte(len(payload)))
	case len(payload) <= 65535:
		header = append(header, b0, 0x80|126)
		ext := make([]byte, 2)
		binary.BigEndian.PutUint16(ext, uint16(len(payload)))
		header = append(header, ext...)
	default:
		header = append(header, b0, 0x80|127)
		ext := make([]byte, 8)
		binary.BigEndian.PutUint64(ext, uint64(len(payload)))
		header = append(header, ext...)
	}
	header = append(header, mask...)
	if _, err := c.conn.Write(header); err != nil {
		c.t.Fatalf("write frame header: %v", err)
	}
	if len(masked) > 0 {
		if _, err := c.conn.Write(masked); err != nil {
			c.t.Fatalf("write frame payload: %v", err)
		}
	}
}

func (c *wsClient) sendText(payload string) { c.sendMasked(wsOpcodeText, []byte(payload)) }
func (c *wsClient) sendPing()               { c.sendMasked(wsOpcodePing, nil) }

func (c *wsClient) setReadDeadline(d time.Duration) {
	_ = c.conn.SetReadDeadline(time.Now().Add(d))
}
func (c *wsClient) clearReadDeadline() { _ = c.conn.SetReadDeadline(time.Time{}) }

func (c *wsClient) sendClose(code int) {
	payload := make([]byte, 2)
	binary.BigEndian.PutUint16(payload, uint16(code))
	c.sendMasked(wsOpcodeClose, payload)
}

func (c *wsClient) close() { _ = c.conn.Close() }

// --- fixtures ---------------------------------------------------------------

func newWSTestServer(t *testing.T) (*httptest.Server, *store.Store) {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "ws.db"))
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(NewHandler(st))
	t.Cleanup(srv.Close)
	t.Cleanup(func() { _ = st.Close() })
	return srv, st
}

// setupSession registers a device owning a session and seeds the document with
// changes seed-1..seed-N, each carrying {"n":i}, via the public HTTP surface.
func setupSession(t *testing.T, srv *httptest.Server, device, session, doc string, seed int) {
	t.Helper()
	httpPost := func(path string, body any) {
		raw, _ := json.Marshal(body)
		resp, err := srv.Client().Post(srv.URL+path, "application/json", strings.NewReader(string(raw)))
		if err != nil {
			t.Fatalf("POST %s: %v", path, err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			data, _ := io.ReadAll(resp.Body)
			t.Fatalf("POST %s = %d %s", path, resp.StatusCode, data)
		}
	}
	httpPost("/v1/devices", map[string]any{"deviceId": device})
	httpPost("/v1/devices/"+device+"/sessions", map[string]any{"sessionId": session})
	for i := 0; i < seed; i++ {
		httpPost("/v1/documents/"+doc+"/changes", map[string]any{
			"deviceId": device,
			"changes": []any{
				map[string]any{"id": fmt.Sprintf("seed-%d", i+1), "payload": map[string]any{"n": i + 1}},
			},
		})
	}
}

func subscribeURL(srv *httptest.Server, session, doc, cursor string) string {
	return srv.URL + "/v1/sessions/" + session + "/documents/" + doc + "/changes/subscribe?cursor=" + cursor
}
