package service_test

import (
	"bufio"
	"bytes"
	"crypto/rand"
	"encoding/base64"
	"io"
	"net"
	"net/http"
	"net/url"
	"path/filepath"
	"testing"
	"time"
)

// dialProcessRawHandshake opens a raw TCP WebSocket handshake against a
// running process and returns the HTTP response (non-101 responses are kept so
// the caller can assert the pre-upgrade status) together with the connection.
func dialProcessRawHandshake(t *testing.T, httpURL, requestTarget string) (*http.Response, net.Conn) {
	t.Helper()
	u, err := url.Parse(httpURL)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := net.DialTimeout("tcp", u.Host, 3*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	keyBytes := make([]byte, 16)
	_, _ = rand.Read(keyBytes)
	key := base64.StdEncoding.EncodeToString(keyBytes)
	io.WriteString(conn,
		"GET "+requestTarget+" HTTP/1.1\r\nHost: "+u.Host+"\r\n"+
			"Upgrade: websocket\r\nConnection: Upgrade\r\n"+
			"Sec-WebSocket-Key: "+key+"\r\nSec-WebSocket-Version: 13\r\n\r\n")
	br := bufio.NewReader(conn)
	resp, err := http.ReadResponse(br, nil)
	if err != nil {
		conn.Close()
		t.Fatalf("handshake: %v", err)
	}
	return resp, conn
}

// Document-level subscriptions need no session: a registered device declares
// itself with the deviceId query parameter, catches up from a historical
// cursor, and after a process restart a fresh subscription resumes from any
// cursor while a CRDT subscription immediately re-receives the current merged
// state.
func TestProcessDocumentSubscriptionsWithoutSession(t *testing.T) {
	dataPath := filepath.Join(t.TempDir(), "sync.db")
	p := startProcess(t, "SYNC_ADDR=127.0.0.1:0", "SYNC_DATA="+dataPath)

	if status, _ := processJSON(t, http.MethodPost, "http://"+p.addr+"/v1/devices",
		map[string]string{"deviceId": "dev"}); status != http.StatusOK {
		t.Fatalf("register device = %d", status)
	}
	// Deliberately create no session: these entry points must work for a
	// client that never opens one.
	for i, id := range []string{"c1", "c2"} {
		if status, _ := processJSON(t, http.MethodPost, "http://"+p.addr+"/v1/documents/doc/changes",
			map[string]any{
				"deviceId": "dev",
				"changes":  []any{map[string]any{"id": id, "payload": map[string]any{"n": i + 1}}},
			}); status != http.StatusOK {
			t.Fatalf("seed change %s = %d", id, status)
		}
	}
	if status, _ := processJSON(t, http.MethodPost, "http://"+p.addr+"/v1/documents/doc/crdt/ops",
		map[string]any{
			"deviceId": "dev",
			"type":     "counter",
			"ops":      []any{map[string]any{"id": "op-1", "value": 6}},
		}); status != http.StatusOK {
		t.Fatalf("seed crdt op = %d", status)
	}

	// Change subscription by deviceId only, starting at cursor 0: both seeds
	// arrive in order.
	ws := dialProcessWS(t, "http://"+p.addr+"/v1/documents/doc/changes/subscribe?cursor=0&deviceId=dev")
	for _, want := range []string{`"id":"c1"`, `"id":"c2"`} {
		opcode, payload := ws.nextOpcode()
		if opcode != 0x1 {
			t.Fatalf("change frame opcode = %d, want text", opcode)
		}
		if !bytes.Contains(payload, []byte(want)) {
			t.Fatalf("change frame = %s, want %s", payload, want)
		}
	}
	ws.close()

	// CRDT state subscription by deviceId only: the current state arrives.
	cws := dialProcessWS(t, "http://"+p.addr+"/v1/documents/doc/crdt/state/subscribe?deviceId=dev")
	opcode, payload := cws.nextOpcode()
	if opcode != 0x1 {
		t.Fatalf("crdt frame opcode = %d, want text", opcode)
	}
	if want := []byte(`{"type":"counter","value":6}` + "\n"); string(payload) != string(want) {
		t.Fatalf("crdt frame = %q, want %q", payload, want)
	}
	cws.close()

	// An undeclared/unregistered device is rejected before the upgrade: 404.
	resp404, rawConn := dialProcessRawHandshake(t, "http://"+p.addr,
		"/v1/documents/doc/changes/subscribe?cursor=0&deviceId=ghost")
	rawConn.Close()
	if resp404.StatusCode != http.StatusNotFound {
		t.Fatalf("ghost device handshake status = %d, want 404", resp404.StatusCode)
	}
	// A missing deviceId parameter is the same 404.
	resp404b, rawConnB := dialProcessRawHandshake(t, "http://"+p.addr,
		"/v1/documents/doc/changes/subscribe?cursor=0")
	rawConnB.Close()
	if resp404b.StatusCode != http.StatusNotFound {
		t.Fatalf("missing deviceId handshake status = %d, want 404", resp404b.StatusCode)
	}

	// Restart on the same data path; subscriptions are not persisted, but the
	// history is, so a new subscription resumes from any historical cursor.
	p2 := startProcess(t, "SYNC_ADDR=127.0.0.1:0", "SYNC_DATA="+dataPath)
	defer p2.cmd.Process.Kill()

	ws2 := dialProcessWS(t, "http://"+p2.addr+"/v1/documents/doc/changes/subscribe?cursor=1&deviceId=dev")
	defer ws2.close()
	opcode, payload = ws2.nextOpcode()
	if opcode != 0x1 {
		t.Fatalf("post-restart change frame opcode = %d, want text", opcode)
	}
	if !bytes.Contains(payload, []byte(`"id":"c2"`)) || !bytes.Contains(payload, []byte(`"cursor":2`)) {
		t.Fatalf("post-restart catch-up frame = %s", payload)
	}

	// A fresh CRDT subscription after restart immediately gets the consistent
	// current merged state.
	cws2 := dialProcessWS(t, "http://"+p2.addr+"/v1/documents/doc/crdt/state/subscribe?deviceId=dev")
	defer cws2.close()
	opcode, payload = cws2.nextOpcode()
	if opcode != 0x1 {
		t.Fatalf("post-restart crdt frame opcode = %d, want text", opcode)
	}
	if want := []byte(`{"type":"counter","value":6}` + "\n"); string(payload) != string(want) {
		t.Fatalf("post-restart crdt frame = %q, want %q", payload, want)
	}
}
