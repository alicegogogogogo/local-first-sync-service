package service_test

import (
	"bufio"
	"crypto/rand"
	"crypto/sha1"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"net/http"
	"path/filepath"
	"testing"
	"time"
)

// dialProcessWS opens a subscription against a running process over raw TCP,
// returning the connection and a reader positioned after the 101 headers.
func dialProcessWS(t *testing.T, addr, sessionID, doc, after string) (net.Conn, *bufio.Reader, int) {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	keyBuf := make([]byte, 16)
	if _, err := rand.Read(keyBuf); err != nil {
		t.Fatal(err)
	}
	key := base64.StdEncoding.EncodeToString(keyBuf)
	uri := fmt.Sprintf("/v1/sessions/%s/documents/%s/subscribe?after=%s", sessionID, doc, after)
	fmt.Fprintf(conn, "GET %s HTTP/1.1\r\nHost: %s\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: %s\r\nSec-WebSocket-Version: 13\r\n\r\n",
		uri, addr, key)
	br := bufio.NewReader(conn)
	resp, err := http.ReadResponse(br, nil)
	if err != nil {
		_ = conn.Close()
		t.Fatalf("read response: %v", err)
	}
	if resp.StatusCode != http.StatusSwitchingProtocols {
		_ = conn.Close()
		return conn, br, resp.StatusCode
	}
	sum := sha1.Sum([]byte(key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"))
	if got := resp.Header.Get("Sec-WebSocket-Accept"); got != base64.StdEncoding.EncodeToString(sum[:]) {
		_ = conn.Close()
		t.Fatalf("accept = %q", got)
	}
	return conn, br, http.StatusSwitchingProtocols
}

func readCloseCode(t *testing.T, br *bufio.Reader) uint16 {
	t.Helper()
	var head [2]byte
	if _, err := io.ReadFull(br, head[:]); err != nil {
		t.Fatalf("read close header: %v", err)
	}
	op := head[0] & 0x0F
	if op != 0x8 {
		t.Fatalf("opcode = %d, want close(8)", op)
	}
	n := int(head[1] & 0x7F)
	body := make([]byte, n)
	if _, err := io.ReadFull(br, body); err != nil {
		t.Fatal(err)
	}
	return binary.BigEndian.Uint16(body[:2])
}

// On SIGTERM every live subscription ends with WebSocket close code 1001, the
// process exits zero promptly, and committed changes remain readable at their
// original cursors after restart.
func TestProcessShutdownClosesSubscription1001(t *testing.T) {
	dataPath := filepath.Join(t.TempDir(), "sync.db")
	p := startProcess(t, "SYNC_ADDR=127.0.0.1:0", "SYNC_DATA="+dataPath)

	if status, body := processJSON(t, http.MethodPost, "http://"+p.addr+"/v1/devices",
		map[string]string{"deviceId": "dev"}); status != http.StatusOK || body["created"] != true {
		t.Fatalf("register device: %d %v", status, body)
	}
	if status, body := processJSON(t, http.MethodPost, "http://"+p.addr+"/v1/devices/dev/sessions",
		map[string]string{"sessionId": "sess"}); status != http.StatusOK || body["created"] != true {
		t.Fatalf("create session: %d %v", status, body)
	}
	if status, body := processJSON(t, http.MethodPost, "http://"+p.addr+"/v1/documents/doc/changes",
		map[string]any{
			"deviceId": "dev",
			"changes":  []any{map[string]any{"id": "c1", "payload": map[string]any{"n": 1}}},
		}); status != http.StatusOK || body["results"] == nil {
		t.Fatalf("seed change: %d %v", status, body)
	}

	conn, br, code := dialProcessWS(t, p.addr, "sess", "doc", "0")
	if code != http.StatusSwitchingProtocols {
		t.Fatalf("upgrade status = %d, want 101", code)
	}
	defer conn.Close()
	// Receive the backfilled c1.
	var head [2]byte
	if _, err := io.ReadFull(br, head[:]); err != nil {
		t.Fatalf("read backfill header: %v", err)
	}
	if op := head[0] & 0x0F; op != 0x1 {
		t.Fatalf("backfill opcode = %d, want text", op)
	}
	n := int64(head[1] & 0x7F)
	if _, err := io.CopyN(io.Discard, br, n); err != nil {
		t.Fatal(err)
	}

	terminated := make(chan int, 1)
	go func() { terminated <- p.terminate(t) }()

	// The parked subscription must receive a 1001 close promptly.
	closeCodeCh := make(chan uint16, 1)
	go func() {
		defer close(closeCodeCh)
		closeCodeCh <- readCloseCode(t, br)
	}()
	select {
	case got := <-closeCodeCh:
		if got != 1001 {
			t.Fatalf("close code = %d, want 1001", got)
		}
	case <-time.After(5 * time.Second):
		_ = p.cmd.Process.Kill()
		t.Fatal("subscription was not closed with 1001 on shutdown")
	}

	select {
	case exit := <-terminated:
		if exit != 0 {
			t.Fatalf("exit code = %d, want 0", exit)
		}
	case <-time.After(5 * time.Second):
		_ = p.cmd.Process.Kill()
		t.Fatal("process did not exit promptly after closing subscriptions")
	}

	// Restart against the same data: the committed change is still at cursor 1
	// and a new subscription can resume from the historical cursor 0.
	p2 := startProcess(t, "SYNC_ADDR=127.0.0.1:0", "SYNC_DATA="+dataPath)
	defer p2.cmd.Process.Kill()

	status, body := processJSON(t, http.MethodGet, "http://"+p2.addr+"/v1/documents/doc/changes?after=0", nil)
	if status != http.StatusOK || body["nextCursor"].(float64) != 1 {
		t.Fatalf("post-restart read: %d %v", status, body)
	}
	rows := body["changes"].([]any)
	if len(rows) != 1 || rows[0].(map[string]any)["id"] != "c1" {
		t.Fatalf("post-restart rows = %v", rows)
	}

	conn2, br2, code2 := dialProcessWS(t, p2.addr, "sess", "doc", "0")
	if code2 != http.StatusSwitchingProtocols {
		t.Fatalf("post-restart upgrade = %d", code2)
	}
	defer conn2.Close()
	_ = conn2.SetReadDeadline(time.Now().Add(3 * time.Second))
	var h2 [2]byte
	if _, err := io.ReadFull(br2, h2[:]); err != nil {
		t.Fatalf("post-restart read: %v", err)
	}
	if op := h2[0] & 0x0F; op != 0x1 {
		t.Fatalf("post-restart opcode = %d, want text", op)
	}
}
