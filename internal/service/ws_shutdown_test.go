package service_test

import (
	"bufio"
	"bytes"
	"crypto/rand"
	"crypto/sha1"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"path/filepath"
	"testing"
	"time"
)

// Minimal raw RFC 6455 client for the real-binary process tests.
type procWS struct {
	conn net.Conn
	br   *bufio.Reader
}

func dialProcessWS(t *testing.T, httpURL string) *procWS {
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
	fmt.Fprintf(conn,
		"GET %s HTTP/1.1\r\nHost: %s\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"+
			"Sec-WebSocket-Key: %s\r\nSec-WebSocket-Version: 13\r\n\r\n",
		u.RequestURI(), u.Host, key)
	br := bufio.NewReader(conn)
	resp, err := http.ReadResponse(br, nil)
	if err != nil {
		_ = conn.Close()
		t.Fatalf("handshake: %v", err)
	}
	if resp.StatusCode != http.StatusSwitchingProtocols {
		_ = conn.Close()
		t.Fatalf("status = %d, want 101", resp.StatusCode)
	}
	sum := sha1.Sum([]byte(key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"))
	if resp.Header.Get("Sec-WebSocket-Accept") != base64.StdEncoding.EncodeToString(sum[:]) {
		_ = conn.Close()
		t.Fatal("bad Sec-WebSocket-Accept")
	}
	return &procWS{conn: conn, br: br}
}

// nextOpcode blocks until a frame arrives and returns its opcode/payload.
func (c *procWS) nextOpcode() (byte, []byte) {
	t := time.Now().Add(5 * time.Second)
	_ = c.conn.SetReadDeadline(t)
	header := make([]byte, 2)
	if _, err := io.ReadFull(c.br, header); err != nil {
		return 0, nil
	}
	opcode := header[0] & 0x0F
	n := int64(header[1] & 0x7F)
	if n == 126 {
		var ext [2]byte
		if _, err := io.ReadFull(c.br, ext[:]); err != nil {
			return 0, nil
		}
		n = int64(binary.BigEndian.Uint16(ext[:]))
	} else if n == 127 {
		var ext [8]byte
		if _, err := io.ReadFull(c.br, ext[:]); err != nil {
			return 0, nil
		}
		n = int64(binary.BigEndian.Uint64(ext[:]))
	}
	payload := make([]byte, n)
	if n > 0 {
		if _, err := io.ReadFull(c.br, payload); err != nil {
			return opcode, nil
		}
	}
	return opcode, payload
}

func (c *procWS) close() { _ = c.conn.Close() }

// On SIGTERM the process ends a live WebSocket subscription with close code
// 1001, exits zero, and the committed change log (with its cursors) is intact
// when the same data path is served again.
func TestProcessShutdownClosesSubscription1001(t *testing.T) {
	dataPath := filepath.Join(t.TempDir(), "sync.db")
	p := startProcess(t, "SYNC_ADDR=127.0.0.1:0", "SYNC_DATA="+dataPath)

	if status, _ := processJSON(t, http.MethodPost, "http://"+p.addr+"/v1/devices",
		map[string]string{"deviceId": "dev"}); status != http.StatusOK {
		t.Fatalf("register device = %d", status)
	}
	if status, _ := processJSON(t, http.MethodPost, "http://"+p.addr+"/v1/devices/dev/sessions",
		map[string]string{"sessionId": "sess"}); status != http.StatusOK {
		t.Fatalf("create session = %d", status)
	}
	if status, _ := processJSON(t, http.MethodPost, "http://"+p.addr+"/v1/documents/doc/changes",
		map[string]any{
			"deviceId": "dev",
			"changes":  []any{map[string]any{"id": "c1", "payload": map[string]any{"n": 1}}},
		}); status != http.StatusOK {
		t.Fatalf("seed change = %d", status)
	}

	wsURL := "http://" + p.addr + "/v1/sessions/sess/documents/doc/changes/subscribe?cursor=1"
	ws := dialProcessWS(t, wsURL)

	// Park until SIGTERM; the connection must close 1001.
	terminated := make(chan int, 1)
	go func() { terminated <- p.terminate(t) }()

	opcode, payload := ws.nextOpcode()
	if opcode != 0x8 {
		t.Fatalf("first frame after SIGTERM opcode = %d, want close (8)", opcode)
	}
	if got := int(binary.BigEndian.Uint16(payload)); got != 1001 {
		t.Fatalf("close code = %d, want 1001", got)
	}
	ws.close()

	select {
	case code := <-terminated:
		if code != 0 {
			t.Fatalf("exit code = %d, want 0", code)
		}
	case <-time.After(5 * time.Second):
		_ = p.cmd.Process.Kill()
		t.Fatal("process did not exit promptly after closing subscriptions")
	}

	// Restart (any free port): committed data and cursors remain readable.
	p2 := startProcess(t, "SYNC_ADDR=127.0.0.1:0", "SYNC_DATA="+dataPath)
	defer p2.cmd.Process.Kill()
	status, body := processJSON(t, http.MethodGet,
		"http://"+p2.addr+"/v1/sessions/sess/documents/doc/changes", nil)
	if status != http.StatusOK {
		t.Fatalf("read after restart = %d", status)
	}
	rows := body["changes"].([]any)
	if len(rows) != 1 || rows[0].(map[string]any)["id"] != "c1" ||
		body["nextCursor"].(float64) != 1 {
		t.Fatalf("post-restart log = %v", body)
	}

	// A fresh subscription after restart can start at a historical cursor.
	ws2 := dialProcessWS(t, "http://"+p2.addr+"/v1/sessions/sess/documents/doc/changes/subscribe?cursor=0")
	defer ws2.close()
	opcode, payload = ws2.nextOpcode()
	if opcode != 0x1 {
		t.Fatalf("post-restart frame opcode = %d, want text", opcode)
	}
	if !bytes.Contains(payload, []byte(`"id":"c1"`)) {
		t.Fatalf("post-restart frame = %s", payload)
	}
}
