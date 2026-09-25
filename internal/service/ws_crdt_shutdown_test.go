package service_test

import (
	"bytes"
	"encoding/binary"
	"net/http"
	"path/filepath"
	"testing"
	"time"
)

// On SIGTERM a live CRDT state subscription ends with close code 1001, the
// process exits zero, and the committed CRDT state is intact after a restart:
// a fresh subscription immediately receives the current merged state.
func TestProcessShutdownClosesCRDTSubscription1001(t *testing.T) {
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
	if status, _ := processJSON(t, http.MethodPost, "http://"+p.addr+"/v1/documents/doc/crdt/ops", map[string]any{
		"deviceId": "dev",
		"type":     "counter",
		"ops":      []any{map[string]any{"id": "op-1", "value": 6}},
	}); status != http.StatusOK {
		t.Fatalf("seed crdt op = %d", status)
	}

	wsURL := "http://" + p.addr + "/v1/sessions/sess/documents/doc/crdt/state/subscribe"
	ws := dialProcessWS(t, wsURL)

	// First frame is the current merged state.
	opcode, payload := ws.nextOpcode()
	if opcode != 0x1 || !bytes.Equal(payload, []byte(`{"type":"counter","value":6}`+"\n")) {
		t.Fatalf("first frame opcode=%d payload=%q, want counter/6 text frame", opcode, payload)
	}

	// Park until SIGTERM; the connection must close 1001.
	terminated := make(chan int, 1)
	go func() { terminated <- p.terminate(t) }()

	opcode, payload = ws.nextOpcode()
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
		t.Fatal("process did not exit promptly after closing CRDT subscriptions")
	}

	// Restart: committed state stays readable through the state endpoint...
	p2 := startProcess(t, "SYNC_ADDR=127.0.0.1:0", "SYNC_DATA="+dataPath)
	defer p2.cmd.Process.Kill()
	status, body := processJSON(t, http.MethodGet,
		"http://"+p2.addr+"/v1/documents/doc/crdt/state", nil)
	if status != http.StatusOK || body["type"] != "counter" || body["value"].(float64) != 6 {
		t.Fatalf("post-restart state = %d %v", status, body)
	}

	// ...and a fresh subscription immediately pushes the same state (the
	// old subscription did not survive the restart).
	ws2 := dialProcessWS(t, "http://"+p2.addr+"/v1/sessions/sess/documents/doc/crdt/state/subscribe")
	defer ws2.close()
	opcode, payload = ws2.nextOpcode()
	if opcode != 0x1 || !bytes.Equal(payload, []byte(`{"type":"counter","value":6}`+"\n")) {
		t.Fatalf("post-restart frame opcode=%d payload=%q, want counter/6", opcode, payload)
	}
}

// A subscription to a document without any CRDT operations pushes nothing
// until the first state appears, even across the upgrade boundary.
func TestProcessCRDTSubscriptionWaitsForFirstState(t *testing.T) {
	dataPath := filepath.Join(t.TempDir(), "sync.db")
	p := startProcess(t, "SYNC_ADDR=127.0.0.1:0", "SYNC_DATA="+dataPath)
	defer p.cmd.Process.Kill()

	if status, _ := processJSON(t, http.MethodPost, "http://"+p.addr+"/v1/devices",
		map[string]string{"deviceId": "dev"}); status != http.StatusOK {
		t.Fatalf("register device = %d", status)
	}
	if status, _ := processJSON(t, http.MethodPost, "http://"+p.addr+"/v1/devices/dev/sessions",
		map[string]string{"sessionId": "sess"}); status != http.StatusOK {
		t.Fatalf("create session = %d", status)
	}

	ws := dialProcessWS(t, "http://"+p.addr+"/v1/sessions/sess/documents/doc/crdt/state/subscribe")
	defer ws.close()

	// No frame before the first op: a short read window times out.
	_ = ws.conn.SetReadDeadline(time.Now().Add(400 * time.Millisecond))
	if _, err := ws.br.Peek(2); err == nil {
		t.Fatal("a frame arrived before the first CRDT operation")
	}
	_ = ws.conn.SetReadDeadline(time.Time{})

	if status, _ := processJSON(t, http.MethodPost, "http://"+p.addr+"/v1/documents/doc/crdt/ops", map[string]any{
		"deviceId": "dev",
		"type":     "gset",
		"ops":      []any{map[string]any{"id": "g-1", "elements": []any{"banana", "apple"}}},
	}); status != http.StatusOK {
		t.Fatalf("first gset op = %d", status)
	}

	opcode, payload := ws.nextOpcode()
	if opcode != 0x1 || !bytes.Equal(payload, []byte(`{"type":"gset","value":["apple","banana"]}`+"\n")) {
		t.Fatalf("first state frame opcode=%d payload=%q", opcode, payload)
	}
}
