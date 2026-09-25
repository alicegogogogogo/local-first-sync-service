package service_test

import (
	"encoding/binary"
	"net/http"
	"path/filepath"
	"testing"
	"time"
)

// On SIGTERM the process ends a live CRDT state subscription with close code
// 1001, exits zero, and the committed CRDT state is intact when the same data
// path is served again; a fresh subscription then receives that state.
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
	if status, _ := processJSON(t, http.MethodPost, "http://"+p.addr+"/v1/documents/doc/crdt/ops",
		map[string]any{
			"deviceId": "dev",
			"type":     "counter",
			"ops":      []any{map[string]any{"id": "op-1", "value": 5}},
		}); status != http.StatusOK {
		t.Fatalf("seed crdt op = %d", status)
	}

	wsURL := "http://" + p.addr + "/v1/sessions/sess/documents/doc/crdt/state/subscribe"
	ws := dialProcessWS(t, wsURL)

	// The current merged state is the first frame.
	opcode, payload := ws.nextOpcode()
	if opcode != 0x1 {
		t.Fatalf("first frame opcode = %d, want text", opcode)
	}
	if want := []byte(`{"type":"counter","value":5}` + "\n"); string(payload) != string(want) {
		t.Fatalf("first frame = %q, want %q", payload, want)
	}

	// Park until SIGTERM; the connection must close 1001.
	terminated := make(chan int, 1)
	go func() { terminated <- p.terminate(t) }()

	opcode, payload = ws.nextOpcode()
	if opcode != 0x8 {
		t.Fatalf("frame after SIGTERM opcode = %d, want close (8)", opcode)
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
		t.Fatal("process did not exit promptly after closing crdt subscriptions")
	}

	// Restart: committed CRDT state stays readable.
	p2 := startProcess(t, "SYNC_ADDR=127.0.0.1:0", "SYNC_DATA="+dataPath)
	defer p2.cmd.Process.Kill()
	status, body := processJSON(t, http.MethodGet,
		"http://"+p2.addr+"/v1/documents/doc/crdt/state", nil)
	if status != http.StatusOK {
		t.Fatalf("crdt state after restart = %d", status)
	}
	if body["type"] != "counter" || body["value"].(float64) != 5 {
		t.Fatalf("post-restart state = %v", body)
	}

	// A fresh subscription after restart immediately receives the state.
	ws2 := dialProcessWS(t, "http://"+p2.addr+"/v1/sessions/sess/documents/doc/crdt/state/subscribe")
	defer ws2.close()
	opcode, payload = ws2.nextOpcode()
	if opcode != 0x1 {
		t.Fatalf("post-restart frame opcode = %d, want text", opcode)
	}
	if want := []byte(`{"type":"counter","value":5}` + "\n"); string(payload) != string(want) {
		t.Fatalf("post-restart frame = %q, want %q", payload, want)
	}
}
