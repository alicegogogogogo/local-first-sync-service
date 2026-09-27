package service_test

import (
	"encoding/binary"
	"net/http"
	"path/filepath"
	"testing"
	"time"
)

// A session-scoped restore pushes the appended change to a live subscription;
// a SIGTERM then ends that subscription with 1001 and the process exits zero.
// After a restart against the same data path the restore provenance still
// answers idempotent and the appended change reads back.
func TestProcessSessionRestorePushesThenCloses1001AndPersists(t *testing.T) {
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
	if status, _ := processJSON(t, http.MethodPost, "http://"+p.addr+"/v1/documents/doc/snapshots",
		map[string]any{"cursor": 1, "state": map[string]any{"n": 1}}); status != http.StatusOK {
		t.Fatalf("seed snapshot = %d", status)
	}

	ws := dialProcessWS(t, "http://"+p.addr+"/v1/sessions/sess/documents/doc/changes/subscribe?cursor=1")

	// The session restore appends at cursor 2 and must push the new row.
	status, body := processJSON(t, http.MethodPost,
		"http://"+p.addr+"/v1/sessions/sess/documents/doc/restore",
		map[string]any{"changeId": "r1", "snapshotCursor": 1})
	if status != http.StatusOK {
		t.Fatalf("session restore = %d %v", status, body)
	}
	if body["created"] != true || body["cursor"].(float64) != 2 || body["restoredFrom"].(float64) != 1 {
		t.Fatalf("restore body = %v", body)
	}

	opcode, payload := ws.nextOpcode()
	if opcode != 0x1 {
		t.Fatalf("pushed frame opcode = %d, want text (1)", opcode)
	}
	if string(payload) != `{"id":"r1","deviceId":"dev","payload":{"n":1},"cursor":2}` {
		t.Fatalf("pushed frame = %s", payload)
	}

	// SIGTERM ends the subscription with going-away 1001 and the process exits
	// zero promptly.
	terminated := make(chan int, 1)
	go func() { terminated <- p.terminate(t) }()

	opcode, payload = ws.nextOpcode()
	if opcode != 0x8 {
		t.Fatalf("post-SIGTERM opcode = %d, want close (8)", opcode)
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
		t.Fatal("process did not exit promptly after closing the subscription")
	}

	// Restart: the restored change is synchronously readable and the restore is
	// idempotent with its first cursor.
	p2 := startProcess(t, "SYNC_ADDR=127.0.0.1:0", "SYNC_DATA="+dataPath)
	defer p2.cmd.Process.Kill()

	status, readBody := processJSON(t, http.MethodGet,
		"http://"+p2.addr+"/v1/sessions/sess/documents/doc/changes?after=1", nil)
	if status != http.StatusOK {
		t.Fatalf("read after restart = %d", status)
	}
	rows := readBody["changes"].([]any)
	if len(rows) != 1 {
		t.Fatalf("rows after restart = %v", rows)
	}
	row := rows[0].(map[string]any)
	if row["id"] != "r1" || row["cursor"].(float64) != 2 {
		t.Fatalf("restored row after restart = %v", row)
	}

	status, body = processJSON(t, http.MethodPost,
		"http://"+p2.addr+"/v1/sessions/sess/documents/doc/restore",
		map[string]any{"changeId": "r1", "snapshotCursor": 1})
	if status != http.StatusOK || body["created"] != false || body["cursor"].(float64) != 2 {
		t.Fatalf("idempotent restore after restart = %d %v", status, body)
	}
}
