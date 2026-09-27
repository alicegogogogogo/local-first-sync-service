package service_test

import (
	"encoding/json"
	"io"
	"net/http"
	"path/filepath"
	"testing"
	"time"
)

// A parked session-scoped long poll is canceled on shutdown exactly like the
// document-level one: SIGTERM interrupts the wait, the parked request answers a
// JSON 503, and the process exits zero promptly. No change or cursor is written
// by the canceled wait.
func TestProcessShutdownInterruptsSessionLongPoll(t *testing.T) {
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
	if status, body := processJSON(t, http.MethodPost,
		"http://"+p.addr+"/v1/sessions/sess/documents/doc/changes",
		map[string]any{
			"changes": []any{map[string]any{"id": "c1", "payload": map[string]any{"n": 1}}},
		}); status != http.StatusOK || body["results"] == nil {
		t.Fatalf("seed change through session: %d %v", status, body)
	}

	type pollResult struct {
		status int
		body   map[string]any
		err    error
	}
	result := make(chan pollResult, 1)
	req, err := http.NewRequest(http.MethodGet,
		"http://"+p.addr+"/v1/sessions/sess/documents/doc/changes/poll?after=1&waitMs=30000", nil)
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			result <- pollResult{err: err}
			return
		}
		defer resp.Body.Close()
		raw, _ := io.ReadAll(resp.Body)
		var body map[string]any
		_ = json.Unmarshal(raw, &body)
		result <- pollResult{status: resp.StatusCode, body: body}
	}()

	time.Sleep(200 * time.Millisecond)

	terminated := make(chan int, 1)
	go func() { terminated <- p.terminate(t) }()

	select {
	case code := <-terminated:
		if code != 0 {
			t.Fatalf("exit code = %d, want 0", code)
		}
	case <-time.After(5 * time.Second):
		_ = p.cmd.Process.Kill()
		t.Fatal("shutdown waited for the session long poll instead of interrupting it")
	}

	select {
	case r := <-result:
		if r.err != nil {
			t.Fatalf("session poll connection error = %v, want 503 response", r.err)
		}
		if r.status != http.StatusServiceUnavailable {
			t.Fatalf("session poll status on shutdown = %d, want 503, body = %v", r.status, r.body)
		}
		if r.body["error"] == nil {
			t.Fatalf("session poll body = %v, want JSON error", r.body)
		}
		if r.body["changes"] != nil || r.body["timedOut"] != nil {
			t.Fatalf("503 leaked page content: %v", r.body)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("parked session poll did not return after shutdown")
	}
}
