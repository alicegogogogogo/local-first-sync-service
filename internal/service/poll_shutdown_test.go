package service_test

import (
	"encoding/json"
	"io"
	"net/http"
	"path/filepath"
	"testing"
	"time"
)

// A parked long poll must be canceled when the service shuts down: SIGTERM
// interrupts the wait, the parked request answers a JSON 503, and the process
// exits zero promptly instead of holding the graceful drain until the waitMs
// deadline. No change or cursor is written by the canceled wait.
func TestProcessShutdownInterruptsLongPoll(t *testing.T) {
	dataPath := filepath.Join(t.TempDir(), "sync.db")
	p := startProcess(t, "SYNC_ADDR=127.0.0.1:0", "SYNC_DATA="+dataPath)

	// Seed one change so the document is known and a poll after cursor 1 parks.
	if status, body := processJSON(t, http.MethodPost, "http://"+p.addr+"/v1/documents/doc/changes",
		map[string]any{
			"deviceId": "dev",
			"changes":  []any{map[string]any{"id": "c1", "payload": map[string]any{"n": 1}}},
		}); status != http.StatusOK || body["results"] == nil {
		t.Fatalf("seed change: %d %v", status, body)
	}

	type pollResult struct {
		status int
		body   map[string]any
		err    error
	}
	result := make(chan pollResult, 1)
	req, err := http.NewRequest(http.MethodGet,
		"http://"+p.addr+"/v1/documents/doc/changes/poll?after=1&waitMs=30000", nil)
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

	// Let the poll park before signaling shutdown.
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
		t.Fatal("shutdown waited for the long poll instead of interrupting it")
	}

	select {
	case r := <-result:
		if r.err != nil {
			// A hard connection drop is also a cancellation outcome, but the
			// implemented contract answers 503 before draining, so require it.
			t.Fatalf("poll connection error = %v, want 503 response", r.err)
		}
		if r.status != http.StatusServiceUnavailable {
			t.Fatalf("poll status on shutdown = %d, want 503, body = %v", r.status, r.body)
		}
		if r.body["error"] == nil {
			t.Fatalf("poll body = %v, want JSON error", r.body)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("parked poll did not return after shutdown")
	}
}
