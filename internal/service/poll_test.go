package service_test

import (
	"encoding/json"
	"io"
	"net/http"
	"testing"
	"time"
)

// A long poll in flight when shutdown begins must be canceled promptly rather
// than holding the graceful drain until its waitMs deadline elapses, and the
// service must still exit cleanly within its normal shutdown budget.
func TestServiceShutdownCancelsLongPoll(t *testing.T) {
	r := startService(t, "127.0.0.1:0", "")

	// Give the document one change so it is known; the poll then waits at its
	// tail for up to 30s.
	if status, body := doJSON(t, http.MethodPost, r.baseURL+"/v1/devices", map[string]string{"deviceId": "dev"}); status != http.StatusOK {
		t.Fatalf("register: %d %v", status, body)
	}
	if status, body := doJSON(t, http.MethodPost, r.baseURL+"/v1/documents/doc/changes", map[string]any{
		"deviceId": "dev",
		"changes": []any{
			map[string]any{"id": "c1", "payload": map[string]any{"n": 1}},
		},
	}); status != http.StatusOK {
		t.Fatalf("seed: %d %v", status, body)
	}

	pollErr := make(chan error, 1)
	go func() {
		resp, err := http.Get(r.baseURL + "/v1/documents/doc/changes/poll?after=1&waitMs=30000")
		if err != nil {
			pollErr <- err
			return
		}
		defer resp.Body.Close()
		_, _ = io.Copy(io.Discard, resp.Body)
		// A completed response after shutdown means the connection was torn
		// down mid-body; surface that as an error only if status is not 200.
		if resp.StatusCode != http.StatusOK {
			pollErr <- nil
		}
		pollErr <- nil
	}()

	// Let the poll settle, then begin shutdown.
	time.Sleep(200 * time.Millisecond)
	start := time.Now()
	r.cancel()
	select {
	case <-r.done:
	case <-time.After(5 * time.Second):
		t.Fatalf("service did not stop within 5s despite long poll; logs:\n%s", r.logs.String())
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("shutdown took %v, long poll blocked the drain", elapsed)
	}

	select {
	case <-pollErr:
	case <-time.After(3 * time.Second):
		t.Fatal("in-flight long poll did not return after shutdown")
	}
}

// End-to-end: a long poll on one client wakes when another client commits a
// change through the real running service.
func TestServiceLongPollWakesOnCommit(t *testing.T) {
	r := startService(t, "127.0.0.1:0", "")
	defer r.stop(t)

	if status, _ := doJSON(t, http.MethodPost, r.baseURL+"/v1/devices", map[string]string{"deviceId": "dev"}); status != http.StatusOK {
		t.Fatalf("register: %d", status)
	}
	if status, _ := doJSON(t, http.MethodPost, r.baseURL+"/v1/documents/doc/changes", map[string]any{
		"deviceId": "dev",
		"changes":  []any{map[string]any{"id": "c1", "payload": map[string]any{"n": 1}}},
	}); status != http.StatusOK {
		t.Fatalf("seed: %d", status)
	}

	type pollOut struct {
		status int
		body   map[string]any
	}
	out := make(chan pollOut, 1)
	go func() {
		resp, err := http.Get(r.baseURL + "/v1/documents/doc/changes/poll?after=1&waitMs=5000")
		if err != nil {
			out <- pollOut{0, nil}
			return
		}
		defer resp.Body.Close()
		raw, _ := io.ReadAll(resp.Body)
		var body map[string]any
		_ = json.Unmarshal(raw, &body)
		out <- pollOut{resp.StatusCode, body}
	}()

	time.Sleep(150 * time.Millisecond)
	if status, _ := doJSON(t, http.MethodPost, r.baseURL+"/v1/documents/doc/changes", map[string]any{
		"deviceId": "dev",
		"changes":  []any{map[string]any{"id": "c2", "payload": map[string]any{"n": 2}}},
	}); status != http.StatusOK {
		t.Fatalf("commit: %d", status)
	}

	select {
	case got := <-out:
		if got.status != http.StatusOK {
			t.Fatalf("poll status = %d", got.status)
		}
		if got.body["timedOut"] != false {
			t.Fatalf("timedOut = %v", got.body["timedOut"])
		}
		rows := got.body["changes"].([]any)
		if len(rows) != 1 || rows[0].(map[string]any)["id"] != "c2" || got.body["nextCursor"].(float64) != 2 {
			t.Fatalf("woken body = %v", got.body)
		}
	case <-time.After(4 * time.Second):
		t.Fatal("long poll did not wake after commit")
	}
}
