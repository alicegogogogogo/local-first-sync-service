package server

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicegogogogogo/local-first-sync-service/internal/store"
)

func newPollServer(t *testing.T) (*httptest.Server, *store.Store) {
	t.Helper()
	s, err := store.Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	h := NewHandler(s)
	ts := httptest.NewServer(h)
	// LIFO: the store closes first (waking long polls), then the server.
	t.Cleanup(ts.Close)
	t.Cleanup(func() { _ = s.Close() })
	return ts, s
}

func pollGet(t *testing.T, ts *httptest.Server, rawQuery string, ctx context.Context) (int, map[string]any, error) {
	t.Helper()
	if ctx == nil {
		ctx = context.Background()
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, ts.URL+"/v1/documents/poll-doc/changes/poll?"+rawQuery, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	var body map[string]any
	_ = json.Unmarshal(raw, &body)
	return resp.StatusCode, body, nil
}

func pollPostChange(t *testing.T, ts *httptest.Server, doc, id string, payload any) {
	t.Helper()
	raw, _ := json.Marshal(map[string]any{
		"deviceId": "dev",
		"changes":  []any{map[string]any{"id": id, "payload": payload}},
	})
	resp, err := http.Post(ts.URL+"/v1/documents/"+doc+"/changes", "application/json", strings.NewReader(string(raw)))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("post change status = %d", resp.StatusCode)
	}
}

// Data already present returns immediately with timedOut=false, even when the
// caller offers a long wait.
func TestPollReturnsImmediatelyWhenChangesExist(t *testing.T) {
	ts, _ := newPollServer(t)
	pollPostChange(t, ts, "poll-doc", "c1", map[string]any{"n": 1})
	pollPostChange(t, ts, "poll-doc", "c2", map[string]any{"n": 2})

	start := time.Now()
	status, body, err := pollGet(t, ts, "after=0&limit=1&waitMs=30000", nil)
	if err != nil {
		t.Fatal(err)
	}
	if time.Since(start) > 2*time.Second {
		t.Fatalf("immediate poll blocked for %v", time.Since(start))
	}
	if status != http.StatusOK {
		t.Fatalf("status = %d body = %v", status, body)
	}
	if body["timedOut"] != false {
		t.Fatalf("timedOut = %v, want false", body["timedOut"])
	}
	list := body["changes"].([]any)
	if len(list) != 1 || list[0].(map[string]any)["id"] != "c1" {
		t.Fatalf("page = %v", list)
	}
	if body["nextCursor"].(float64) != 1 {
		t.Fatalf("nextCursor = %v", body["nextCursor"])
	}
}

// An unknown document never waits: empty list, cursor 0, timedOut=false.
func TestPollUnknownDocumentReturnsEmptyImmediately(t *testing.T) {
	ts, _ := newPollServer(t)
	req, _ := http.NewRequest(http.MethodGet, ts.URL+"/v1/documents/ghost/changes/poll?after=9&waitMs=5000", nil)
	start := time.Now()
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if time.Since(start) > 2*time.Second {
		t.Fatalf("unknown-doc poll blocked for %v", time.Since(start))
	}
	raw, _ := io.ReadAll(resp.Body)
	var body map[string]any
	if err := json.Unmarshal(raw, &body); err != nil {
		t.Fatalf("body = %q", raw)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	if len(body["changes"].([]any)) != 0 || body["nextCursor"].(float64) != 0 || body["timedOut"] != false {
		t.Fatalf("unknown doc body = %v", body)
	}
}

// A known document at its tail waits; the first committed change wakes it.
func TestPollWaitsAndWakesOnNewChange(t *testing.T) {
	ts, _ := newPollServer(t)
	pollPostChange(t, ts, "poll-doc", "c1", map[string]any{"n": 1})

	type pollResult struct {
		status int
		body   map[string]any
		err    error
	}
	done := make(chan pollResult, 1)
	go func() {
		st, b, e := pollGet(t, ts, "after=1&limit=100&waitMs=5000", nil)
		done <- pollResult{st, b, e}
	}()

	// Let the long poll settle in to waiting, then commit the wake.
	time.Sleep(150 * time.Millisecond)
	pollPostChange(t, ts, "poll-doc", "c2", map[string]any{"n": 2})

	select {
	case res := <-done:
		if res.err != nil {
			t.Fatal(res.err)
		}
		if res.status != http.StatusOK {
			t.Fatalf("status = %d body = %v", res.status, res.body)
		}
		if res.body["timedOut"] != false {
			t.Fatalf("timedOut = %v, want false", res.body["timedOut"])
		}
		list := res.body["changes"].([]any)
		if len(list) != 1 || list[0].(map[string]any)["id"] != "c2" {
			t.Fatalf("woken page = %v", list)
		}
		if res.body["nextCursor"].(float64) != 2 {
			t.Fatalf("nextCursor = %v, want 2", res.body["nextCursor"])
		}
	case <-time.After(3 * time.Second):
		t.Fatal("long poll did not wake on the new commit")
	}
}

// A merge and a restore are change-log commits too and must wake a long poll.
func TestPollWakesOnMergeAndRestore(t *testing.T) {
	ts, _ := newPollServer(t)
	pollPostChange(t, ts, "poll-doc", "c1", map[string]any{"a": 1})

	wait := make(chan map[string]any, 1)
	go func() {
		_, body, err := pollGet(t, ts, "after=1&waitMs=5000", nil)
		wait <- body
		_ = err
	}()
	time.Sleep(100 * time.Millisecond)

	// Merge a second change.
	merge, _ := json.Marshal(map[string]any{
		"deviceId":   "dev",
		"baseCursor": 1,
		"change":     map[string]any{"id": "c2", "payload": map[string]any{"b": 2}},
	})
	resp, err := http.Post(ts.URL+"/v1/documents/poll-doc/merge", "application/json", strings.NewReader(string(merge)))
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("merge status = %d body = %s", resp.StatusCode, raw)
	}

	select {
	case body := <-wait:
		list := body["changes"].([]any)
		if len(list) != 1 || list[0].(map[string]any)["id"] != "c2" {
			t.Fatalf("merge wake page = %v", list)
		}
		if body["timedOut"] != false {
			t.Fatalf("timedOut = %v", body["timedOut"])
		}
	case <-time.After(3 * time.Second):
		t.Fatal("long poll did not wake on merge")
	}

	// Snapshot at cursor 2, then a restore must wake the next poll.
	snap, _ := json.Marshal(map[string]any{"cursor": 2, "state": map[string]any{"x": 1}})
	sresp, err := http.Post(ts.URL+"/v1/documents/poll-doc/snapshots", "application/json", strings.NewReader(string(snap)))
	if err != nil {
		t.Fatal(err)
	}
	sresp.Body.Close()

	wait2 := make(chan map[string]any, 1)
	go func() {
		_, body, err := pollGet(t, ts, "after=2&waitMs=5000", nil)
		wait2 <- body
		_ = err
	}()
	time.Sleep(100 * time.Millisecond)
	restore, _ := json.Marshal(map[string]any{
		"deviceId": "dev", "changeId": "r1", "snapshotCursor": 2,
	})
	rresp, err := http.Post(ts.URL+"/v1/documents/poll-doc/restore", "application/json", strings.NewReader(string(restore)))
	if err != nil {
		t.Fatal(err)
	}
	rresp.Body.Close()

	select {
	case body := <-wait2:
		list := body["changes"].([]any)
		if len(list) != 1 || list[0].(map[string]any)["id"] != "r1" || body["nextCursor"].(float64) != 3 {
			t.Fatalf("restore wake page = %v", body)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("long poll did not wake on restore")
	}
}

// Expiry returns an empty list, the original cursor and timedOut=true; the
// cursor is not advanced by waiting.
func TestPollTimeoutKeepsCursor(t *testing.T) {
	ts, _ := newPollServer(t)
	pollPostChange(t, ts, "poll-doc", "c1", map[string]any{"n": 1})

	start := time.Now()
	status, body, err := pollGet(t, ts, "after=1&waitMs=120", nil)
	if err != nil {
		t.Fatal(err)
	}
	elapsed := time.Since(start)
	if elapsed < 80*time.Millisecond || elapsed > 3*time.Second {
		t.Fatalf("poll returned after %v, want ~120ms", elapsed)
	}
	if status != http.StatusOK {
		t.Fatalf("status = %d", status)
	}
	if len(body["changes"].([]any)) != 0 || body["nextCursor"].(float64) != 1 || body["timedOut"] != true {
		t.Fatalf("timeout body = %v", body)
	}
}

// waitMs omitted or zero on a caught-up known document ends immediately as a
// timeout with the unchanged cursor.
func TestPollZeroWaitDoesNotBlock(t *testing.T) {
	ts, _ := newPollServer(t)
	pollPostChange(t, ts, "poll-doc", "c1", map[string]any{"n": 1})

	for _, q := range []string{"after=1", "after=1&waitMs=0"} {
		start := time.Now()
		status, body, err := pollGet(t, ts, q, nil)
		if err != nil {
			t.Fatal(err)
		}
		if time.Since(start) > time.Second {
			t.Fatalf("%s blocked for %v", q, time.Since(start))
		}
		if status != http.StatusOK || body["timedOut"] != true || body["nextCursor"].(float64) != 1 {
			t.Fatalf("%s body = %v", q, body)
		}
	}
}

func TestPollRejectsBadParams(t *testing.T) {
	ts, _ := newPollServer(t)
	bad := []string{
		"after=-1",
		"after=x",
		"limit=0",
		"limit=1001",
		"waitMs=-1",
		"waitMs=30001",
		"waitMs=abc",
		"waitMs=1.5",
		"after=1&waitMs=99999999999999999999",
	}
	for _, q := range bad {
		resp, err := http.Get(ts.URL + "/v1/documents/doc/changes/poll?" + q)
		if err != nil {
			t.Fatal(err)
		}
		raw, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("%s status = %d, want 400, body = %s", q, resp.StatusCode, raw)
		}
		if ct := resp.Header.Get("Content-Type"); ct != "application/json" {
			t.Fatalf("%s content type = %q", q, ct)
		}
		var b map[string]string
		if err := json.Unmarshal(raw, &b); err != nil || b["error"] == "" {
			t.Fatalf("%s body = %q", q, raw)
		}
	}
}

// Empty/missing path segments and the wrong verb are 400 JSON, never HTML.
func TestPollPathAndMethodErrors(t *testing.T) {
	ts, _ := newPollServer(t)
	cases := []struct {
		method string
		path   string
	}{
		{http.MethodGet, "/v1/documents//changes/poll"},
		{http.MethodGet, "/v1/documents/d/changes//poll"},
		{http.MethodGet, "/v1/documents/d/changes/poll/"},
		{http.MethodPost, "/v1/documents/d/changes/poll"},
		{http.MethodGet, "/v1/documents//replay"},
		{http.MethodGet, "/v1/documents/d/replay"},
		{http.MethodGet, "/v1/documents/d/replay/"},
	}
	for _, tc := range cases {
		req, _ := http.NewRequest(tc.method, ts.URL+tc.path, nil)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		raw, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("%s %s status = %d, want 400, body = %s", tc.method, tc.path, resp.StatusCode, raw)
		}
		if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
			t.Fatalf("%s %s content type = %q, want JSON", tc.method, tc.path, ct)
		}
		if strings.Contains(string(raw), "<html") || strings.Contains(string(raw), "a href") {
			t.Fatalf("%s %s returned HTML/redirect body: %s", tc.method, tc.path, raw)
		}
	}
}

// A client disconnect cancels the wait without leaving any state, and the
// store keeps serving commits from other clients.
func TestPollClientDisconnectCancelsWait(t *testing.T) {
	ts, s := newPollServer(t)
	pollPostChange(t, ts, "poll-doc", "c1", map[string]any{"n": 1})

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		_, _, e := pollGet(t, ts, "after=1&waitMs=30000", ctx)
		errCh <- e
	}()
	time.Sleep(150 * time.Millisecond)
	cancel()

	select {
	case err := <-errCh:
		if err == nil {
			t.Fatal("canceled poll returned no error")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("canceled poll did not return")
	}

	// The canceled wait recorded nothing: still only the seeded change.
	rows, next, err := s.ListChanges("poll-doc", 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || next != 1 {
		t.Fatalf("state after canceled wait: rows=%d next=%d", len(rows), next)
	}

	// Liveness: a normal commit still lands right after the cancellation.
	pollPostChange(t, ts, "poll-doc", "c2", map[string]any{"n": 2})
	rows, next, _ = s.ListChanges("poll-doc", 0, 100)
	if len(rows) != 2 || next != 2 {
		t.Fatalf("commit after cancel: rows=%d next=%d", len(rows), next)
	}
}

// Many concurrent long polls all wake from one commit, each reading the page.
func TestPollMultipleWaitersWake(t *testing.T) {
	ts, _ := newPollServer(t)
	pollPostChange(t, ts, "poll-doc", "c1", map[string]any{"n": 1})

	const waiters = 20
	var wg sync.WaitGroup
	errs := make(chan error, waiters)
	for i := 0; i < waiters; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			status, body, err := pollGet(t, ts, "after=1&waitMs=5000", nil)
			if err != nil {
				errs <- err
				return
			}
			if status != http.StatusOK || body["timedOut"] != false {
				errs <- fmt.Errorf("bad poll result: %d %v", status, body)
				return
			}
			if len(body["changes"].([]any)) != 1 {
				errs <- fmt.Errorf("woken page = %v", body["changes"])
			}
		}()
	}
	time.Sleep(200 * time.Millisecond)
	pollPostChange(t, ts, "poll-doc", "c2", map[string]any{"n": 2})

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("not all waiters woke")
	}
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}
