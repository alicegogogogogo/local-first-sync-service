package server

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicegogogogogo/local-first-sync-service/internal/app"
)

// deleteDocument issues the document-level data-clearing call. The calling
// device travels in the query parameter, exactly as on the document-level
// subscription handshakes.
func deleteDocument(t *testing.T, h http.Handler, documentID, deviceID string) (*httptest.ResponseRecorder, map[string]any) {
	t.Helper()
	return doRequest(t, h, http.MethodDelete,
		"/v1/documents/"+documentID+"?deviceId="+deviceID)
}

// deleteDocumentHTTP issues the same call against a real test server, for
// WebSocket and timing tests that cannot use a ResponseRecorder.
func deleteDocumentHTTP(t *testing.T, srv *httptest.Server, documentID, deviceID string) int {
	t.Helper()
	req, err := http.NewRequest(http.MethodDelete,
		srv.URL+"/v1/documents/"+documentID+"?deviceId="+deviceID, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatalf("DELETE document: %v", err)
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	return resp.StatusCode
}

// TestDeleteDocumentHTTPSuccessShape: one compact JSON line naming the document
// and the deletion marker and nothing else, with a trailing newline.
func TestDeleteDocumentHTTPSuccessShape(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	postDocChanges(t, h, "doc-1", "dev-1", 1)

	w, body := deleteDocument(t, h, "doc-1", "dev-1")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", w.Code, w.Body.String())
	}
	if body["documentId"] != "doc-1" || body["deleted"] != true || len(body) != 2 {
		t.Fatalf("body = %v, want exactly {documentId, deleted}", body)
	}
	if got := w.Body.String(); got != "{\"documentId\":\"doc-1\",\"deleted\":true}\n" {
		t.Fatalf("body = %q, want one compact JSON line plus newline", got)
	}
}

func TestDeleteDocumentHTTPUnknownRepeatAndDevice404(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")

	// Never-created document: 404 JSON, zero writes.
	w, body := deleteDocument(t, h, "ghost", "dev-1")
	if w.Code != http.StatusNotFound || body["error"] == nil {
		t.Fatalf("unknown document = %d %v", w.Code, body)
	}

	// A missing or empty deviceId query parameter is indistinguishable from an
	// unregistered caller: 404 JSON.
	for _, target := range []string{
		"/v1/documents/ghost",
		"/v1/documents/ghost?deviceId=",
	} {
		rec := serveRecorder(h, newJSONRequest(http.MethodDelete, target, "", ""))
		if rec.Code != http.StatusNotFound {
			t.Fatalf("DELETE %s = %d, want 404", target, rec.Code)
		}
		assertJSONError(t, rec)
	}

	// An unregistered device gets the same 404.
	w, body = deleteDocument(t, h, "ghost", "nobody")
	if w.Code != http.StatusNotFound || body["error"] == nil {
		t.Fatalf("unregistered device = %d %v", w.Code, body)
	}

	postDocChanges(t, h, "doc-1", "dev-1", 1)
	if w, _ = deleteDocument(t, h, "doc-1", "dev-1"); w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	// A second delete misses exactly like a never-created document.
	w, body = deleteDocument(t, h, "doc-1", "dev-1")
	if w.Code != http.StatusNotFound || body["error"] == nil {
		t.Fatalf("repeat delete = %d %v, want 404 JSON", w.Code, body)
	}
}

// TestDeleteDocumentHTTPRevoked403: the permission verdict comes after the
// shape and device checks but before the document existence verdict, and a
// rejected call clears nothing.
func TestDeleteDocumentHTTPRevoked403(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	registerDevice(t, h, "dev-2")
	postDocChanges(t, h, "doc-1", "dev-1", 2)

	w, _ := postJSON(t, h, "/v1/documents/doc-1/permissions",
		map[string]any{"deviceId": "dev-1", "action": "revoke"})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}

	// Revoked caller: 403 even though the document very much exists.
	w, body := deleteDocument(t, h, "doc-1", "dev-1")
	if w.Code != http.StatusForbidden || body["error"] == nil {
		t.Fatalf("revoked delete = %d %v", w.Code, body)
	}
	// Revoke on a document with no other data: the revoke row is its only
	// durable trace, but the gate runs first, so the revoked caller still sees
	// 403 rather than learning the document has no other content.
	if w, _ := postJSON(t, h, "/v1/documents/ghost/permissions",
		map[string]any{"deviceId": "dev-1", "action": "revoke"}); w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	w, body = deleteDocument(t, h, "ghost", "dev-1")
	if w.Code != http.StatusForbidden || body["error"] == nil {
		t.Fatalf("revoked delete of ledger-only doc = %d %v", w.Code, body)
	}

	// Zero writes: an authorized caller still sees both changes.
	w, body = doRequest(t, h, http.MethodGet, "/v1/documents/doc-1/changes")
	if w.Code != http.StatusOK || len(body["changes"].([]any)) != 2 {
		t.Fatalf("document changed by a rejected delete: %d %v", w.Code, body)
	}

	// An authorized device performs the delete; the revoked status of dev-1 is
	// removed with the document's ledger.
	if w, _ = deleteDocument(t, h, "doc-1", "dev-2"); w.Code != http.StatusOK {
		t.Fatalf("authorized delete = %d %s", w.Code, w.Body.String())
	}
}

// TestDeleteDocumentHTTPRejectsBadShape: every shape failure is a JSON 400,
// never a redirect or HTML, and precedes the device/permission/document
// verdicts.
func TestDeleteDocumentHTTPRejectsBadShape(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	postDocChanges(t, h, "doc-1", "dev-1", 1)

	cases := []struct {
		method string
		path   string
	}{
		// Method mismatch on the exact item path.
		{http.MethodGet, "/v1/documents/doc-1?deviceId=dev-1"},
		{http.MethodPost, "/v1/documents/doc-1"},
		{http.MethodPut, "/v1/documents/doc-1"},
		{http.MethodPatch, "/v1/documents/doc-1"},
		// Missing id segment, empty segment, trailing slash.
		{http.MethodDelete, "/v1/documents"},
		{http.MethodDelete, "/v1/documents/"},
		{http.MethodDelete, "/v1/documents//"},
		{http.MethodDelete, "/v1/documents/doc-1/"},
		// Extra segments.
		{http.MethodDelete, "/v1/documents/doc-1/extra"},
		{http.MethodDelete, "/v1/documents/doc-1/extra/more"},
		// A DELETE on an existing document subresource keeps that route's 400.
		{http.MethodDelete, "/v1/documents/doc-1/changes"},
		{http.MethodDelete, "/v1/documents/doc-1/permissions"},
		{http.MethodDelete, "/v1/documents/doc-1/crdt/state"},
	}
	for _, tc := range cases {
		w := serveRecorder(h, newJSONRequest(tc.method, tc.path, "", ""))
		if w.Code != http.StatusBadRequest {
			t.Fatalf("%s %s = %d, want 400", tc.method, tc.path, w.Code)
		}
		assertJSONError(t, w)
	}

	// Shape beats the device verdict: a bad shape with no registered caller is
	// still a 400, not a 404.
	w := serveRecorder(h, newJSONRequest(http.MethodGet,
		"/v1/documents/doc-1?deviceId=ghost", "", ""))
	if w.Code != http.StatusBadRequest {
		t.Fatalf("shape should precede device lookup: %d", w.Code)
	}

	// Zero writes: the document still reads with its change.
	w, body := doRequest(t, h, http.MethodGet, "/v1/documents/doc-1/changes")
	if w.Code != http.StatusOK || len(body["changes"].([]any)) != 1 {
		t.Fatalf("document changed by rejected deletes: %d %v", w.Code, body)
	}
}

// TestDeleteDocumentHTTPCascade wipes every layer and resets the name to a
// brand-new document: the cursor space restarts at 1, compacted identities are
// forgotten, snapshots/restores and CRDT state are gone, and the permission
// ledger starts from defaults again.
func TestDeleteDocumentHTTPCascade(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	registerDevice(t, h, "dev-2")

	// Three changes; snapshot at cursor 2; compaction trims c1,c2 keeping
	// their idempotency summaries; a restore appends r1.
	postDocChanges(t, h, "doc", "dev-1", 3)
	if w, _ := postJSON(t, h, "/v1/documents/doc/snapshots",
		map[string]any{"cursor": 2, "state": map[string]any{"s": 1}}); w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	if w := compactChanges(t, h, "doc", "dev-1"); w.Code != http.StatusOK {
		t.Fatalf("compact = %d %s", w.Code, w.Body.String())
	}
	if w, _ := postJSON(t, h, "/v1/documents/doc/restore", map[string]any{
		"deviceId":       "dev-1",
		"changeId":       "r1",
		"snapshotCursor": 2,
	}); w.Code != http.StatusOK {
		t.Fatalf("restore = %d %s", w.Code, w.Body.String())
	}
	// A CRDT counter and a permission deviation.
	if w, _ := postJSON(t, h, "/v1/documents/doc/crdt/ops", map[string]any{
		"deviceId": "dev-1",
		"type":     "counter",
		"ops":      []any{map[string]any{"id": "op-1", "value": 5}},
	}); w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	if w, _ := postJSON(t, h, "/v1/documents/doc/permissions",
		map[string]any{"deviceId": "dev-2", "action": "revoke"}); w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}

	if w, _ := deleteDocument(t, h, "doc", "dev-1"); w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}

	// Change log: empty page with nextCursor 0 like any unknown document.
	w, body := doRequest(t, h, http.MethodGet, "/v1/documents/doc/changes")
	if w.Code != http.StatusOK {
		t.Fatalf("changes after delete = %d", w.Code)
	}
	if changes := body["changes"].([]any); len(changes) != 0 || body["nextCursor"].(float64) != 0 {
		t.Fatalf("changes after delete = %v", body)
	}
	// Snapshot and restore provenance gone.
	if w, _ := doRequest(t, h, http.MethodGet, "/v1/documents/doc/snapshots/2"); w.Code != http.StatusNotFound {
		t.Fatalf("snapshot after delete = %d, want 404", w.Code)
	}
	// CRDT state gone (type row, ops, counter values and identities).
	if w, _ := doRequest(t, h, http.MethodGet, "/v1/documents/doc/crdt/state"); w.Code != http.StatusNotFound {
		t.Fatalf("crdt state after delete = %d, want 404", w.Code)
	}

	// The same name is a brand-new document: the old change ids are new again
	// and cursors restart at 1 (compaction summaries forgotten).
	w, body = postJSON(t, h, "/v1/documents/doc/changes", map[string]any{
		"deviceId": "dev-2",
		"changes":  []any{map[string]any{"id": "c1", "payload": map[string]any{"fresh": true}}},
	})
	if w.Code != http.StatusOK {
		t.Fatalf("recreate with old id = %d %s", w.Code, w.Body.String())
	}
	results := body["results"].([]any)
	r0 := results[0].(map[string]any)
	if r0["created"] != true || r0["cursor"].(float64) != 1 {
		t.Fatalf("recreated cursor = %v, want created cursor 1", r0)
	}
	// A compacted-away id from the old incarnation is not an idempotent replay.
	w, body = postJSON(t, h, "/v1/documents/doc/changes", map[string]any{
		"deviceId": "dev-2",
		"changes":  []any{map[string]any{"id": "c2", "payload": "new"}},
	})
	if w.Code != http.StatusOK || body["results"].([]any)[0].(map[string]any)["created"] != true {
		t.Fatalf("old trimmed id c2 not fresh: %d %v", w.Code, body)
	}
	// The CRDT type is not fixed anymore: a different first batch is accepted.
	if w, _ = postJSON(t, h, "/v1/documents/doc/crdt/ops", map[string]any{
		"deviceId": "dev-2",
		"type":     "gset",
		"ops":      []any{map[string]any{"id": "op-1", "elements": []any{"apple"}}},
	}); w.Code != http.StatusOK {
		t.Fatalf("crdt type should reset after delete: %d %s", w.Code, w.Body.String())
	}
	// dev-2's old revoke vanished with the ledger: it now writes by default.
	w, body = postJSON(t, h, "/v1/documents/doc/replay", map[string]any{
		"deviceId":   "dev-2",
		"operations": []any{map[string]any{"id": "x", "payload": 1}},
	})
	if w.Code != http.StatusOK {
		t.Fatalf("old revoke survived the delete: %d %v", w.Code, body)
	}
}

// TestDeleteDocumentHTTPEveryLayerIsExistence: a document with only CRDT state,
// or only a permission ledger row, is still known and deletes successfully.
func TestDeleteDocumentHTTPEveryLayerIsExistence(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	registerDevice(t, h, "dev-2")

	// CRDT-only document: no change rows at all.
	if w, _ := postJSON(t, h, "/v1/documents/crdt-only/crdt/ops", map[string]any{
		"deviceId": "dev-1",
		"type":     "counter",
		"ops":      []any{map[string]any{"id": "op-1", "value": 1}},
	}); w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	if w, _ := deleteDocument(t, h, "crdt-only", "dev-1"); w.Code != http.StatusOK {
		t.Fatalf("delete crdt-only doc = %d %s", w.Code, w.Body.String())
	}
	if w, _ := doRequest(t, h, http.MethodGet, "/v1/documents/crdt-only/crdt/state"); w.Code != http.StatusNotFound {
		t.Fatalf("crdt state survives delete: %d", w.Code)
	}
	if w, _ := deleteDocument(t, h, "crdt-only", "dev-1"); w.Code != http.StatusNotFound {
		t.Fatalf("repeat delete crdt-only = %d, want 404", w.Code)
	}

	// Ledger-only document: a revoke is its only row.
	if w, _ := postJSON(t, h, "/v1/documents/ledger-only/permissions",
		map[string]any{"deviceId": "dev-1", "action": "revoke"}); w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	if w, _ := deleteDocument(t, h, "ledger-only", "dev-2"); w.Code != http.StatusOK {
		t.Fatalf("delete ledger-only doc = %d %s", w.Code, w.Body.String())
	}
	// The revoke row vanished: dev-1 is back to the default authorized state,
	// and a repeat delete finds no trace of the document.
	if w, _ := postJSON(t, h, "/v1/documents/ledger-only/changes", map[string]any{
		"deviceId": "dev-1",
		"changes":  []any{map[string]any{"id": "c", "payload": 1}},
	}); w.Code != http.StatusOK {
		t.Fatalf("old revoke survived delete: %d %s", w.Code, w.Body.String())
	}
}

// TestDeleteDocumentHTTPOtherDocumentsUnaffected: clearing one document leaves
// another's change log, snapshots, CRDT state, permissions and attachments
// behavior exactly as they were.
func TestDeleteDocumentHTTPOtherDocumentsUnaffected(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	postDocChanges(t, h, "doc-a", "dev-1", 2)
	postDocChanges(t, h, "doc-b", "dev-1", 1)
	if w, _ := postJSON(t, h, "/v1/documents/doc-b/crdt/ops", map[string]any{
		"deviceId": "dev-1",
		"type":     "gset",
		"ops":      []any{map[string]any{"id": "op-b", "elements": []any{"keep"}}},
	}); w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}

	if w, _ := deleteDocument(t, h, "doc-a", "dev-1"); w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}

	w, body := doRequest(t, h, http.MethodGet, "/v1/documents/doc-b/changes")
	if w.Code != http.StatusOK || len(body["changes"].([]any)) != 1 {
		t.Fatalf("doc-b changes changed: %d %v", w.Code, body)
	}
	w, body = doRequest(t, h, http.MethodGet, "/v1/documents/doc-b/crdt/state")
	if w.Code != http.StatusOK || body["type"] != "gset" {
		t.Fatalf("doc-b crdt changed: %d %v", w.Code, body)
	}
	// doc-b's cursor space did not restart.
	w, body = postJSON(t, h, "/v1/documents/doc-b/changes", map[string]any{
		"deviceId": "dev-1",
		"changes":  []any{map[string]any{"id": "c2", "payload": 2}},
	})
	if w.Code != http.StatusOK || body["results"].([]any)[0].(map[string]any)["cursor"].(float64) != 2 {
		t.Fatalf("doc-b cursor space changed: %d %v", w.Code, body)
	}
}

func TestDeleteDocumentHTTPConcurrentAtMostOnce(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	postDocChanges(t, h, "hot", "dev-1", 1)

	const n = 40
	var wg sync.WaitGroup
	var mu sync.Mutex
	ok, miss := 0, 0
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			w := serveRecorder(h, newJSONRequest(http.MethodDelete,
				"/v1/documents/hot?deviceId=dev-1", "", ""))
			mu.Lock()
			defer mu.Unlock()
			switch w.Code {
			case http.StatusOK:
				ok++
			case http.StatusNotFound:
				miss++
			default:
				t.Errorf("status = %d body = %s", w.Code, w.Body.String())
			}
		}()
	}
	wg.Wait()
	if ok != 1 || miss != n-1 {
		t.Fatalf("ok = %d, miss = %d, want 1 and %d", ok, miss, n-1)
	}
}

func TestDeleteDocumentHTTPRestartPersists(t *testing.T) {
	path := filepath.Join(t.TempDir(), "doc-delete.db")
	s, err := app.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	h := NewHandler(s)
	registerDevice(t, h, "dev-1")
	postDocChanges(t, h, "doc", "dev-1", 2)
	if w, _ := postJSON(t, h, "/v1/documents/doc/crdt/ops", map[string]any{
		"deviceId": "dev-1",
		"type":     "counter",
		"ops":      []any{map[string]any{"id": "op-1", "value": 9}},
	}); w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	if w, _ := deleteDocument(t, h, "doc", "dev-1"); w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s2, err := app.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s2.Close() })
	h2 := NewHandler(s2)

	// The miss verdict is stable after a restart.
	w, body := deleteDocument(t, h2, "doc", "dev-1")
	if w.Code != http.StatusNotFound || body["error"] == nil {
		t.Fatalf("delete after restart = %d %v", w.Code, body)
	}
	w, body = doRequest(t, h2, http.MethodGet, "/v1/documents/doc/changes")
	if w.Code != http.StatusOK || len(body["changes"].([]any)) != 0 || body["nextCursor"].(float64) != 0 {
		t.Fatalf("changes after restart = %v", body)
	}
	if w, _ = doRequest(t, h2, http.MethodGet, "/v1/documents/doc/crdt/state"); w.Code != http.StatusNotFound {
		t.Fatalf("crdt state after restart = %d, want 404", w.Code)
	}
	// A same-named document starts over at cursor 1.
	w, body = postJSON(t, h2, "/v1/documents/doc/changes", map[string]any{
		"deviceId": "dev-1",
		"changes":  []any{map[string]any{"id": "c1", "payload": "again"}},
	})
	if w.Code != http.StatusOK || body["results"].([]any)[0].(map[string]any)["cursor"].(float64) != 1 {
		t.Fatalf("cursor after restart = %d %v", w.Code, body)
	}
}

// TestDeleteDocumentHTTPLongPollWakes: a parked poll on the document returns
// at once with an unknown-document empty page when the document is deleted,
// instead of waiting out its deadline.
func TestDeleteDocumentHTTPLongPollWakes(t *testing.T) {
	srv, _ := newWSTestServer(t)
	registerDeviceViaHTTP(t, srv, "dev-1")
	postDocChange(t, srv, "dev-1", "doc", "c1", map[string]any{"n": 1})

	type pollResult struct {
		body []byte
		code int
	}
	done := make(chan pollResult, 1)
	go func() {
		resp, err := srv.Client().Get(srv.URL + "/v1/documents/doc/changes/poll?after=1&limit=100&waitMs=30000")
		if err != nil {
			t.Errorf("poll: %v", err)
			return
		}
		defer resp.Body.Close()
		data, _ := io.ReadAll(resp.Body)
		done <- pollResult{body: data, code: resp.StatusCode}
	}()

	// Let the poll park, then delete.
	time.Sleep(150 * time.Millisecond)
	if code := deleteDocumentHTTP(t, srv, "doc", "dev-1"); code != http.StatusOK {
		t.Fatalf("delete = %d", code)
	}

	select {
	case res := <-done:
		if res.code != http.StatusOK {
			t.Fatalf("poll status = %d", res.code)
		}
		var page struct {
			Changes    []any `json:"changes"`
			NextCursor int64 `json:"nextCursor"`
			TimedOut   bool  `json:"timedOut"`
		}
		if err := json.Unmarshal(res.body, &page); err != nil {
			t.Fatalf("poll body = %q", res.body)
		}
		if len(page.Changes) != 0 || page.NextCursor != 0 {
			t.Fatalf("poll after delete = %+v, want empty page with cursor 0", page)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("parked poll was not woken by the document delete")
	}
}

// TestDeleteDocumentHTTPClosesSubscriptions: every live subscription on the
// document — session and document level, change-log and CRDT state — ends with
// 4420 right after the delete commits, including when another device deletes.
func TestDeleteDocumentHTTPClosesSubscriptions(t *testing.T) {
	t.Run("session change subscription", func(t *testing.T) {
		srv, _ := newWSTestServer(t)
		setupSession(t, srv, "dev-1", "sess", "doc", 1)
		conn, resp := dialWS(t, subscribeURL(srv, "sess", "doc", "0"))
		if conn == nil {
			t.Fatalf("status = %d", resp.StatusCode)
		}
		defer conn.close()
		if c := conn.readChange(); c.Cursor != 1 {
			t.Fatalf("seed frame = %+v", c)
		}
		if code := deleteDocumentHTTP(t, srv, "doc", "dev-1"); code != http.StatusOK {
			t.Fatalf("delete = %d", code)
		}
		conn.setReadDeadline(2 * time.Second)
		if code := conn.readCloseCode(); code != 4420 {
			t.Fatalf("close code = %d, want 4420", code)
		}
		conn.clearReadDeadline()
	})

	t.Run("document change subscription", func(t *testing.T) {
		srv, _ := newWSTestServer(t)
		registerAndSeedDevice(t, srv, "dev-1", "doc", 1)
		conn, resp := dialWS(t, docSubscribeURL(srv, "doc", "dev-1", "0"))
		if conn == nil {
			t.Fatalf("status = %d", resp.StatusCode)
		}
		defer conn.close()
		if c := conn.readChange(); c.Cursor != 1 {
			t.Fatalf("seed frame = %+v", c)
		}
		if code := deleteDocumentHTTP(t, srv, "doc", "dev-1"); code != http.StatusOK {
			t.Fatalf("delete = %d", code)
		}
		conn.setReadDeadline(2 * time.Second)
		if code := conn.readCloseCode(); code != 4420 {
			t.Fatalf("close code = %d, want 4420", code)
		}
		conn.clearReadDeadline()
	})

	t.Run("session crdt state subscription", func(t *testing.T) {
		srv, _ := newWSTestServer(t)
		setupSession(t, srv, "dev-1", "sess", "doc", 0)
		// The document must have durable CRDT state for the delete to hit.
		if code := postHTTP(t, srv, "/v1/documents/doc/crdt/ops", map[string]any{
			"deviceId": "dev-1",
			"type":     "counter",
			"ops":      []any{map[string]any{"id": "op-1", "value": 1}},
		}); code != http.StatusOK {
			t.Fatalf("seed crdt op = %d", code)
		}
		conn, resp := dialWS(t, crdtSubscribeURL(srv, "sess", "doc"))
		if conn == nil {
			t.Fatalf("status = %d", resp.StatusCode)
		}
		defer conn.close()
		if code := deleteDocumentHTTP(t, srv, "doc", "dev-1"); code != http.StatusOK {
			t.Fatalf("delete = %d", code)
		}
		conn.setReadDeadline(2 * time.Second)
		if code := conn.readCloseCode(); code != 4420 {
			t.Fatalf("close code = %d, want 4420", code)
		}
		conn.clearReadDeadline()
	})

	t.Run("document crdt state subscription", func(t *testing.T) {
		srv, _ := newWSTestServer(t)
		registerDeviceViaHTTP(t, srv, "dev-1")
		if code := postHTTP(t, srv, "/v1/documents/doc/crdt/ops", map[string]any{
			"deviceId": "dev-1",
			"type":     "gset",
			"ops":      []any{map[string]any{"id": "op-1", "elements": []any{"a"}}},
		}); code != http.StatusOK {
			t.Fatalf("seed crdt op = %d", code)
		}
		conn, resp := dialWS(t, docCRDTSubscribeURL(srv, "doc", "dev-1"))
		if conn == nil {
			t.Fatalf("status = %d", resp.StatusCode)
		}
		defer conn.close()
		if code := deleteDocumentHTTP(t, srv, "doc", "dev-1"); code != http.StatusOK {
			t.Fatalf("delete = %d", code)
		}
		conn.setReadDeadline(2 * time.Second)
		if code := conn.readCloseCode(); code != 4420 {
			t.Fatalf("close code = %d, want 4420", code)
		}
		conn.clearReadDeadline()
	})

	t.Run("closed by a different device", func(t *testing.T) {
		srv, _ := newWSTestServer(t)
		setupSession(t, srv, "dev-1", "sess", "doc", 1)
		registerDeviceViaHTTP(t, srv, "dev-2")
		conn, resp := dialWS(t, subscribeURL(srv, "sess", "doc", "0"))
		if conn == nil {
			t.Fatalf("status = %d", resp.StatusCode)
		}
		defer conn.close()
		_ = conn.readChange()
		if code := deleteDocumentHTTP(t, srv, "doc", "dev-2"); code != http.StatusOK {
			t.Fatalf("delete by other device = %d", code)
		}
		conn.setReadDeadline(2 * time.Second)
		if code := conn.readCloseCode(); code != 4420 {
			t.Fatalf("close code = %d, want 4420", code)
		}
		conn.clearReadDeadline()
	})
}

// TestDeleteDocumentHTTPSubscriptionIsolation: 4420 is document-scoped. While a
// subscription on the deleted document ends, a subscription on another document
// stays open and still receives pushes afterward.
func TestDeleteDocumentHTTPSubscriptionIsolation(t *testing.T) {
	srv, _ := newWSTestServer(t)
	setupSession(t, srv, "dev-1", "sess-a", "doc-a", 1)
	setupSession(t, srv, "dev-1", "sess-b", "doc-b", 1)

	connA, resp := dialWS(t, subscribeURL(srv, "sess-a", "doc-a", "0"))
	if connA == nil {
		t.Fatalf("A status = %d", resp.StatusCode)
	}
	defer connA.close()
	connB, resp := dialWS(t, subscribeURL(srv, "sess-b", "doc-b", "0"))
	if connB == nil {
		t.Fatalf("B status = %d", resp.StatusCode)
	}
	defer connB.close()
	if c := connA.readChange(); c.Cursor != 1 {
		t.Fatalf("A seed = %+v", c)
	}
	if c := connB.readChange(); c.Cursor != 1 {
		t.Fatalf("B seed = %+v", c)
	}

	if code := deleteDocumentHTTP(t, srv, "doc-a", "dev-1"); code != http.StatusOK {
		t.Fatalf("delete = %d", code)
	}

	connA.setReadDeadline(2 * time.Second)
	if code := connA.readCloseCode(); code != 4420 {
		t.Fatalf("doc-a close code = %d, want 4420", code)
	}
	connA.clearReadDeadline()

	// doc-b stays open: a later commit arrives, and no close frame does.
	postDocChange(t, srv, "dev-1", "doc-b", "c2", map[string]any{"n": 2})
	connB.setReadDeadline(2 * time.Second)
	fin, opcode, payload, ok := connB.readFrameMaybe()
	if !ok {
		t.Fatal("doc-b subscription received nothing after doc-a delete")
	}
	if !fin || opcode != wsOpcodeText {
		t.Fatalf("doc-b frame opcode=%d fin=%v payload=%q", opcode, fin, payload)
	}
	if !strings.Contains(string(payload), `"cursor":2`) {
		t.Fatalf("doc-b frame = %q, want cursor 2", payload)
	}
	// No close frame is pending for doc-b.
	fin, opcode, _, ok = connB.readFrameMaybe()
	if ok && opcode == wsOpcodeClose {
		t.Fatalf("doc-b was closed with code %d", closeCode(payload))
	}
	connB.clearReadDeadline()
}

// TestDeleteDocumentHTTPRecreatedDocRepushesFromCursor1: after 4420 a fresh
// subscription on the same name receives the new incarnation's changes starting
// at cursor 1.
func TestDeleteDocumentHTTPRecreatedDocRepushesFromCursor1(t *testing.T) {
	srv, _ := newWSTestServer(t)
	registerAndSeedDevice(t, srv, "dev-1", "doc", 2)
	conn, resp := dialWS(t, docSubscribeURL(srv, "doc", "dev-1", "0"))
	if conn == nil {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	if c := conn.readChange(); c.Cursor != 1 {
		t.Fatalf("seed 1 = %+v", c)
	}
	if c := conn.readChange(); c.Cursor != 2 {
		t.Fatalf("seed 2 = %+v", c)
	}
	if code := deleteDocumentHTTP(t, srv, "doc", "dev-1"); code != http.StatusOK {
		t.Fatalf("delete = %d", code)
	}
	conn.setReadDeadline(2 * time.Second)
	if code := conn.readCloseCode(); code != 4420 {
		t.Fatalf("close = %d, want 4420", code)
	}
	conn.clearReadDeadline()
	conn.close()

	postDocChange(t, srv, "dev-1", "doc", "fresh-1", map[string]any{"fresh": true})
	conn2, resp := dialWS(t, docSubscribeURL(srv, "doc", "dev-1", "0"))
	if conn2 == nil {
		t.Fatalf("resubscribe status = %d", resp.StatusCode)
	}
	defer conn2.close()
	conn2.setReadDeadline(2 * time.Second)
	if c := conn2.readChange(); c.Cursor != 1 || c.ID != "fresh-1" {
		t.Fatalf("recreated doc frame = %+v, want cursor 1 fresh-1", c)
	}
	conn2.clearReadDeadline()
}
