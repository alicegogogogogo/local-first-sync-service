package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicegogogogogo/local-first-sync-service/internal/app"
)

// deleteDocument issues the document deletion and returns status, content
// type and raw body.
func deleteDocument(t *testing.T, srv *httptest.Server, doc, device string) (int, string, string) {
	t.Helper()
	return rawRequest(t, srv, http.MethodDelete,
		"/v1/documents/"+doc+"?deviceId="+device)
}

// Successful deletion answers one compact JSON line plus a trailing newline
// naming exactly the document id and the deletion marker.
func TestDeleteDocumentSuccessShape(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	w, _ := postJSON(t, h, "/v1/documents/doc-1/changes", map[string]any{
		"deviceId": "dev-1",
		"changes":  []any{map[string]any{"id": "c1", "payload": map[string]any{"n": 1}}},
	})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}

	w, _ = doRequest(t, h, http.MethodDelete, "/v1/documents/doc-1?deviceId=dev-1")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", w.Code, w.Body.String())
	}
	if ct := w.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("content type = %q", ct)
	}
	if got := w.Body.String(); got != `{"deleted":true,"documentId":"doc-1"}`+"\n" {
		t.Fatalf("body = %q, want the id and deletion marker on one line", got)
	}
}

// The reads differ before and after the delete: the change log, a snapshot and
// the CRDT state are all reachable beforehand and all miss afterward, while a
// second document is untouched.
func TestDeleteDocumentClearsAllLayers(t *testing.T) {
	srv, _ := newWSTestServer(t)
	registerDeviceViaHTTP(t, srv, "dev-1")

	// Two documents share the process; only doc-a is deleted.
	postDocChange(t, srv, "dev-1", "doc-a", "a1", map[string]any{"n": 1})
	postDocChange(t, srv, "dev-1", "doc-b", "b1", map[string]any{"n": 2})
	// Snapshot on doc-a.
	if code := postHTTP(t, srv, "/v1/documents/doc-a/snapshots", map[string]any{
		"cursor": 1, "state": map[string]any{"n": 1},
	}); code != http.StatusOK {
		t.Fatalf("snapshot = %d", code)
	}
	// CRDT state on doc-a.
	if code := postCRDTOps(t, srv, "doc-a", map[string]any{
		"deviceId": "dev-1", "type": "counter",
		"ops": []any{crdtCounterOp("op-1", 5)},
	}); code != http.StatusOK {
		t.Fatalf("crdt ops = %d", code)
	}

	// Before: everything is reachable.
	if status, _ := httpGet(t, srv, "/v1/documents/doc-a/changes"); status != http.StatusOK {
		t.Fatalf("changes before = %d", status)
	}
	if status, _ := httpGet(t, srv, "/v1/documents/doc-a/snapshots/1"); status != http.StatusOK {
		t.Fatalf("snapshot before = %d", status)
	}
	if status, _ := httpGet(t, srv, "/v1/documents/doc-a/crdt/state"); status != http.StatusOK {
		t.Fatalf("crdt state before = %d", status)
	}

	if status, _, body := deleteDocument(t, srv, "doc-a", "dev-1"); status != http.StatusOK {
		t.Fatalf("delete = %d %s", status, body)
	}

	// After: the change log reads as an unknown document.
	status, body := httpGet(t, srv, "/v1/documents/doc-a/changes")
	if status != http.StatusOK {
		t.Fatalf("changes after = %d", status)
	}
	if len(body["changes"].([]any)) != 0 || body["nextCursor"].(float64) != 0 {
		t.Fatalf("changes after = %v, want an unknown document", body)
	}
	// Snapshot and CRDT state miss with 404.
	if status, _ := httpGet(t, srv, "/v1/documents/doc-a/snapshots/1"); status != http.StatusNotFound {
		t.Fatalf("snapshot after = %d, want 404", status)
	}
	if status, _ := httpGet(t, srv, "/v1/documents/doc-a/crdt/state"); status != http.StatusNotFound {
		t.Fatalf("crdt state after = %d, want 404", status)
	}
	if status, _ := httpGet(t, srv, "/v1/documents/doc-a/crdt/snapshot"); status != http.StatusNotFound {
		t.Fatalf("crdt snapshot after = %d, want 404", status)
	}

	// The other document is byte-for-byte unchanged.
	_, bbody := httpGet(t, srv, "/v1/documents/doc-b/changes")
	row := bbody["changes"].([]any)[0].(map[string]any)
	if row["id"] != "b1" || row["cursor"].(float64) != 1 {
		t.Fatalf("doc-b changed after doc-a deletion: %v", bbody)
	}
}

// A document that holds only CRDT operations (no change log) still exists and
// deletes successfully; afterward its type is no longer fixed.
func TestDeleteCRDTOnlyDocument(t *testing.T) {
	srv, _ := newWSTestServer(t)
	registerDeviceViaHTTP(t, srv, "dev-1")
	if code := postCRDTOps(t, srv, "crdt-doc", map[string]any{
		"deviceId": "dev-1", "type": "counter",
		"ops": []any{crdtCounterOp("op-1", 5)},
	}); code != http.StatusOK {
		t.Fatalf("crdt ops = %d", code)
	}

	if status, _, body := deleteDocument(t, srv, "crdt-doc", "dev-1"); status != http.StatusOK {
		t.Fatalf("delete crdt-only doc = %d %s", status, body)
	}
	if status, _ := httpGet(t, srv, "/v1/documents/crdt-doc/crdt/state"); status != http.StatusNotFound {
		t.Fatalf("crdt state after delete = %d, want 404", status)
	}
	// The type is no longer fixed: a gset batch now wins as the first type.
	if code := postCRDTOps(t, srv, "crdt-doc", map[string]any{
		"deviceId": "dev-1", "type": "gset",
		"ops": []any{crdtGSetOp("op-a", "apple")},
	}); code != http.StatusOK {
		t.Fatalf("recreate crdt doc with a new type = %d", code)
	}
}

// After deletion the same id is a brand-new document: cursors restart at 1, the
// type is free again, and a compaction-retained id is treated as new.
func TestDeleteDocumentRecreatesFresh(t *testing.T) {
	srv, _ := newWSTestServer(t)
	registerDeviceViaHTTP(t, srv, "dev-1")

	postDocChange(t, srv, "dev-1", "doc", "c1", map[string]any{"n": 1})
	postDocChange(t, srv, "dev-1", "doc", "c2", map[string]any{"n": 2})
	if code := postHTTP(t, srv, "/v1/documents/doc/snapshots", map[string]any{
		"cursor": 2, "state": map[string]any{"n": 2},
	}); code != http.StatusOK {
		t.Fatalf("snapshot = %d", code)
	}
	// Compact both changes away, leaving only the boundary and retained
	// idempotency summaries.
	if code := postHTTP(t, srv, "/v1/documents/doc/changes/compact", map[string]any{
		"deviceId": "dev-1",
	}); code != http.StatusOK {
		t.Fatalf("compact = %d", code)
	}

	if status, _, body := deleteDocument(t, srv, "doc", "dev-1"); status != http.StatusOK {
		t.Fatalf("delete = %d %s", status, body)
	}

	// Re-posting a formerly trimmed id creates it anew at cursor 1 — the
	// retained summary is gone, so it is neither idempotent nor a conflict.
	postDocChange(t, srv, "dev-1", "doc", "c1", map[string]any{"n": 1})
	_, body := httpGet(t, srv, "/v1/documents/doc/changes")
	row := body["changes"].([]any)[0].(map[string]any)
	if row["id"] != "c1" || row["cursor"].(float64) != 1 || row["created"] != nil {
		t.Fatalf("fresh doc cursor did not restart: %v", body)
	}
}

// The per-document permission ledger is cleared, so a device revoked on the
// old document is authorized by default on the freshly re-created id.
func TestDeleteDocumentClearsPermissionLedger(t *testing.T) {
	srv, _ := newWSTestServer(t)
	registerDeviceViaHTTP(t, srv, "dev-1")
	registerDeviceViaHTTP(t, srv, "dev-2")
	postDocChange(t, srv, "dev-1", "doc", "c1", map[string]any{"n": 1})

	// Revoke dev-2, then delete the document as the still-authorized dev-1.
	if code := postHTTP(t, srv, "/v1/documents/doc/permissions", map[string]any{
		"deviceId": "dev-2", "action": "revoke",
	}); code != http.StatusOK {
		t.Fatalf("revoke = %d", code)
	}
	if status, _, body := deleteDocument(t, srv, "doc", "dev-1"); status != http.StatusOK {
		t.Fatalf("delete = %d %s", status, body)
	}

	// The ledger for the deleted id shows the default (authorized) again.
	status, ledger := httpGet(t, srv, "/v1/documents/doc/permissions")
	if status != http.StatusOK {
		t.Fatalf("ledger after delete = %d", status)
	}
	for _, e := range ledger["permissions"].([]any) {
		entry := e.(map[string]any)
		if entry["authorized"] != true {
			t.Fatalf("ledger row not reset to default after delete: %v", entry)
		}
	}

	// dev-2 can write to the re-created document without a new grant.
	postDocChange(t, srv, "dev-2", "doc", "c2", map[string]any{"n": 2})
}

// A document whose only durable trace is a permission-ledger deviation (no
// change log, snapshot or CRDT) still exists and deletes cleanly; afterward
// the ledger is empty and a repeat delete misses.
func TestDeleteLedgerOnlyDocument(t *testing.T) {
	srv, _ := newWSTestServer(t)
	registerDeviceViaHTTP(t, srv, "dev-1")
	registerDeviceViaHTTP(t, srv, "dev-2")

	// A revoke writes the document's only row.
	if code := postHTTP(t, srv, "/v1/documents/ledger-only/permissions", map[string]any{
		"deviceId": "dev-2", "action": "revoke",
	}); code != http.StatusOK {
		t.Fatalf("revoke = %d", code)
	}
	if status, _, body := deleteDocument(t, srv, "ledger-only", "dev-1"); status != http.StatusOK {
		t.Fatalf("delete ledger-only doc = %d %s", status, body)
	}
	status, ledger := httpGet(t, srv, "/v1/documents/ledger-only/permissions")
	if status != http.StatusOK {
		t.Fatalf("ledger after delete = %d", status)
	}
	for _, e := range ledger["permissions"].([]any) {
		if e.(map[string]any)["authorized"] != true {
			t.Fatalf("ledger row survived the delete: %v", e)
		}
	}
	// Repeat delete now misses: the ledger-only document no longer exists.
	if status, _, _ := deleteDocument(t, srv, "ledger-only", "dev-1"); status != http.StatusNotFound {
		t.Fatalf("repeat ledger-only delete = %d, want 404", status)
	}
}

// Verdict ordering: shape (400) -> device existence (404) -> permission (403)
// -> document existence (404); every failure is JSON and writes nothing.
func TestDeleteDocumentFailureOrdering(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	registerDevice(t, h, "dev-2")
	postDocChangeHandler(t, h, "dev-1", "doc")
	// dev-2 is revoked on doc-rev.
	postDocChangeHandler(t, h, "dev-1", "doc-rev")
	w, _ := postJSON(t, h, "/v1/documents/doc-rev/permissions", map[string]any{
		"deviceId": "dev-2", "action": "revoke",
	})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	// A document deleted ahead of time so the "already deleted" case misses.
	postDocChangeHandler(t, h, "dev-1", "doc-gone")
	if w, _ := doRequest(t, h, http.MethodDelete, "/v1/documents/doc-gone?deviceId=dev-1"); w.Code != http.StatusOK {
		t.Fatalf("pre-delete doc-gone = %d", w.Code)
	}

	cases := []struct {
		name       string
		method     string
		path       string
		wantStatus int
	}{
		// Shape failures, regardless of device/doc state.
		{"wrong method GET", http.MethodGet, "/v1/documents/doc?deviceId=dev-1", http.StatusBadRequest},
		{"wrong method POST", http.MethodPost, "/v1/documents/doc?deviceId=dev-1", http.StatusBadRequest},
		{"wrong method PUT", http.MethodPut, "/v1/documents/doc?deviceId=dev-1", http.StatusBadRequest},
		{"trailing slash", http.MethodDelete, "/v1/documents/doc/?deviceId=dev-1", http.StatusBadRequest},
		{"missing id segment", http.MethodDelete, "/v1/documents?deviceId=dev-1", http.StatusBadRequest},
		{"extra segment", http.MethodDelete, "/v1/documents/doc/extra?deviceId=dev-1", http.StatusBadRequest},
		{"empty id", http.MethodDelete, "/v1/documents//?deviceId=dev-1", http.StatusBadRequest},
		{"shape beats unknown device", http.MethodDelete, "/v1/documents/doc/extra?deviceId=ghost", http.StatusBadRequest},

		// Device existence comes after shape; a missing/empty/unknown device is
		// one indistinguishable 404.
		{"missing deviceId param", http.MethodDelete, "/v1/documents/doc", http.StatusNotFound},
		{"empty deviceId param", http.MethodDelete, "/v1/documents/doc?deviceId=", http.StatusNotFound},
		{"unknown device", http.MethodDelete, "/v1/documents/doc?deviceId=ghost", http.StatusNotFound},

		// Permission precedes document existence: a revoked caller is 403 even
		// against a document that exists.
		{"revoked device", http.MethodDelete, "/v1/documents/doc-rev?deviceId=dev-2", http.StatusForbidden},

		// Document existence is last.
		{"unknown document", http.MethodDelete, "/v1/documents/never?deviceId=dev-1", http.StatusNotFound},
		{"already deleted", http.MethodDelete, "/v1/documents/doc-gone?deviceId=dev-1", http.StatusNotFound},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w, _ := doRequest(t, h, tc.method, tc.path)
			if w.Code != tc.wantStatus {
				t.Fatalf("%s %s = %d, want %d, body = %s", tc.method, tc.path, w.Code, tc.wantStatus, w.Body.String())
			}
			if ct := w.Header().Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
				t.Fatalf("content type = %q, want JSON", ct)
			}
			if !strings.Contains(w.Body.String(), `"error"`) {
				t.Fatalf("body = %q, want a JSON error", w.Body.String())
			}
		})
	}

	// The already-deleted case above ran against "doc"; confirm a failed delete
	// left its content intact and a revoked delete changed nothing.
	w, _ = doRequest(t, h, http.MethodGet, "/v1/documents/doc/changes")
	if !strings.Contains(w.Body.String(), `"c1"`) {
		t.Fatalf("a failed delete changed the document: %s", w.Body.String())
	}
	w, _ = doRequest(t, h, http.MethodGet, "/v1/documents/doc-rev/changes")
	if !strings.Contains(w.Body.String(), `"c1"`) {
		t.Fatalf("a revoked delete changed the document: %s", w.Body.String())
	}
}

// postDocChangeHandler seeds one change c1 through the in-process handler.
func postDocChangeHandler(t *testing.T, h http.Handler, device, doc string) {
	t.Helper()
	w, _ := postJSON(t, h, "/v1/documents/"+doc+"/changes", map[string]any{
		"deviceId": device,
		"changes":  []any{map[string]any{"id": "c1", "payload": map[string]any{"n": 1}}},
	})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
}

// Concurrent deletes of one document take effect at most once: exactly one
// 200, every other call a 404, and no half-cleaned state survives.
func TestDeleteDocumentConcurrentOnce(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	postDocChangeHandler(t, h, "dev-1", "doc")

	const n = 20
	var wg sync.WaitGroup
	start := make(chan struct{})
	statuses := make([]int, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			w, _ := doRequest(t, h, http.MethodDelete, "/v1/documents/doc?deviceId=dev-1")
			statuses[i] = w.Code
		}(i)
	}
	close(start)
	wg.Wait()

	var ok, missing int
	for _, code := range statuses {
		switch code {
		case http.StatusOK:
			ok++
		case http.StatusNotFound:
			missing++
		default:
			t.Fatalf("unexpected status %d", code)
		}
	}
	if ok != 1 || missing != n-1 {
		t.Fatalf("want exactly one 200 and %d 404s, got %d and %d", n-1, ok, missing)
	}

	// The document is fully gone: a read is an empty unknown document and a
	// further delete still misses.
	w, _ := doRequest(t, h, http.MethodGet, "/v1/documents/doc/changes")
	if strings.TrimSpace(w.Body.String()) != `{"changes":[],"nextCursor":0}` {
		t.Fatalf("document not fully cleaned: %s", w.Body.String())
	}
	w, _ = doRequest(t, h, http.MethodDelete, "/v1/documents/doc?deviceId=dev-1")
	if w.Code != http.StatusNotFound {
		t.Fatalf("repeat delete after concurrent cleanup = %d, want 404", w.Code)
	}
}

// Deleting a document ends every live subscription on it — both push channels,
// document- and session-level, across devices — with close code 4420, drops
// them from the management lists, and leaves other documents' subscriptions
// pushing.
func TestDeleteDocumentClosesSubscriptions4420(t *testing.T) {
	srv, _ := newWSTestServer(t)
	setupSession(t, srv, "dev-1", "sess-1", "victim", 1)
	registerDeviceViaHTTP(t, srv, "dev-2")
	setupSession(t, srv, "dev-2", "sess-2", "other", 0)
	// Give the victim a CRDT state and the control doc one too.
	if code := postCRDTOps(t, srv, "victim", map[string]any{
		"deviceId": "dev-1", "type": "counter",
		"ops": []any{crdtCounterOp("op-1", 1)},
	}); code != http.StatusOK {
		t.Fatalf("victim crdt = %d", code)
	}
	if code := postCRDTOps(t, srv, "other", map[string]any{
		"deviceId": "dev-2", "type": "counter",
		"ops": []any{crdtCounterOp("op-1", 1)},
	}); code != http.StatusOK {
		t.Fatalf("other crdt = %d", code)
	}

	// Four connections on the victim: document- and session-level for each
	// channel.
	victimChangeDoc, _ := dialWS(t, docSubscribeURL(srv, "victim", "dev-1", "0"))
	if victimChangeDoc == nil {
		t.Fatal("victim doc change subscription did not upgrade")
	}
	defer victimChangeDoc.close()
	victimChangeSess, _ := dialWS(t, subscribeURL(srv, "sess-1", "victim", "0"))
	if victimChangeSess == nil {
		t.Fatal("victim session change subscription did not upgrade")
	}
	defer victimChangeSess.close()
	victimStateDoc, _ := dialWS(t, docCRDTSubscribeURL(srv, "victim", "dev-1"))
	if victimStateDoc == nil {
		t.Fatal("victim doc state subscription did not upgrade")
	}
	defer victimStateDoc.close()
	victimStateSess, _ := dialWS(t, crdtSubscribeURL(srv, "sess-1", "victim"))
	if victimStateSess == nil {
		t.Fatal("victim session state subscription did not upgrade")
	}
	defer victimStateSess.close()

	// Two control connections on another document.
	controlChange, _ := dialWS(t, docSubscribeURL(srv, "other", "dev-2", "0"))
	if controlChange == nil {
		t.Fatal("control change subscription did not upgrade")
	}
	defer controlChange.close()
	controlState, _ := dialWS(t, docCRDTSubscribeURL(srv, "other", "dev-2"))
	if controlState == nil {
		t.Fatal("control state subscription did not upgrade")
	}
	defer controlState.close()

	waitForSubscriptionCount(t, srv, "dev-1", 4) // all four victim connections
	waitForSubscriptionCount(t, srv, "dev-2", 2) // control change+state are dev-2's

	// The state subscriptions push the current merge first; drain so the close
	// frame is the next thing read.
	if state := victimStateDoc.readCRDTState(); state.Type != "counter" {
		t.Fatalf("victim state = %v", state)
	}
	if state := victimStateSess.readCRDTState(); state.Type != "counter" {
		t.Fatalf("victim session state = %v", state)
	}
	if state := controlState.readCRDTState(); state.Type != "counter" {
		t.Fatalf("control state = %v", state)
	}
	// The change subscriptions replay the one seeded victim change / nothing
	// for the empty control doc.
	if row := victimChangeDoc.readChange(); row.ID != "seed-1" {
		t.Fatalf("victim replay = %q", row.ID)
	}
	if row := victimChangeSess.readChange(); row.ID != "seed-1" {
		t.Fatalf("victim session replay = %q", row.ID)
	}

	if status, _, body := deleteDocument(t, srv, "victim", "dev-1"); status != http.StatusOK {
		t.Fatalf("delete = %d %s", status, body)
	}

	// Every victim connection ends 4420 on both channels and both identity
	// flows.
	for name, c := range map[string]*wsClient{
		"doc change":     victimChangeDoc,
		"session change": victimChangeSess,
		"doc state":      victimStateDoc,
		"session state":  victimStateSess,
	} {
		c.setReadDeadline(2 * time.Second)
		if code := c.readCloseCode(); code != 4420 {
			t.Fatalf("%s subscription close code = %d, want 4420", name, code)
		}
		c.clearReadDeadline()
	}

	// The victim's entries vanish from both devices' management lists.
	if got := waitForSubscriptionCount(t, srv, "dev-1", 0); len(got) != 0 {
		t.Fatalf("victim subscriptions still listed for dev-1: %+v", got)
	}

	// The control document keeps pushing on both channels.
	postDocChange(t, srv, "dev-2", "other", "after-delete", map[string]any{"n": 9})
	if row := controlChange.readChange(); row.ID != "after-delete" {
		t.Fatalf("control change subscriber got %q, want the live commit", row.ID)
	}
	if code := postCRDTOps(t, srv, "other", map[string]any{
		"deviceId": "dev-2", "type": "counter",
		"ops": []any{crdtCounterOp("op-2", 4)},
	}); code != http.StatusOK {
		t.Fatalf("control crdt op = %d", code)
	}
	state := controlState.readCRDTState()
	if n := crdtCounterValue(t, state); n != 4 {
		t.Fatalf("control state after delete = %d, want 4 (per-device max)", n)
	}

	// Control connections stay open: no frame arrives unsolicited.
	controlChange.setReadDeadline(300 * time.Millisecond)
	if _, _, _, ok := controlChange.readFrameMaybe(); ok {
		t.Fatal("control change connection was disturbed by the deletion")
	}
	controlChange.clearReadDeadline()
}

// A long poll parked on the document returns at once when the document is
// deleted, answering as an unknown document (empty list, cursor 0, not timed
// out) rather than holding until its deadline.
func TestDeleteDocumentWakesParkedPoll(t *testing.T) {
	srv, _ := newWSTestServer(t)
	registerDeviceViaHTTP(t, srv, "dev-1")
	postDocChange(t, srv, "dev-1", "doc", "c1", map[string]any{"n": 1})

	type pollResult struct {
		status int
		body   string
	}
	done := make(chan pollResult, 1)
	go func() {
		req, _ := http.NewRequest(http.MethodGet,
			srv.URL+"/v1/documents/doc/changes/poll?after=1&limit=100&waitMs=10000", nil)
		resp, err := srv.Client().Do(req)
		if err != nil {
			done <- pollResult{0, err.Error()}
			return
		}
		defer resp.Body.Close()
		buf := make([]byte, 4096)
		n, _ := resp.Body.Read(buf)
		done <- pollResult{resp.StatusCode, string(buf[:n])}
	}()

	// Give the poll time to park, then delete.
	time.Sleep(200 * time.Millisecond)
	if status, _, body := deleteDocument(t, srv, "doc", "dev-1"); status != http.StatusOK {
		t.Fatalf("delete = %d %s", status, body)
	}

	select {
	case got := <-done:
		if got.status != http.StatusOK {
			t.Fatalf("poll status = %d", got.status)
		}
		if !strings.Contains(got.body, `"timedOut":false`) ||
			!strings.Contains(got.body, `"nextCursor":0`) ||
			!strings.Contains(got.body, `"changes":[]`) {
			t.Fatalf("parked poll after delete = %q", got.body)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("parked poll was not woken by the document deletion")
	}
}

// The deletion is durable: after a process restart a repeat delete still
// misses (404), the reads stay those of an unknown document, and the verdict
// is unchanged.
func TestDeleteDocumentPersistsAcrossRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "doc-delete-restart.db")

	s, err := app.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	h := NewHandler(s)
	srv := httptest.NewServer(h)
	registerDevice(t, h, "dev-1")
	postDocChangeHandler(t, h, "dev-1", "doc")
	if code := postHTTP(t, srv, "/v1/documents/doc/crdt/ops", map[string]any{
		"deviceId": "dev-1", "type": "counter",
		"ops": []any{crdtCounterOp("op-1", 3)},
	}); code != http.StatusOK {
		t.Fatalf("crdt = %d", code)
	}
	w, _ := doRequest(t, h, http.MethodDelete, "/v1/documents/doc?deviceId=dev-1")
	if w.Code != http.StatusOK {
		t.Fatalf("delete = %d", w.Code)
	}
	srv.Close()
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s2, err := app.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s2.Close() })
	h2 := NewHandler(s2)

	// Repeat delete after restart: still 404, zero writes.
	w, _ = doRequest(t, h2, http.MethodDelete, "/v1/documents/doc?deviceId=dev-1")
	if w.Code != http.StatusNotFound {
		t.Fatalf("repeat delete after restart = %d, want 404", w.Code)
	}
	// Changes and CRDT state stay gone.
	w, _ = doRequest(t, h2, http.MethodGet, "/v1/documents/doc/changes")
	if strings.TrimSpace(w.Body.String()) != `{"changes":[],"nextCursor":0}` {
		t.Fatalf("changes after restart = %s", w.Body.String())
	}
	w, _ = doRequest(t, h2, http.MethodGet, "/v1/documents/doc/crdt/state")
	if w.Code != http.StatusNotFound {
		t.Fatalf("crdt state after restart = %d, want 404", w.Code)
	}

	// The re-created document starts fresh with cursor 1.
	postDocChangeHandler(t, h2, "dev-1", "doc")
	w, _ = doRequest(t, h2, http.MethodGet, "/v1/documents/doc/changes")
	var page struct {
		Changes []struct {
			Cursor float64 `json:"cursor"`
		} `json:"changes"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &page); err != nil || len(page.Changes) != 1 || page.Changes[0].Cursor != 1 {
		t.Fatalf("fresh cursor after restart: %s", w.Body.String())
	}
}
