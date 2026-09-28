package server

import (
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/alicegogogogogo/local-first-sync-service/internal/app"
)

// listCRDTOps sends a raw GET to the document-level CRDT operation listing
// and returns the recorder.
func listCRDTOps(t *testing.T, h http.Handler, target string) *httptest.ResponseRecorder {
	t.Helper()
	r := httptest.NewRequest(http.MethodGet, target, nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

func crdtListPath(doc, query string) string {
	return "/v1/documents/" + doc + "/crdt/list" + query
}

// All four types render the stable id order, the fixed item key order (id,
// deviceId, type then the type-specific content) and the ops/count/compacted
// top-level order with one trailing newline.
func TestListCRDTOpsHTTPSuccessAllTypes(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev")
	registerDevice(t, h, "dev-2")

	// The two counter ops arrive from different devices (per-device
	// contributions only move forward); the browse order is the stable id
	// order, not arrival or device order.
	w, _ := postJSON(t, h, "/v1/documents/c/crdt/ops", crdtCounterBody("dev-2", crdtCounterOp("c2", 8)))
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	w, _ = postJSON(t, h, "/v1/documents/c/crdt/ops", crdtCounterBody("dev", crdtCounterOp("c1", 5)))
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	w, _ = postJSON(t, h, "/v1/documents/g/crdt/ops", crdtGSetBody("dev", crdtGSetOp("g1", "b", "a")))
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	w, _ = postJSON(t, h, "/v1/documents/r/crdt/ops", crdtRegisterBody("dev", crdtRegisterOp("r1", 3, map[string]any{"k": "v"})))
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	w, _ = postJSON(t, h, "/v1/documents/o/crdt/ops", crdtORSetBody("dev",
		crdtORSetOp("o1", "add", "apple"), crdtORSetOp("o2", "remove", "apple")))
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}

	cases := []struct {
		doc, want string
	}{
		{"c", `{"ops":[{"id":"c1","deviceId":"dev","type":"counter","value":5},{"id":"c2","deviceId":"dev-2","type":"counter","value":8}],"count":2,"compacted":0}` + "\n"},
		{"g", `{"ops":[{"id":"g1","deviceId":"dev","type":"gset","elements":["b","a"]}],"count":1,"compacted":0}` + "\n"},
		{"r", `{"ops":[{"id":"r1","deviceId":"dev","type":"register","version":3,"value":{"k":"v"}}],"count":1,"compacted":0}` + "\n"},
		{"o", `{"ops":[{"id":"o1","deviceId":"dev","type":"orset","action":"add","element":"apple"},{"id":"o2","deviceId":"dev","type":"orset","action":"remove","element":"apple"}],"count":2,"compacted":0}` + "\n"},
	}
	for _, tc := range cases {
		w := listCRDTOps(t, h, crdtListPath(tc.doc, "?deviceId=dev"))
		if w.Code != http.StatusOK {
			t.Fatalf("%s status = %d body = %s", tc.doc, w.Code, w.Body.String())
		}
		if ct := w.Header().Get("Content-Type"); ct != "application/json" {
			t.Fatalf("%s content type = %q", tc.doc, ct)
		}
		if w.Body.String() != tc.want {
			t.Fatalf("%s body = %q\nwant %q", tc.doc, w.Body.String(), tc.want)
		}
	}
}

// Register values are presented verbatim as saved, including null and scalar
// JSON; the default (no pagination parameters) page lists every online op.
func TestListCRDTOpsHTTPVerbatimAndDefaults(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev")
	w, _ := postJSON(t, h, "/v1/documents/r/crdt/ops", crdtRegisterBody("dev",
		map[string]any{"id": "n", "version": 0, "value": nil},
		map[string]any{"id": "s", "version": 1, "value": "hello"},
		map[string]any{"id": "x", "version": 2, "value": []any{1, true, nil}},
	))
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	w = listCRDTOps(t, h, crdtListPath("r", "?deviceId=dev"))
	want := `{"ops":[` +
		`{"id":"n","deviceId":"dev","type":"register","version":0,"value":null},` +
		`{"id":"s","deviceId":"dev","type":"register","version":1,"value":"hello"},` +
		`{"id":"x","deviceId":"dev","type":"register","version":2,"value":[1,true,null]}` +
		`],"count":3,"compacted":0}` + "\n"
	if w.Body.String() != want {
		t.Fatalf("body = %q\nwant %q", w.Body.String(), want)
	}
}

// limit/offset page the stable order without repeats or gaps; an offset past
// the end is an empty page with count 0, not an error.
func TestListCRDTOpsHTTPPagination(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev")
	w, _ := postJSON(t, h, "/v1/documents/doc/crdt/ops", crdtCounterBody("dev",
		crdtCounterOp("a", 1), crdtCounterOp("b", 2), crdtCounterOp("c", 3)))
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}

	w1 := listCRDTOps(t, h, crdtListPath("doc", "?deviceId=dev&limit=2&offset=0"))
	if w1.Body.String() != `{"ops":[{"id":"a","deviceId":"dev","type":"counter","value":1},{"id":"b","deviceId":"dev","type":"counter","value":2}],"count":2,"compacted":0}`+"\n" {
		t.Fatalf("page1 = %q", w1.Body.String())
	}
	w2 := listCRDTOps(t, h, crdtListPath("doc", "?deviceId=dev&limit=2&offset=2"))
	if w2.Body.String() != `{"ops":[{"id":"c","deviceId":"dev","type":"counter","value":3}],"count":1,"compacted":0}`+"\n" {
		t.Fatalf("page2 = %q", w2.Body.String())
	}
	w3 := listCRDTOps(t, h, crdtListPath("doc", "?deviceId=dev&offset=3"))
	if w3.Body.String() != `{"ops":[],"count":0,"compacted":0}`+"\n" {
		t.Fatalf("page3 = %q", w3.Body.String())
	}
}

// Illegal pagination values are 400 JSON errors checked before the device and
// CRDT-existence verdicts, so they win even for an unregistered device or a
// document with no CRDT operation.
func TestListCRDTOpsHTTPRejectsBadPagination(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev")
	w, _ := postJSON(t, h, "/v1/documents/doc/crdt/ops", crdtCounterBody("dev", crdtCounterOp("c1", 1)))
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}

	bad := []string{
		"?deviceId=dev&limit=0",
		"?deviceId=dev&limit=1001",
		"?deviceId=dev&limit=-1",
		"?deviceId=dev&limit=1.5",
		"?deviceId=dev&limit=abc",
		"?deviceId=dev&offset=-1",
		"?deviceId=dev&offset=1.5",
		"?deviceId=dev&offset=abc",
	}
	for _, q := range bad {
		w := listCRDTOps(t, h, crdtListPath("doc", q))
		if w.Code != http.StatusBadRequest {
			t.Fatalf("%s status = %d want 400 body = %s", q, w.Code, w.Body.String())
		}
		if !strings.HasPrefix(w.Body.String(), `{"error":`) {
			t.Fatalf("%s body = %q, want a JSON error", q, w.Body.String())
		}
	}

	// Shape first: a bad limit against an unregistered device or a document
	// with no CRDT operation is still the 400.
	if w := listCRDTOps(t, h, crdtListPath("doc", "?deviceId=ghost&limit=0")); w.Code != http.StatusBadRequest {
		t.Fatalf("shape-before-device = %d body = %s", w.Code, w.Body.String())
	}
	if w := listCRDTOps(t, h, crdtListPath("empty-doc", "?deviceId=dev&offset=-1")); w.Code != http.StatusBadRequest {
		t.Fatalf("shape-before-state = %d body = %s", w.Code, w.Body.String())
	}
}

// The fixed verdict order: shape (400), device existence (404), permission
// (403), CRDT state existence (404); none of the failures exposes content.
func TestListCRDTOpsHTTPGateOrder(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev")
	w, _ := postJSON(t, h, "/v1/documents/doc/crdt/ops", crdtCounterBody("dev", crdtCounterOp("c1", 1)))
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}

	// A missing or empty deviceId is indistinguishable from an unregistered
	// caller: 404.
	for _, q := range []string{"", "?deviceId=", "?deviceId=ghost"} {
		if w := listCRDTOps(t, h, crdtListPath("doc", q)); w.Code != http.StatusNotFound {
			t.Fatalf("device verdict for %q = %d body = %s", q, w.Code, w.Body.String())
		}
	}
	// A document with no committed CRDT operation is the same 404 the state
	// read gives, for a registered authorized device.
	if w := listCRDTOps(t, h, crdtListPath("empty-doc", "?deviceId=dev")); w.Code != http.StatusNotFound {
		t.Fatalf("no-crdt document = %d body = %s", w.Code, w.Body.String())
	}

	// Revoked permission: 403 with no ops content.
	w, _ = postJSON(t, h, "/v1/documents/doc/permissions", map[string]any{"deviceId": "dev", "action": "revoke"})
	if w.Code != http.StatusOK {
		t.Fatalf("revoke = %d %s", w.Code, w.Body.String())
	}
	w = listCRDTOps(t, h, crdtListPath("doc", "?deviceId=dev"))
	if w.Code != http.StatusForbidden {
		t.Fatalf("revoked device = %d body = %s", w.Code, w.Body.String())
	}
	if strings.Contains(w.Body.String(), `"ops"`) || strings.Contains(w.Body.String(), `"value"`) {
		t.Fatalf("403 leaked crdt content: %s", w.Body.String())
	}
}

// After CRDT compaction the trimmed ids leave the listing and compacted
// reports their total; the surviving operation stays in the stable order.
func TestListCRDTOpsHTTPCompacted(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	w, _ := postJSON(t, h, "/v1/documents/doc/crdt/ops", crdtCounterBody("dev-1",
		crdtCounterOp("a1", 5), crdtCounterOp("a2", 8)))
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	if w, _ := postJSON(t, h, "/v1/documents/doc/crdt/compact", map[string]any{"deviceId": "dev-1"}); w.Code != http.StatusOK {
		t.Fatalf("compact = %d %s", w.Code, w.Body.String())
	}

	w = listCRDTOps(t, h, crdtListPath("doc", "?deviceId=dev-1"))
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", w.Code, w.Body.String())
	}
	want := `{"ops":[{"id":"a2","deviceId":"dev-1","type":"counter","value":8}],"count":1,"compacted":1}` + "\n"
	if w.Body.String() != want {
		t.Fatalf("body = %q\nwant %q", w.Body.String(), want)
	}
}

// The listing is read-only: it writes no operation, advances no cursor and
// wakes neither a parked change long poll nor any subscriber.
func TestListCRDTOpsHTTPReadOnly(t *testing.T) {
	h, s := newTestHandler(t)
	registerDevice(t, h, "dev")
	w, _ := postJSON(t, h, "/v1/documents/doc/crdt/ops", crdtCounterBody("dev",
		crdtCounterOp("c1", 1), crdtCounterOp("c2", 2)))
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	postRawChanges(t, h, "doc", "dev", `[{"id":"ch1","payload":1}]`)

	done := make(chan struct {
		changes  int
		timedOut bool
		err      error
	}, 1)
	go func() {
		changes, _, timedOut, err := s.WaitForChanges(t.Context(), "doc", 1, 100, 250*time.Millisecond)
		done <- struct {
			changes  int
			timedOut bool
			err      error
		}{len(changes), timedOut, err}
	}()
	time.Sleep(30 * time.Millisecond) // let the poll park
	for i := 0; i < 3; i++ {
		if w := listCRDTOps(t, h, crdtListPath("doc", "?deviceId=dev&limit=1")); w.Code != http.StatusOK {
			t.Fatalf("list %d = %d %s", i, w.Code, w.Body.String())
		}
	}
	got := <-done
	if got.err != nil {
		t.Fatalf("poll: %v", got.err)
	}
	if !got.timedOut || got.changes != 0 {
		t.Fatalf("poll woken by a read-only listing: %+v", got)
	}

	snap, err := s.GetCRDTSnapshot("doc")
	if err != nil {
		t.Fatal(err)
	}
	if snap.Operations != 2 {
		t.Fatalf("listing wrote operations: snapshot = %+v", snap)
	}
}

// Only GET at the exact .../crdt/list location is accepted; every other verb
// or shape is a JSON 400, never a redirect.
func TestListCRDTOpsHTTPMethodAndPath(t *testing.T) {
	h, _ := newTestHandler(t)

	cases := []struct {
		method, path string
	}{
		{http.MethodPost, "/v1/documents/doc1/crdt/list"},
		{http.MethodPut, "/v1/documents/doc1/crdt/list"},
		{http.MethodDelete, "/v1/documents/doc1/crdt/list"},
		{http.MethodGet, "/v1/documents/doc1/crdt/list/"},
		{http.MethodGet, "/v1/documents/doc1/crdt/list/extra"},
		{http.MethodGet, "/v1/documents/doc1/crdt/listextra"},
		{http.MethodGet, "/v1/documents//crdt/list"},
		{http.MethodGet, "/v1/documents/doc1/crdt"},
	}
	for _, tc := range cases {
		t.Run(tc.method+" "+tc.path, func(t *testing.T) {
			r := httptest.NewRequest(tc.method, tc.path, nil)
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			if w.Code != http.StatusBadRequest {
				t.Fatalf("status = %d want 400 body = %s", w.Code, w.Body.String())
			}
			if ct := w.Header().Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
				t.Fatalf("content type = %q, want JSON", ct)
			}
			if loc := w.Header().Get("Location"); loc != "" {
				t.Fatalf("failure redirected to %q", loc)
			}
		})
	}
}

// A document literally named "list" keeps its ordinary CRDT routes; the list
// keyword is only recognized in the terminal subresource position.
func TestListCRDTOpsHTTPDocumentNamedList(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev")
	w, _ := postJSON(t, h, "/v1/documents/list/crdt/ops", crdtCounterBody("dev", crdtCounterOp("z", 1)))
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	if w := listCRDTOps(t, h, crdtListPath("list", "?deviceId=dev")); w.Code != http.StatusOK ||
		!strings.Contains(w.Body.String(), `"id":"z"`) {
		t.Fatalf("list on doc named list = %d %s", w.Code, w.Body.String())
	}
}

// After a process restart the same listing yields the byte-identical body and
// verdicts, including the compacted count.
func TestListCRDTOpsHTTPRestartStable(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "sync.db")

	st, err := app.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	h := NewHandler(st)
	registerDevice(t, h, "dev-1")
	w, _ := postJSON(t, h, "/v1/documents/doc/crdt/ops", crdtCounterBody("dev-1",
		crdtCounterOp("a1", 5), crdtCounterOp("a2", 8)))
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	if w, _ := postJSON(t, h, "/v1/documents/doc/crdt/compact", map[string]any{"deviceId": "dev-1"}); w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	body := listCRDTOps(t, h, crdtListPath("doc", "?deviceId=dev-1&limit=10&offset=0")).Body.String()
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}

	st2, err := app.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st2.Close() }()
	h2 := NewHandler(st2)
	w2 := listCRDTOps(t, h2, crdtListPath("doc", "?deviceId=dev-1&limit=10&offset=0"))
	if w2.Code != http.StatusOK {
		t.Fatalf("after restart = %d %s", w2.Code, w2.Body.String())
	}
	if w2.Body.String() != body {
		t.Fatalf("body changed across restart:\nbefore %q\nafter  %q", body, w2.Body.String())
	}
}
