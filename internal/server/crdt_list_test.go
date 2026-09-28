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
func listCRDTOps(h http.Handler, doc, rawQuery string) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/v1/documents/"+doc+"/crdt/list"+rawQuery, nil))
	return w
}

// Every type's online operations are listed in ascending operation-id order
// with the fixed per-item key order, verbatim stored content, the page count
// and a zero trimmed total before compaction.
func TestListCRDTOpsHTTPSuccessAllTypes(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev")

	w, _ := postJSON(t, h, "/v1/documents/c/crdt/ops", crdtCounterBody("dev",
		crdtCounterOp("c1", 3), crdtCounterOp("c2", 5)))
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	w, _ = postJSON(t, h, "/v1/documents/g/crdt/ops", crdtGSetBody("dev", crdtGSetOp("g1", "b", "a")))
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	w, _ = postJSON(t, h, "/v1/documents/r/crdt/ops", crdtRegisterBody("dev",
		crdtRegisterOp("r1", 1, map[string]any{"k": "v"})))
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	w, _ = postJSON(t, h, "/v1/documents/o/crdt/ops", crdtORSetBody("dev",
		crdtORSetOp("o2", "remove", "apple"), crdtORSetOp("o1", "add", "apple")))
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}

	cases := []struct {
		doc, want string
	}{
		{"c", `{"ops":[{"id":"c1","deviceId":"dev","type":"counter","value":3},{"id":"c2","deviceId":"dev","type":"counter","value":5}],"count":2,"compacted":0}` + "\n"},
		{"g", `{"ops":[{"id":"g1","deviceId":"dev","type":"gset","elements":["b","a"]}],"count":1,"compacted":0}` + "\n"},
		{"r", `{"ops":[{"id":"r1","deviceId":"dev","type":"register","version":1,"value":{"k":"v"}}],"count":1,"compacted":0}` + "\n"},
		{"o", `{"ops":[{"id":"o1","deviceId":"dev","type":"orset","action":"add","element":"apple"},{"id":"o2","deviceId":"dev","type":"orset","action":"remove","element":"apple"}],"count":2,"compacted":0}` + "\n"},
	}
	for _, tc := range cases {
		w := listCRDTOps(h, tc.doc, "?deviceId=dev")
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
// JSON; gset element order is the stored order, not a sorted rewrite.
func TestListCRDTOpsHTTPVerbatimContent(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev")

	w, _ := postJSON(t, h, "/v1/documents/r/crdt/ops", crdtRegisterBody("dev",
		crdtRegisterOp("n", 0, nil),
		crdtRegisterOp("s", 1, "hello"),
		crdtRegisterOp("x", 2, []any{1, true, nil}),
	))
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	w = listCRDTOps(h, "r", "?deviceId=dev&limit=1000&offset=0")
	want := `{"ops":[` +
		`{"id":"n","deviceId":"dev","type":"register","version":0,"value":null},` +
		`{"id":"s","deviceId":"dev","type":"register","version":1,"value":"hello"},` +
		`{"id":"x","deviceId":"dev","type":"register","version":2,"value":[1,true,null]}` +
		`],"count":3,"compacted":0}` + "\n"
	if w.Body.String() != want {
		t.Fatalf("body = %q\nwant %q", w.Body.String(), want)
	}
}

// limit/offset page the ascending listing with no duplication and no gap:
// default limit is 100, count always equals the page length, and an offset
// past the end is an empty page, not an error.
func TestListCRDTOpsHTTPPagination(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev")
	for _, id := range []string{"op-03", "op-01", "op-02"} {
		w, _ := postJSON(t, h, "/v1/documents/doc/crdt/ops", crdtCounterBody("dev", crdtCounterOp(id, 1)))
		if w.Code != http.StatusOK {
			t.Fatal(w.Body.String())
		}
	}

	var seen []string
	for _, offset := range []string{"0", "1", "2"} {
		w := listCRDTOps(h, "doc", "?deviceId=dev&limit=1&offset="+offset)
		if w.Code != http.StatusOK {
			t.Fatalf("offset %s = %d %s", offset, w.Code, w.Body.String())
		}
		body := w.Body.String()
		if !strings.Contains(body, `"count":1`) {
			t.Fatalf("offset %s count not 1: %s", offset, body)
		}
		switch offset {
		case "0":
			if !strings.Contains(body, `"id":"op-01"`) {
				t.Fatalf("page 0 = %s", body)
			}
			seen = append(seen, "op-01")
		case "1":
			if !strings.Contains(body, `"id":"op-02"`) {
				t.Fatalf("page 1 = %s", body)
			}
			seen = append(seen, "op-02")
		case "2":
			if !strings.Contains(body, `"id":"op-03"`) {
				t.Fatalf("page 2 = %s", body)
			}
			seen = append(seen, "op-03")
		}
	}
	if seen[0] != "op-01" || seen[1] != "op-02" || seen[2] != "op-03" {
		t.Fatalf("paged ids = %v", seen)
	}

	// An empty last page still reports count 0 and the trimmed total.
	w := listCRDTOps(h, "doc", "?deviceId=dev&limit=1&offset=3")
	if w.Body.String() != `{"ops":[],"count":0,"compacted":0}`+"\n" {
		t.Fatalf("past-end body = %q", w.Body.String())
	}

	// No parameters at all: the default page carries all three operations.
	w = listCRDTOps(h, "doc", "?deviceId=dev")
	if !strings.Contains(w.Body.String(), `"count":3`) {
		t.Fatalf("default page = %s", w.Body.String())
	}
}

// Every illegal pagination parameter is a 400 judged before device
// existence, document permission and CRDT existence.
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
		"?deviceId=dev&limit=x",
		"?deviceId=dev&offset=-1",
		"?deviceId=dev&offset=1.5",
		"?deviceId=dev&offset=x",
	}
	for _, q := range bad {
		w := listCRDTOps(h, "doc", q)
		if w.Code != http.StatusBadRequest {
			t.Fatalf("%s status = %d want 400 body = %s", q, w.Code, w.Body.String())
		}
		if !strings.HasPrefix(w.Body.String(), `{"error":`) {
			t.Fatalf("%s body = %q, want a JSON error", q, w.Body.String())
		}
	}

	// Repeated params: Go takes the first value; a legal first value must not
	// be overridden by an illegal later one — but an illegal first value is a
	// 400 regardless of what follows.
	if w := listCRDTOps(h, "doc", "?deviceId=dev&limit=0&limit=10"); w.Code != http.StatusBadRequest {
		t.Fatalf("first illegal limit status = %d body = %s", w.Code, w.Body.String())
	}

	// Shape precedes existence: an illegal page against an unregistered device
	// and a never-touched document is still a 400.
	for _, q := range []string{
		"/crdt/list?deviceId=ghost&limit=0",
		"/crdt/list?deviceId=dev&offset=-1",
	} {
		if w := getRaw(t, h, "/v1/documents/empty-doc"+q); w.Code != http.StatusBadRequest {
			t.Fatalf("shape-before-existence %s = %d body = %s", q, w.Code, w.Body.String())
		}
	}
}

// The fixed verdict order: shape (400), device existence (404), permission
// (403), CRDT state existence (404); the latter failures expose no content.
func TestListCRDTOpsHTTPGateOrder(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev")
	w, _ := postJSON(t, h, "/v1/documents/doc/crdt/ops", crdtCounterBody("dev", crdtCounterOp("c1", 1)))
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}

	// A missing or empty deviceId is indistinguishable from an unregistered
	// device: 404 with an error only.
	for _, q := range []string{"", "?deviceId=", "?deviceId=ghost"} {
		if w := listCRDTOps(h, "doc", q); w.Code != http.StatusNotFound {
			t.Fatalf("device %q = %d body = %s", q, w.Code, w.Body.String())
		}
	}
	// A document with no committed CRDT operation is the same 404 the state
	// read gives, even for a registered authorized device.
	if w := listCRDTOps(h, "empty-doc", "?deviceId=dev"); w.Code != http.StatusNotFound {
		t.Fatalf("no-crdt document = %d body = %s", w.Code, w.Body.String())
	}

	// Revoked permission: 403 with an error only and no listing content.
	w, _ = postJSON(t, h, "/v1/documents/doc/permissions", map[string]any{"deviceId": "dev", "action": "revoke"})
	if w.Code != http.StatusOK {
		t.Fatalf("revoke = %d %s", w.Code, w.Body.String())
	}
	w = listCRDTOps(h, "doc", "?deviceId=dev")
	if w.Code != http.StatusForbidden {
		t.Fatalf("revoked device = %d body = %s", w.Code, w.Body.String())
	}
	if strings.Contains(w.Body.String(), `"ops"`) || strings.Contains(w.Body.String(), `"c1"`) {
		t.Fatalf("403 leaked crdt content: %s", w.Body.String())
	}
}

// After compaction the trimmed ids leave every page, the surviving operations
// keep their order, and compacted reports the total number trimmed.
func TestListCRDTOpsHTTPCompacted(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	registerDevice(t, h, "dev-2")

	submit := func(device, id string, value int) {
		t.Helper()
		w, _ := postJSON(t, h, "/v1/documents/doc/crdt/ops", crdtCounterBody(device, crdtCounterOp(id, value)))
		if w.Code != http.StatusOK {
			t.Fatalf("submit %s: %d %s", id, w.Code, w.Body.String())
		}
	}
	submit("dev-1", "a1", 5)
	submit("dev-1", "a2", 8)
	submit("dev-2", "d1", 1)
	submit("dev-2", "d2", 3)

	w, _ := postJSON(t, h, "/v1/documents/doc/crdt/compact", map[string]any{"deviceId": "dev-1"})
	if w.Code != http.StatusOK {
		t.Fatalf("compact = %d %s", w.Code, w.Body.String())
	}

	w = listCRDTOps(h, "doc", "?deviceId=dev-1")
	want := `{"ops":[` +
		`{"id":"a2","deviceId":"dev-1","type":"counter","value":8},` +
		`{"id":"d2","deviceId":"dev-2","type":"counter","value":3}` +
		`],"count":2,"compacted":2}` + "\n"
	if w.Body.String() != want {
		t.Fatalf("body = %q\nwant %q", w.Body.String(), want)
	}
	// A page past the survivors keeps the trimmed total and carries no items.
	w = listCRDTOps(h, "doc", "?deviceId=dev-1&offset=9")
	if w.Body.String() != `{"ops":[],"count":0,"compacted":2}`+"\n" {
		t.Fatalf("past-end body = %q", w.Body.String())
	}
}

// The listing is read-only: it commits no operation, advances no cursor and
// wakes neither a parked change long poll nor a CRDT push subscriber.
func TestListCRDTOpsHTTPReadOnly(t *testing.T) {
	h, s := newTestHandler(t)
	registerDevice(t, h, "dev")
	w, _ := postJSON(t, h, "/v1/documents/doc/crdt/ops", crdtCounterBody("dev", crdtCounterOp("c1", 1)))
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	// One change-log row so the poll parks waiting for the cursor past it; the
	// CRDT listing must not wake that wait.
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
		if w := listCRDTOps(h, "doc", "?deviceId=dev&limit=1"); w.Code != http.StatusOK {
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
	if snap.Operations != 1 {
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
	if w := getRaw(t, h, "/v1/documents/list/crdt/state"); w.Code != http.StatusOK {
		t.Fatalf("state on doc named list = %d %s", w.Code, w.Body.String())
	}
	if w := listCRDTOps(h, "list", "?deviceId=dev"); w.Code != http.StatusOK ||
		!strings.Contains(w.Body.String(), `"id":"z"`) {
		t.Fatalf("list on doc named list = %d %s", w.Code, w.Body.String())
	}
}

// After a process restart the same listing yields the byte-identical body and
// verdicts, including the trimmed total.
func TestListCRDTOpsHTTPRestartStable(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "sync.db")

	st, err := app.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	h := NewHandler(st)
	registerDevice(t, h, "dev-1")
	registerDevice(t, h, "dev-2")
	for _, c := range []struct {
		device, id string
		value      int
	}{
		{"dev-1", "a1", 5}, {"dev-1", "a2", 8}, {"dev-2", "d1", 1}, {"dev-2", "d2", 3},
	} {
		w, _ := postJSON(t, h, "/v1/documents/doc/crdt/ops", crdtCounterBody(c.device, crdtCounterOp(c.id, c.value)))
		if w.Code != http.StatusOK {
			t.Fatal(w.Body.String())
		}
	}
	if w, _ := postJSON(t, h, "/v1/documents/doc/crdt/compact", map[string]any{"deviceId": "dev-1"}); w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	body := listCRDTOps(h, "doc", "?deviceId=dev-1&limit=2&offset=1").Body.String()
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}

	st2, err := app.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st2.Close() }()
	h2 := NewHandler(st2)
	w2 := listCRDTOps(h2, "doc", "?deviceId=dev-1&limit=2&offset=1")
	if w2.Code != http.StatusOK {
		t.Fatalf("after restart = %d %s", w2.Code, w2.Body.String())
	}
	if w2.Body.String() != body {
		t.Fatalf("body changed across restart:\nbefore %q\nafter  %q", body, w2.Body.String())
	}
}
