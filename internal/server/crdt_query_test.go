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

// queryCRDTOps sends a raw POST to the document-level CRDT batch lookup and
// returns the recorder.
func queryCRDTOps(t *testing.T, h http.Handler, doc, rawBody string) *httptest.ResponseRecorder {
	t.Helper()
	r := httptest.NewRequest(http.MethodPost, "/v1/documents/"+doc+"/crdt/query", strings.NewReader(rawBody))
	r.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

// Found answers arrive in the order asked, each carrying the source device
// and the type-specific comparison content, with the fixed per-type key
// order; missing ids keep their slot with id and status only.
func TestQueryCRDTOpsHTTPSuccessAllTypes(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev")

	w, _ := postJSON(t, h, "/v1/documents/c/crdt/ops", crdtCounterBody("dev", crdtCounterOp("c1", 5)))
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
		doc  string
		body string
		want string
	}{
		{"c", `{"deviceId":"dev","ids":["nope","c1"]}`,
			`{"results":[{"id":"nope","status":"missing"},{"id":"c1","status":"found","deviceId":"dev","value":5}],"count":2}` + "\n"},
		{"g", `{"deviceId":"dev","ids":["g1","nope"]}`,
			`{"results":[{"id":"g1","status":"found","deviceId":"dev","elements":["b","a"]},{"id":"nope","status":"missing"}],"count":2}` + "\n"},
		{"r", `{"deviceId":"dev","ids":["r1"]}`,
			`{"results":[{"id":"r1","status":"found","deviceId":"dev","version":3,"value":{"k":"v"}}],"count":1}` + "\n"},
		{"o", `{"deviceId":"dev","ids":["o2","o1","nope"]}`,
			`{"results":[{"id":"o2","status":"found","deviceId":"dev","action":"remove","element":"apple"},{"id":"o1","status":"found","deviceId":"dev","action":"add","element":"apple"},{"id":"nope","status":"missing"}],"count":3}` + "\n"},
	}
	for _, tc := range cases {
		w := queryCRDTOps(t, h, tc.doc, tc.body)
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
// JSON; counter values are the stored contribution integers.
func TestQueryCRDTOpsHTTPVerbatimContent(t *testing.T) {
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
	w = queryCRDTOps(t, h, "r", `{"deviceId":"dev","ids":["n","s","x"]}`)
	want := `{"results":[` +
		`{"id":"n","status":"found","deviceId":"dev","version":0,"value":null},` +
		`{"id":"s","status":"found","deviceId":"dev","version":1,"value":"hello"},` +
		`{"id":"x","status":"found","deviceId":"dev","version":2,"value":[1,true,null]}` +
		`],"count":3}` + "\n"
	if w.Body.String() != want {
		t.Fatalf("body = %q\nwant %q", w.Body.String(), want)
	}
}

// After CRDT compaction, trimmed counter ids answer "compacted" with no device
// or value while the surviving maximum stays found; never-known ids stay
// missing.
func TestQueryCRDTOpsHTTPCompacted(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")

	submit := func(id string, value int) {
		t.Helper()
		w, _ := postJSON(t, h, "/v1/documents/doc/crdt/ops", crdtCounterBody("dev-1", crdtCounterOp(id, value)))
		if w.Code != http.StatusOK {
			t.Fatalf("submit %s: %d %s", id, w.Code, w.Body.String())
		}
	}
	submit("a1", 5)
	submit("a2", 8)

	w, _ := postJSON(t, h, "/v1/documents/doc/crdt/compact", map[string]any{"deviceId": "dev-1"})
	if w.Code != http.StatusOK {
		t.Fatalf("compact = %d %s", w.Code, w.Body.String())
	}

	w = queryCRDTOps(t, h, "doc", `{"deviceId":"dev-1","ids":["a1","a2","nope"]}`)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", w.Code, w.Body.String())
	}
	want := `{"results":[` +
		`{"id":"a1","status":"compacted"},` +
		`{"id":"a2","status":"found","deviceId":"dev-1","value":8},` +
		`{"id":"nope","status":"missing"}` +
		`],"count":3}` + "\n"
	if w.Body.String() != want {
		t.Fatalf("body = %q\nwant %q", w.Body.String(), want)
	}
}

// Every request-shape violation is a 400 JSON error with zero writes, checked
// before the device even exists.
func TestQueryCRDTOpsHTTPRejectsBadShape(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev")
	url := "/v1/documents/doc1/crdt/query"

	send := func(contentType, raw string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(http.MethodPost, url, strings.NewReader(raw))
		if contentType != "" {
			r.Header.Set("Content-Type", contentType)
		}
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		return w
	}

	cases := []struct {
		name, contentType, body string
	}{
		{"wrong content type", "text/plain", `{"deviceId":"dev","ids":["a"]}`},
		{"missing content type", "", `{"deviceId":"dev","ids":["a"]}`},
		{"content type with json suffix", "application/vnd.api+json", `{"deviceId":"dev","ids":["a"]}`},
		{"invalid json", "application/json", `{"deviceId":"dev","ids":[`},
		{"trailing content", "application/json", `{"deviceId":"dev","ids":["a"]} junk`},
		{"second json value", "application/json", `{"deviceId":"dev","ids":["a"]}{}`},
		{"missing ids", "application/json", `{"deviceId":"dev"}`},
		{"null ids", "application/json", `{"deviceId":"dev","ids":null}`},
		{"empty ids", "application/json", `{"deviceId":"dev","ids":[]}`},
		{"empty id element", "application/json", `{"deviceId":"dev","ids":["a",""]}`},
		{"non-string id element", "application/json", `{"deviceId":"dev","ids":["a",1]}`},
		{"ids not an array", "application/json", `{"deviceId":"dev","ids":"a"}`},
		{"duplicate ids", "application/json", `{"deviceId":"dev","ids":["a","a"]}`},
		{"missing deviceId", "application/json", `{"ids":["a"]}`},
		{"empty deviceId", "application/json", `{"deviceId":"","ids":["a"]}`},
		{"non-string deviceId", "application/json", `{"deviceId":7,"ids":["a"]}`},
		{"non-object body", "application/json", `[1,2]`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w := send(tc.contentType, tc.body)
			if w.Code != http.StatusBadRequest {
				t.Fatalf("status = %d want 400 body = %s", w.Code, w.Body.String())
			}
			if !strings.HasPrefix(w.Body.String(), `{"error":`) {
				t.Fatalf("body = %q, want a JSON error", w.Body.String())
			}
		})
	}

	// A malformed body against an unregistered device is still a 400: shape
	// validation precedes device existence.
	if w := send("application/json", `{"deviceId":"ghost","ids":[]}`); w.Code != http.StatusBadRequest {
		t.Fatalf("shape-before-existence = %d body = %s", w.Code, w.Body.String())
	}
}

// The fixed verdict order: shape (400), device existence (404), permission
// (403), CRDT state existence (404); the latter failures expose no content.
func TestQueryCRDTOpsHTTPGateOrder(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev")
	w, _ := postJSON(t, h, "/v1/documents/doc/crdt/ops", crdtCounterBody("dev", crdtCounterOp("c1", 1)))
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}

	// Unregistered device: 404 with an error only.
	if w := queryCRDTOps(t, h, "doc", `{"deviceId":"ghost","ids":["c1"]}`); w.Code != http.StatusNotFound {
		t.Fatalf("unknown device = %d body = %s", w.Code, w.Body.String())
	}
	// A document with no committed CRDT operation is the same 404 the state
	// read gives, even for a registered authorized device.
	if w := queryCRDTOps(t, h, "empty-doc", `{"deviceId":"dev","ids":["c1"]}`); w.Code != http.StatusNotFound {
		t.Fatalf("no-crdt document = %d body = %s", w.Code, w.Body.String())
	}

	// Revoked permission: 403 with an error only.
	w, _ = postJSON(t, h, "/v1/documents/doc/permissions", map[string]any{"deviceId": "dev", "action": "revoke"})
	if w.Code != http.StatusOK {
		t.Fatalf("revoke = %d %s", w.Code, w.Body.String())
	}
	w = queryCRDTOps(t, h, "doc", `{"deviceId":"dev","ids":["c1"]}`)
	if w.Code != http.StatusForbidden {
		t.Fatalf("revoked device = %d body = %s", w.Code, w.Body.String())
	}
	if strings.Contains(w.Body.String(), `"results"`) || strings.Contains(w.Body.String(), `"value"`) {
		t.Fatalf("403 leaked crdt content: %s", w.Body.String())
	}
}

// The lookup is read-only: it commits no operation, advances no cursor and
// wakes neither a parked change long poll nor a CRDT push subscriber.
func TestQueryCRDTOpsHTTPReadOnly(t *testing.T) {
	h, s := newTestHandler(t)
	registerDevice(t, h, "dev")
	w, _ := postJSON(t, h, "/v1/documents/doc/crdt/ops", crdtCounterBody("dev", crdtCounterOp("c1", 1)))
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	// One change-log row so the poll parks waiting for the cursor past it; the
	// CRDT query must not wake that wait.
	postRawChanges(t, h, "doc", "dev", `[{"id":"ch1","payload":1}]`)

	// A parked change long poll on the same document must time out despite
	// the CRDT queries.
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
		if w := queryCRDTOps(t, h, "doc", `{"deviceId":"dev","ids":["c1","nope"]}`); w.Code != http.StatusOK {
			t.Fatalf("query %d = %d %s", i, w.Code, w.Body.String())
		}
	}
	got := <-done
	if got.err != nil {
		t.Fatalf("poll: %v", got.err)
	}
	if !got.timedOut || got.changes != 0 {
		t.Fatalf("poll woken by a read-only query: %+v", got)
	}

	// The queries wrote no operation: snapshot operation count is unchanged.
	snap, err := s.GetCRDTSnapshot("doc")
	if err != nil {
		t.Fatal(err)
	}
	if snap.Operations != 1 {
		t.Fatalf("query wrote operations: snapshot = %+v", snap)
	}
}

// Only POST at the exact .../crdt/query location is accepted; every other
// verb or shape is a JSON 400, never a redirect.
func TestQueryCRDTOpsHTTPMethodAndPath(t *testing.T) {
	h, _ := newTestHandler(t)

	cases := []struct {
		method, path string
	}{
		{http.MethodGet, "/v1/documents/doc1/crdt/query"},
		{http.MethodPut, "/v1/documents/doc1/crdt/query"},
		{http.MethodDelete, "/v1/documents/doc1/crdt/query"},
		{http.MethodPost, "/v1/documents/doc1/crdt/query/"},
		{http.MethodPost, "/v1/documents/doc1/crdt/query/extra"},
		{http.MethodPost, "/v1/documents/doc1/crdt/queryextra"},
		{http.MethodPost, "/v1/documents//crdt/query"},
		{http.MethodPost, "/v1/documents/doc1/crdtx/query"},
		{http.MethodPost, "/v1/documents/doc1/crdt"},
	}
	for _, tc := range cases {
		t.Run(tc.method+" "+tc.path, func(t *testing.T) {
			r := httptest.NewRequest(tc.method, tc.path, strings.NewReader(`{"deviceId":"dev","ids":["a"]}`))
			r.Header.Set("Content-Type", "application/json")
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

// A document literally named "query" keeps its ordinary CRDT routes; the
// query keyword is only recognized in the terminal subresource position.
func TestQueryCRDTOpsHTTPDocumentNamedQuery(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev")
	w, _ := postJSON(t, h, "/v1/documents/query/crdt/ops", crdtCounterBody("dev", crdtCounterOp("z", 1)))
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	// The state read on the document named "query" works.
	if w := getRaw(t, h, "/v1/documents/query/crdt/state"); w.Code != http.StatusOK {
		t.Fatalf("state on doc named query = %d %s", w.Code, w.Body.String())
	}
	// The lookup subresource of that document answers normally too.
	if w := queryCRDTOps(t, h, "query", `{"deviceId":"dev","ids":["z"]}`); w.Code != http.StatusOK ||
		!strings.Contains(w.Body.String(), `"status":"found"`) {
		t.Fatalf("query on doc named query = %d %s", w.Code, w.Body.String())
	}
}

// After a process restart the same query yields the byte-identical body and
// verdicts, including found, missing and compacted answers.
func TestQueryCRDTOpsHTTPRestartStable(t *testing.T) {
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
	body := queryCRDTOps(t, h, "doc", `{"deviceId":"dev-1","ids":["a1","a2","nope"]}`).Body.String()
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}

	st2, err := app.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st2.Close() }()
	h2 := NewHandler(st2)
	w2 := queryCRDTOps(t, h2, "doc", `{"deviceId":"dev-1","ids":["a1","a2","nope"]}`)
	if w2.Code != http.StatusOK {
		t.Fatalf("after restart = %d %s", w2.Code, w2.Body.String())
	}
	if w2.Body.String() != body {
		t.Fatalf("body changed across restart:\nbefore %q\nafter  %q", body, w2.Body.String())
	}
}
