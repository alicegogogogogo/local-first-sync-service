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

// compactCRDT posts a CRDT compaction and fails the test unless it is a 200.
func compactCRDT(t *testing.T, h http.Handler, doc, device string) {
	t.Helper()
	w, _ := postJSON(t, h, "/v1/documents/"+doc+"/crdt/compact", map[string]any{"deviceId": device})
	if w.Code != http.StatusOK {
		t.Fatalf("compact %s = %d %s", doc, w.Code, w.Body.String())
	}
}

// Found answers for all four types carry the source device and that type's
// comparison content exactly as saved, in request order, with unknown ids
// answered missing in line.
func TestQueryCRDTOpsHTTPSuccessAllTypes(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	registerDevice(t, h, "dev-2")

	if w := submitCRDT(t, h, "doc-counter", crdtCounterBody("dev-1",
		crdtCounterOp("a1", 5), crdtCounterOp("a2", 8))); w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	if w := submitCRDT(t, h, "doc-counter", crdtCounterBody("dev-2", crdtCounterOp("b1", 3))); w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	if w := submitCRDT(t, h, "doc-gset", crdtGSetBody("dev-1",
		crdtGSetOp("g1", "banana", "apple"))); w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	if w, _ := postJSON(t, h, "/v1/documents/doc-register/crdt/ops", map[string]any{
		"deviceId": "dev-1", "type": "register", "ops": []map[string]any{
			{"id": "r1", "version": 1, "value": map[string]any{"k": "v"}},
			{"id": "r2", "version": 2, "value": nil},
		},
	}); w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	if w := submitCRDT(t, h, "doc-orset", crdtORSetBody("dev-1",
		crdtORSetOp("o1", "add", "apple"), crdtORSetOp("o2", "remove", "apple"))); w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}

	// Counter: request order is honored, the found row names the source
	// device, the missing id is a bare in-line answer.
	w := queryCRDTOps(t, h, "doc-counter", `{"deviceId":"dev-1","ids":["b1","nope","a1"]}`)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", w.Code, w.Body.String())
	}
	want := `{"results":[` +
		`{"id":"b1","status":"found","deviceId":"dev-2","value":3},` +
		`{"id":"nope","status":"missing"},` +
		`{"id":"a1","status":"found","deviceId":"dev-1","value":5}` +
		`],"count":3}` + "\n"
	if w.Body.String() != want {
		t.Fatalf("counter body = %q\nwant %q", w.Body.String(), want)
	}

	// GSet: the element set is presented in its saved array order.
	w = queryCRDTOps(t, h, "doc-gset", `{"deviceId":"dev-1","ids":["g1","x"]}`)
	want = `{"results":[` +
		`{"id":"g1","status":"found","deviceId":"dev-1","elements":["banana","apple"]},` +
		`{"id":"x","status":"missing"}` +
		`],"count":2}` + "\n"
	if w.Body.String() != want {
		t.Fatalf("gset body = %q\nwant %q", w.Body.String(), want)
	}

	// Register: version then value, the saved value verbatim, null included.
	w = queryCRDTOps(t, h, "doc-register", `{"deviceId":"dev-1","ids":["r2","r1"]}`)
	want = `{"results":[` +
		`{"id":"r2","status":"found","deviceId":"dev-1","version":2,"value":null},` +
		`{"id":"r1","status":"found","deviceId":"dev-1","version":1,"value":{"k":"v"}}` +
		`],"count":2}` + "\n"
	if w.Body.String() != want {
		t.Fatalf("register body = %q\nwant %q", w.Body.String(), want)
	}

	// ORSet: action and element, adds and removes alike.
	w = queryCRDTOps(t, h, "doc-orset", `{"deviceId":"dev-1","ids":["o1","o2"]}`)
	want = `{"results":[` +
		`{"id":"o1","status":"found","deviceId":"dev-1","action":"add","element":"apple"},` +
		`{"id":"o2","status":"found","deviceId":"dev-1","action":"remove","element":"apple"}` +
		`],"count":2}` + "\n"
	if w.Body.String() != want {
		t.Fatalf("orset body = %q\nwant %q", w.Body.String(), want)
	}
}

// A register value is presented exactly as the JSON content saved at submit
// time: the submitted bytes (key order included) round-trip verbatim.
func TestQueryCRDTOpsHTTPRegisterValueVerbatim(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev")
	w, _ := postJSON(t, h, "/v1/documents/doc/crdt/ops",
		`{"deviceId":"dev","type":"register","ops":[{"id":"r1","version":1,"value":{"arr":[1,true,null,"s2"],"nested":{"z":1,"a":[{}]}}}]}`)
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}

	w = queryCRDTOps(t, h, "doc", `{"deviceId":"dev","ids":["r1"]}`)
	want := `{"results":[{"id":"r1","status":"found","deviceId":"dev","version":1,"value":{"arr":[1,true,null,"s2"],"nested":{"z":1,"a":[{}]}}}],"count":1}` + "\n"
	if w.Body.String() != want {
		t.Fatalf("body = %q\nwant %q", w.Body.String(), want)
	}
}

// A document with no committed CRDT operation answers 404 — the same verdict
// the state and snapshot reads give, not a page of missing entries — even for
// a registered, authorized device. A missing id is a normal in-line answer
// only on a document that has CRDT operations.
func TestQueryCRDTOpsHTTPMissing(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev")

	for _, doc := range []string{"never-heard-of-it", "empty-doc"} {
		w := queryCRDTOps(t, h, doc, `{"deviceId":"dev","ids":["x","y"]}`)
		if w.Code != http.StatusNotFound {
			t.Fatalf("%s status = %d, want 404 body = %s", doc, w.Code, w.Body.String())
		}
		if strings.Contains(w.Body.String(), `"results"`) {
			t.Fatalf("%s 404 leaked results: %s", doc, w.Body.String())
		}
	}

	// On a document that does have CRDT operations, unknown ids are normal
	// bare missing answers in line.
	if w := submitCRDT(t, h, "doc1", crdtCounterBody("dev", crdtCounterOp("a1", 1))); w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	w := queryCRDTOps(t, h, "doc1", `{"deviceId":"dev","ids":["x","a1","y"]}`)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", w.Code, w.Body.String())
	}
	want := `{"results":[` +
		`{"id":"x","status":"missing"},` +
		`{"id":"a1","status":"found","deviceId":"dev","value":1},` +
		`{"id":"y","status":"missing"}` +
		`],"count":3}` + "\n"
	if w.Body.String() != want {
		t.Fatalf("body = %q want %q", w.Body.String(), want)
	}
}

// A compacted-away id reports the trimmed status alone — no device and no
// content restored from the retained summary — while the surviving online op
// is still found with full content.
func TestQueryCRDTOpsHTTPCompacted(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")

	submitCounter := func(id string, value int) {
		t.Helper()
		if w := submitCRDT(t, h, "doc-c", crdtCounterBody("dev-1", crdtCounterOp(id, value))); w.Code != http.StatusOK {
			t.Fatal(w.Body.String())
		}
	}
	submitCounter("a1", 5)
	submitCounter("a2", 8)
	compactCRDT(t, h, "doc-c", "dev-1")

	if w, _ := postJSON(t, h, "/v1/documents/doc-r/crdt/ops", map[string]any{
		"deviceId": "dev-1", "type": "register", "ops": []map[string]any{
			{"id": "r1", "version": 1, "value": "old"},
			{"id": "r2", "version": 2, "value": "new"},
		},
	}); w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	compactCRDT(t, h, "doc-r", "dev-1")

	w := queryCRDTOps(t, h, "doc-c", `{"deviceId":"dev-1","ids":["a1","a2","gone-never"]}`)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", w.Code, w.Body.String())
	}
	want := `{"results":[` +
		`{"id":"a1","status":"compacted"},` +
		`{"id":"a2","status":"found","deviceId":"dev-1","value":8},` +
		`{"id":"gone-never","status":"missing"}` +
		`],"count":3}` + "\n"
	if w.Body.String() != want {
		t.Fatalf("counter body = %q\nwant %q", w.Body.String(), want)
	}

	w = queryCRDTOps(t, h, "doc-r", `{"deviceId":"dev-1","ids":["r1","r2"]}`)
	want = `{"results":[` +
		`{"id":"r1","status":"compacted"},` +
		`{"id":"r2","status":"found","deviceId":"dev-1","version":2,"value":"new"}` +
		`],"count":2}` + "\n"
	if w.Body.String() != want {
		t.Fatalf("register body = %q\nwant %q", w.Body.String(), want)
	}
}

// Every request-shape violation is a 400 JSON error with zero writes, checked
// before the device even exists.
func TestQueryCRDTOpsHTTPRejectsBadShape(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev")
	if w := submitCRDT(t, h, "doc1", crdtCounterBody("dev", crdtCounterOp("a1", 1))); w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
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
		name        string
		contentType string
		body        string
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
		{"extra fields are fine", "application/json", `{"deviceId":"dev","ids":["a"],"type":"counter","extra":1}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w := send(tc.contentType, tc.body)
			if tc.name == "extra fields are fine" {
				if w.Code != http.StatusOK {
					t.Fatalf("extra fields = %d body = %s", w.Code, w.Body.String())
				}
				return
			}
			if w.Code != http.StatusBadRequest {
				t.Fatalf("status = %d want 400 body = %s", w.Code, w.Body.String())
			}
			if ct := w.Header().Get("Content-Type"); ct != "application/json" {
				t.Fatalf("content type = %q", ct)
			}
			if !strings.HasPrefix(w.Body.String(), `{"error":`) {
				t.Fatalf("body = %q, want a JSON error", w.Body.String())
			}
		})
	}

	// A malformed body against an unregistered device is still a 400: shape
	// validation precedes device existence.
	w := send("application/json", `{"deviceId":"ghost","ids":[]}`)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("shape-before-existence = %d body = %s", w.Code, w.Body.String())
	}
}

// Device existence, document permission and CRDT state existence are enforced
// in that order after shape validation, and a failure exposes no content.
func TestQueryCRDTOpsHTTPGate(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev")
	registerDevice(t, h, "revoked")
	if w := submitCRDT(t, h, "doc1", crdtCounterBody("dev", crdtCounterOp("a1", 1))); w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}

	// Unregistered device: 404 with an error only.
	w := queryCRDTOps(t, h, "doc1", `{"deviceId":"ghost","ids":["a1"]}`)
	if w.Code != http.StatusNotFound {
		t.Fatalf("unknown device = %d body = %s", w.Code, w.Body.String())
	}
	if strings.Contains(w.Body.String(), `"results"`) || strings.Contains(w.Body.String(), `"value"`) {
		t.Fatalf("404 leaked content: %s", w.Body.String())
	}

	// Revoked permission: 403 with an error only.
	w, _ = postJSON(t, h, "/v1/documents/doc1/permissions", map[string]any{"deviceId": "revoked", "action": "revoke"})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	w = queryCRDTOps(t, h, "doc1", `{"deviceId":"revoked","ids":["a1"]}`)
	if w.Code != http.StatusForbidden {
		t.Fatalf("revoked device = %d body = %s", w.Code, w.Body.String())
	}
	if strings.Contains(w.Body.String(), `"results"`) || strings.Contains(w.Body.String(), `"value"`) {
		t.Fatalf("403 leaked content: %s", w.Body.String())
	}

	// The permission verdict precedes the CRDT-state verdict: a revoked device
	// querying a document with no CRDT ops still gets 403.
	w, _ = postJSON(t, h, "/v1/documents/no-state/permissions", map[string]any{"deviceId": "revoked", "action": "revoke"})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	w = queryCRDTOps(t, h, "no-state", `{"deviceId":"revoked","ids":["a1"]}`)
	if w.Code != http.StatusForbidden {
		t.Fatalf("revoked on no-state doc = %d, want 403", w.Code)
	}
}

// The lookup is read-only: it allocates no change cursor, writes no CRDT op,
// wakes neither a parked long poll nor a CRDT state push subscriber.
func TestQueryCRDTOpsHTTPReadOnly(t *testing.T) {
	h, s := newTestHandler(t)
	registerDevice(t, h, "dev")
	if w := submitCRDT(t, h, "doc1", crdtCounterBody("dev", crdtCounterOp("a1", 5))); w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	// Give the document a change log so a long poll actually parks; an
	// events-unknown document returns an empty page immediately.
	postRawChanges(t, h, "doc1", "dev", `[{"id":"c1","payload":1}]`)

	// A parked change long poll on the document must time out despite CRDT
	// queries hitting the same document.
	done := make(chan struct {
		changes  int
		timedOut bool
		err      error
	}, 1)
	go func() {
		changes, _, timedOut, err := s.WaitForChanges(t.Context(), "doc1", 1, 100, 250*time.Millisecond)
		done <- struct {
			changes  int
			timedOut bool
			err      error
		}{len(changes), timedOut, err}
	}()
	time.Sleep(30 * time.Millisecond)
	for i := 0; i < 3; i++ {
		if w := queryCRDTOps(t, h, "doc1", `{"deviceId":"dev","ids":["a1","missing"]}`); w.Code != http.StatusOK {
			t.Fatalf("query %d = %d %s", i, w.Code, w.Body.String())
		}
	}
	got := <-done
	if got.err != nil || !got.timedOut || got.changes != 0 {
		t.Fatalf("long poll woken by a read-only query: %+v", got)
	}

	// The queries stored nothing: a fresh id is still free and the snapshot
	// operation count is unchanged.
	if w := submitCRDT(t, h, "doc1", crdtCounterBody("dev", crdtCounterOp("a2", 6))); w.Code != http.StatusOK {
		t.Fatalf("submit after queries = %d %s", w.Code, w.Body.String())
	}
	snap, err := s.GetCRDTSnapshot("doc1")
	if err != nil {
		t.Fatal(err)
	}
	if snap.Operations != 2 {
		t.Fatalf("operations after queries = %d, want 2", snap.Operations)
	}
}

// Method and path shape: only POST at the exact .../crdt/query location is
// accepted; every other verb or shape is a JSON 400, never a redirect.
func TestQueryCRDTOpsHTTPMethodAndPath(t *testing.T) {
	h, _ := newTestHandler(t)

	cases := []struct {
		method string
		path   string
	}{
		{http.MethodGet, "/v1/documents/doc1/crdt/query"},
		{http.MethodPut, "/v1/documents/doc1/crdt/query"},
		{http.MethodDelete, "/v1/documents/doc1/crdt/query"},
		{http.MethodPost, "/v1/documents/doc1/crdt/query/"},
		{http.MethodPost, "/v1/documents/doc1/crdt/query/extra"},
		{http.MethodPost, "/v1/documents/doc1/crdt/queryextra"},
		{http.MethodPost, "/v1/documents//crdt/query"},
		{http.MethodPost, "/v1/documents/doc1/crdtx/query"},
		{http.MethodPost, "/v1/documents/doc1/changes/queryx"},
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
	if w := submitCRDT(t, h, "query", crdtCounterBody("dev", crdtCounterOp("z", 1))); w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	if got := crdtStateBody(t, h, "query"); got != `{"type":"counter","value":1}`+"\n" {
		t.Fatalf("state on doc named query = %q", got)
	}
	w := queryCRDTOps(t, h, "query", `{"deviceId":"dev","ids":["z"]}`)
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `"status":"found"`) {
		t.Fatalf("query on doc named query = %d %s", w.Code, w.Body.String())
	}
}

// The query verdict derives solely from durable data: after a process restart
// the same request yields the byte-identical body, including found, missing
// and compacted answers.
func TestQueryCRDTOpsHTTPRestartStable(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "sync.db")

	st, err := app.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	h := NewHandler(st)
	registerDevice(t, h, "dev-1")
	if w := submitCRDT(t, h, "doc", crdtCounterBody("dev-1",
		crdtCounterOp("a1", 5), crdtCounterOp("a2", 8))); w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	compactCRDT(t, h, "doc", "dev-1")
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
	r := httptest.NewRequest(http.MethodPost, "/v1/documents/doc/crdt/query",
		strings.NewReader(`{"deviceId":"dev-1","ids":["a1","a2","nope"]}`))
	r.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h2.ServeHTTP(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("after restart = %d %s", w.Code, w.Body.String())
	}
	if w.Body.String() != body {
		t.Fatalf("body changed across restart:\nbefore %q\nafter  %q", body, w.Body.String())
	}
}
