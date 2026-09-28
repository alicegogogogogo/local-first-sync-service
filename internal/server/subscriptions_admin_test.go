package server

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// rawRequest issues a plain HTTP request against the test server and returns
// the status, content type and raw body.
func rawRequest(t *testing.T, srv *httptest.Server, method, path string) (int, string, string) {
	t.Helper()
	req, err := http.NewRequest(method, srv.URL+path, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, resp.Header.Get("Content-Type"), string(data)
}

// listedSubscriptions parses the management list body.
func listedSubscriptions(t *testing.T, body string) struct {
	Subscriptions []subscriptionEntry `json:"subscriptions"`
	Count         int                 `json:"count"`
} {
	t.Helper()
	var got struct {
		Subscriptions []subscriptionEntry `json:"subscriptions"`
		Count         int                 `json:"count"`
	}
	if err := json.Unmarshal([]byte(body), &got); err != nil {
		t.Fatalf("list body is not JSON: %q (%v)", body, err)
	}
	return got
}

// waitForSubscriptionCount polls the list endpoint until it reports want
// entries, giving the upgraded connections' server goroutine time to register.
func waitForSubscriptionCount(t *testing.T, srv *httptest.Server, device string, want int) []subscriptionEntry {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		status, ct, body := rawRequest(t, srv, http.MethodGet, "/v1/devices/"+device+"/subscriptions")
		if status != http.StatusOK || ct != "application/json" {
			t.Fatalf("list = %d %q (%s)", status, ct, body)
		}
		got := listedSubscriptions(t, body)
		if got.Count == want && len(got.Subscriptions) == want {
			return got.Subscriptions
		}
		if time.Now().After(deadline) {
			t.Fatalf("list count never reached %d: %s", want, body)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// A registered device with no live subscription gets a 200 with an empty
// array and a zero count — an empty view is not an error.
func TestSubscriptionListEmpty(t *testing.T) {
	srv, _ := newWSTestServer(t)
	registerDeviceViaHTTP(t, srv, "dev-1")

	status, ct, body := rawRequest(t, srv, http.MethodGet, "/v1/devices/dev-1/subscriptions")
	if status != http.StatusOK || ct != "application/json" {
		t.Fatalf("list = %d %q (%s)", status, ct, body)
	}
	if body != `{"subscriptions":[],"count":0}`+"\n" {
		t.Fatalf("empty body = %q, want the fixed compact line", body)
	}
}

// The body is one compact JSON line plus a trailing newline; top-level keys
// come in the subscriptions-then-count order and every row keeps its fixed
// five-field order. Both channels, both identity flows (document-level and
// session-level) appear under the one owning device, in establishment order.
func TestSubscriptionListsAllKindsInEstablishmentOrder(t *testing.T) {
	srv, _ := newWSTestServer(t)
	setupSession(t, srv, "dev-1", "sess", "seed-doc", 0)

	// Establish four live connections in a known order, each on its own
	// document so catch-up frames cannot interfere.
	changeDoc, _ := dialWS(t, docSubscribeURL(srv, "doc-a", "dev-1", "5"))
	if changeDoc == nil {
		t.Fatal("doc change subscription did not upgrade")
	}
	defer changeDoc.close()
	changeSess, _ := dialWS(t, subscribeURL(srv, "sess", "doc-b", "3"))
	if changeSess == nil {
		t.Fatal("session change subscription did not upgrade")
	}
	defer changeSess.close()
	stateDoc, _ := dialWS(t, docCRDTSubscribeURL(srv, "doc-c", "dev-1"))
	if stateDoc == nil {
		t.Fatal("doc state subscription did not upgrade")
	}
	defer stateDoc.close()
	stateSess, _ := dialWS(t, crdtSubscribeURL(srv, "sess", "doc-d"))
	if stateSess == nil {
		t.Fatal("session state subscription did not upgrade")
	}
	defer stateSess.close()

	entries := waitForSubscriptionCount(t, srv, "dev-1", 4)

	want := []struct {
		doc  string
		kind string
		cur  int64
	}{
		{"doc-a", "changes", 5},
		{"doc-b", "changes", 3},
		{"doc-c", "state", 0},
		{"doc-d", "state", 0},
	}
	for i, w := range want {
		got := entries[i]
		if got.DocumentID != w.doc || got.Kind != w.kind || got.Cursor != w.cur {
			t.Fatalf("entry %d = %+v, want doc=%s kind=%s cursor=%d", i, got, w.doc, w.kind, w.cur)
		}
		if got.SubscriptionID == "" || got.EstablishedAt <= 0 {
			t.Fatalf("entry %d missing id or timestamp: %+v", i, got)
		}
		if i > 0 && got.SubscriptionID == entries[i-1].SubscriptionID {
			t.Fatalf("subscription ids must be unique, %q repeated", got.SubscriptionID)
		}
	}

	// Byte-level shape: compact, newline-terminated, fixed key order at both
	// levels.
	_, _, body := rawRequest(t, srv, http.MethodGet, "/v1/devices/dev-1/subscriptions")
	if !strings.HasSuffix(body, "\n") || strings.Count(body, "\n") != 1 {
		t.Fatalf("body must be one line plus a trailing newline: %q", body)
	}
	if strings.Contains(body, ", ") || strings.Contains(body, ": ") {
		t.Fatalf("body must be compact JSON: %q", body)
	}
	if !strings.HasPrefix(body, `{"subscriptions":[`) {
		t.Fatalf("top level must open with the subscriptions array: %q", body)
	}
	if !strings.HasSuffix(body, `,"count":4}`+"\n") {
		t.Fatalf("top level must end with the count: %q", body)
	}
	for _, e := range entries {
		row := fmt.Sprintf(`{"subscriptionId":"%s","documentId":"%s","kind":"%s","cursor":%d,"establishedAt":%d}`,
			e.SubscriptionID, e.DocumentID, e.Kind, e.Cursor, e.EstablishedAt)
		if !strings.Contains(body, row) {
			t.Fatalf("body must contain the fixed-order row %q in %q", row, body)
		}
	}

	// The view is per-device: a second device sees none of these.
	registerDeviceViaHTTP(t, srv, "dev-2")
	if other := waitForSubscriptionCount(t, srv, "dev-2", 0); len(other) != 0 {
		t.Fatalf("dev-2 must not see dev-1 subscriptions: %+v", other)
	}
}

// Canceling a change subscription closes exactly that connection with 4410
// and stops its pushes, while another live subscriber of the same document is
// untouched; the canceled id disappears from every list immediately.
func TestCancelChangeSubscriptionCloses4410OnlyTarget(t *testing.T) {
	srv, _ := newWSTestServer(t)
	setupSession(t, srv, "dev-1", "sess-1", "shared", 0)
	registerDeviceViaHTTP(t, srv, "dev-2")

	target, _ := dialWS(t, docSubscribeURL(srv, "shared", "dev-1", "0"))
	if target == nil {
		t.Fatal("target did not upgrade")
	}
	defer target.close()
	other, _ := dialWS(t, docSubscribeURL(srv, "shared", "dev-2", "0"))
	if other == nil {
		t.Fatal("other did not upgrade")
	}
	defer other.close()

	entries := waitForSubscriptionCount(t, srv, "dev-1", 1)
	id := entries[0].SubscriptionID

	status, ct, body := rawRequest(t, srv, http.MethodDelete,
		"/v1/devices/dev-1/subscriptions/"+id)
	if status != http.StatusOK || ct != "application/json" {
		t.Fatalf("cancel = %d %q (%s)", status, ct, body)
	}
	if body != fmt.Sprintf(`{"subscriptionId":%q,"deleted":true}`+"\n", id) {
		t.Fatalf("cancel body = %q, want the id and deletion marker", body)
	}

	if code := target.readCloseCode(); code != 4410 {
		t.Fatalf("target close code = %d, want 4410", code)
	}

	if got := waitForSubscriptionCount(t, srv, "dev-1", 0); len(got) != 0 {
		t.Fatalf("canceled id still listed: %+v", got)
	}

	// The other subscriber keeps receiving commits; the target never does.
	postDocChange(t, srv, "dev-1", "shared", "after-cancel", map[string]any{"n": 1})
	row := other.readChange()
	if row.ID != "after-cancel" {
		t.Fatalf("other subscriber got %q, want the live commit", row.ID)
	}
	other.clearReadDeadline()

	target.setReadDeadline(300 * time.Millisecond)
	if _, _, _, ok := target.readFrameMaybe(); ok {
		t.Fatal("the canceled connection must stay closed")
	}
}

// Canceling a CRDT state subscription likewise ends it with 4410.
func TestCancelStateSubscriptionCloses4410(t *testing.T) {
	srv, _ := newWSTestServer(t)
	registerDeviceViaHTTP(t, srv, "dev-1")

	conn, _ := dialWS(t, docCRDTSubscribeURL(srv, "doc", "dev-1"))
	if conn == nil {
		t.Fatal("state subscription did not upgrade")
	}
	defer conn.close()

	id := waitForSubscriptionCount(t, srv, "dev-1", 1)[0].SubscriptionID
	status, _, body := rawRequest(t, srv, http.MethodDelete, "/v1/devices/dev-1/subscriptions/"+id)
	if status != http.StatusOK {
		t.Fatalf("cancel = %d (%s)", status, body)
	}
	if code := conn.readCloseCode(); code != 4410 {
		t.Fatalf("state subscription close code = %d, want 4410", code)
	}
}

// A client-initiated disconnect removes the entry from the memory view.
func TestSubscriptionDisappearsOnClientDisconnect(t *testing.T) {
	srv, _ := newWSTestServer(t)
	registerDeviceViaHTTP(t, srv, "dev-1")

	conn, _ := dialWS(t, docSubscribeURL(srv, "doc", "dev-1", "0"))
	if conn == nil {
		t.Fatal("subscription did not upgrade")
	}
	waitForSubscriptionCount(t, srv, "dev-1", 1)

	conn.sendClose(1000)
	conn.close()
	waitForSubscriptionCount(t, srv, "dev-1", 0)
}

// All 404 cases answer the same terse JSON error and reveal nothing about
// other devices' subscriptions; checks run shape -> device -> ownership.
func TestSubscriptionManagementNotFoundCases(t *testing.T) {
	srv, _ := newWSTestServer(t)
	setupSession(t, srv, "dev-1", "sess-1", "doc", 0)
	registerDeviceViaHTTP(t, srv, "dev-2")

	conn, _ := dialWS(t, docSubscribeURL(srv, "doc", "dev-1", "0"))
	if conn == nil {
		t.Fatal("subscription did not upgrade")
	}
	defer conn.close()
	id := waitForSubscriptionCount(t, srv, "dev-1", 1)[0].SubscriptionID

	cases := []struct {
		name   string
		method string
		path   string
	}{
		{"unregistered device lists", http.MethodGet, "/v1/devices/ghost/subscriptions"},
		{"unregistered device cancels", http.MethodDelete, "/v1/devices/ghost/subscriptions/" + id},
		{"unknown subscription id", http.MethodDelete, "/v1/devices/dev-1/subscriptions/sub-nope"},
		{"subscription owned by another device", http.MethodDelete, "/v1/devices/dev-2/subscriptions/" + id},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			status, ct, body := rawRequest(t, srv, tc.method, tc.path)
			if status != http.StatusNotFound || ct != "application/json" {
				t.Fatalf("%s = %d %q (%s)", tc.name, status, ct, body)
			}
			var errBody map[string]string
			if err := json.Unmarshal([]byte(body), &errBody); err != nil || errBody["error"] == "" {
				t.Fatalf("body = %q, want a JSON error", body)
			}
			if strings.Contains(body, id) || strings.Contains(body, "doc") {
				t.Fatalf("error body leaks subscription content: %q", body)
			}
		})
	}

	// The foreign cancel left the real connection live and unpushed.
	conn.setReadDeadline(300 * time.Millisecond)
	if _, _, _, ok := conn.readFrameMaybe(); ok {
		t.Fatal("a rejected foreign cancel disturbed the connection")
	}
	conn.clearReadDeadline()

	// The real owner cancels successfully; an immediate repeat is a 404 that
	// changes nothing.
	if status, _, _ := rawRequest(t, srv, http.MethodDelete, "/v1/devices/dev-1/subscriptions/"+id); status != http.StatusOK {
		t.Fatalf("first cancel = %d, want 200", status)
	}
	if code := conn.readCloseCode(); code != 4410 {
		t.Fatalf("close code = %d, want 4410", code)
	}
	status, _, body := rawRequest(t, srv, http.MethodDelete, "/v1/devices/dev-1/subscriptions/"+id)
	if status != http.StatusNotFound {
		t.Fatalf("repeat cancel = %d (%s), want 404", status, body)
	}
}

// Every malformed shape — empty identifier, trailing slash, missing or extra
// segment, wrong method — is a JSON 400 before any device lookup, never a
// redirect or HTML.
func TestSubscriptionManagementBadShape(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")

	cases := []struct {
		name   string
		method string
		path   string
	}{
		{"empty device segment on list", http.MethodGet, "/v1/devices//subscriptions"},
		{"trailing slash on list", http.MethodGet, "/v1/devices/dev-1/subscriptions/"},
		{"trailing slash on item", http.MethodDelete, "/v1/devices/dev-1/subscriptions/sub-1/"},
		{"delete missing the id segment", http.MethodDelete, "/v1/devices/dev-1/subscriptions"},
		{"extra segment on item", http.MethodDelete, "/v1/devices/dev-1/subscriptions/sub-1/extra"},
		{"post on the collection", http.MethodPost, "/v1/devices/dev-1/subscriptions"},
		{"put on the collection", http.MethodPut, "/v1/devices/dev-1/subscriptions"},
		{"get on the item path", http.MethodGet, "/v1/devices/dev-1/subscriptions/sub-1"},
		{"put on the item path", http.MethodPut, "/v1/devices/dev-1/subscriptions/sub-1"},
		{"get with an extra segment", http.MethodGet, "/v1/devices/dev-1/subscriptions/sub-1/extra"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest(tc.method, tc.path, nil)
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, r)

			assertJSONErrorStatus(t, rec, http.StatusBadRequest)
			if loc := rec.Header().Get("Location"); loc != "" {
				t.Fatalf("malformed request produced a redirect to %q", loc)
			}
		})
	}
}
