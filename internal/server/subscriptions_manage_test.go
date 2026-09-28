package server

import (
	"encoding/json"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/alicegogogogogo/local-first-sync-service/internal/app"
)

// httpDelete issues a bodyless DELETE and returns the status and raw body.
func httpDelete(t *testing.T, srvURL, path string) (int, []byte) {
	t.Helper()
	req, err := http.NewRequest(http.MethodDelete, srvURL+path, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := srvHTTPClient().Do(req)
	if err != nil {
		t.Fatalf("DELETE %s: %v", path, err)
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, data
}

// srvHTTPClient returns a client with a short timeout; tests always talk to a
// loopback httptest server.
func srvHTTPClient() *http.Client { return &http.Client{Timeout: 5 * time.Second} }

// listSubscriptionsBody GETs a device's live-subscription view and returns
// the decoded rows plus the exact raw body.
func listSubscriptionsBody(t *testing.T, baseURL, device string) (int, []subscriptionListEntry, []byte) {
	t.Helper()
	resp, err := srvHTTPClient().Get(baseURL + "/v1/devices/" + device + "/subscriptions")
	if err != nil {
		t.Fatalf("GET subscriptions: %v", err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	var body struct {
		Subscriptions []subscriptionListEntry `json:"subscriptions"`
		Count         int                     `json:"count"`
	}
	_ = json.Unmarshal(raw, &body)
	return resp.StatusCode, body.Subscriptions, raw
}

// waitForSubscriptionCount polls the live view until it reports want rows or
// fails the test on timeout.
func waitForSubscriptionCount(t *testing.T, baseURL, device string, want int) []subscriptionListEntry {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	var rows []subscriptionListEntry
	for time.Now().Before(deadline) {
		status, got, raw := listSubscriptionsBody(t, baseURL, device)
		if status != http.StatusOK {
			t.Fatalf("list status = %d body = %s", status, raw)
		}
		if len(got) == want {
			return got
		}
		rows = got
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("subscription count for %q = %d, want %d", device, len(rows), want)
	return nil
}

// A registered device without any live subscription still answers 200 with an
// empty array and a zero count, as one compact JSON line plus a newline.
func TestManageSubscriptionsEmptyList(t *testing.T) {
	srv, _ := newWSTestServer(t)
	registerDeviceViaHTTP(t, srv, "dev-1")

	status, rows, raw := listSubscriptionsBody(t, srv.URL, "dev-1")
	if status != http.StatusOK {
		t.Fatalf("status = %d, want 200, body = %s", status, raw)
	}
	if len(rows) != 0 {
		t.Fatalf("rows = %v, want none", rows)
	}
	if got, want := string(raw), `{"subscriptions":[],"count":0}`+"\n"; got != want {
		t.Fatalf("body = %q, want %q", got, want)
	}
}

// The view lists both document-level subscriptions the device declared and
// session-level subscriptions opened through a session it owns, across both
// push channels, in connection-establishment order with fixed key order.
func TestManageSubscriptionsListsBothChannels(t *testing.T) {
	srv, _ := newWSTestServer(t)
	setupSession(t, srv, "dev-1", "sess-1", "docA", 1)
	registerDeviceViaHTTP(t, srv, "dev-2")
	for _, id := range []string{"docB", "docC"} {
		postDocChange(t, srv, "dev-2", id, "seed", map[string]any{"n": 1})
	}

	// Open, in order: a session change subscription on docA at cursor 0, a
	// document-level change subscription on docB at cursor 1, a session state
	// subscription on docA (cursor always 0), and a document-level state
	// subscription on docC.
	c1, hs := dialWS(t, subscribeURL(srv, "sess-1", "docA", "0"))
	if hs.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("session changes handshake = %d", hs.StatusCode)
	}
	defer c1.close()
	c2, hs := dialWS(t, docSubscribeURL(srv, "docB", "dev-2", "1"))
	if hs.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("document changes handshake = %d", hs.StatusCode)
	}
	defer c2.close()
	c3, hs := dialWS(t, crdtSubscribeURL(srv, "sess-1", "docA"))
	if hs.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("session state handshake = %d", hs.StatusCode)
	}
	defer c3.close()
	c4, hs := dialWS(t, docCRDTSubscribeURL(srv, "docC", "dev-2"))
	if hs.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("document state handshake = %d", hs.StatusCode)
	}
	defer c4.close()

	rows := waitForSubscriptionCount(t, srv.URL, "dev-2", 2)
	want := []subscriptionListEntry{
		{DocumentID: "docB", Channel: app.PushChannelChanges, Cursor: 1},
		{DocumentID: "docC", Channel: app.PushChannelState, Cursor: 0},
	}
	if rows[0].SubscriptionID == "" || rows[0].SubscriptionID == rows[1].SubscriptionID {
		t.Fatalf("subscription ids must be allocated and unique: %q, %q", rows[0].SubscriptionID, rows[1].SubscriptionID)
	}
	for i, w := range want {
		if rows[i].DocumentID != w.DocumentID || rows[i].Channel != w.Channel || rows[i].Cursor != w.Cursor {
			t.Fatalf("row %d = %+v, want doc=%s channel=%s cursor=%d", i, rows[i], w.DocumentID, w.Channel, w.Cursor)
		}
		if rows[i].EstablishedAt <= 0 {
			t.Fatalf("row %d establishedAt = %d, want a positive Unix millis value", i, rows[i].EstablishedAt)
		}
	}
	if rows[1].EstablishedAt < rows[0].EstablishedAt {
		t.Fatalf("rows are not in establishment order: %d before %d", rows[0].EstablishedAt, rows[1].EstablishedAt)
	}

	// dev-1 sees its own two subscriptions, keyed by the session's owning
	// device, and never dev-2's.
	rows1 := waitForSubscriptionCount(t, srv.URL, "dev-1", 2)
	if rows1[0].DocumentID != "docA" || rows1[0].Channel != app.PushChannelChanges || rows1[0].Cursor != 0 {
		t.Fatalf("dev-1 row 0 = %+v", rows1[0])
	}
	if rows1[1].DocumentID != "docA" || rows1[1].Channel != app.PushChannelState || rows1[1].Cursor != 0 {
		t.Fatalf("dev-1 row 1 = %+v", rows1[1])
	}

	// The raw body has the fixed top-level and per-entry key order.
	_, _, raw := listSubscriptionsBody(t, srv.URL, "dev-1")
	var anyBody any
	if err := json.Unmarshal(raw, &anyBody); err != nil || raw[len(raw)-1] != '\n' {
		t.Fatalf("body is not newline-terminated JSON: %q", raw)
	}
	if wantPrefix := `{"subscriptions":[{"subscriptionId":"` + rows1[0].SubscriptionID + `","documentId":"docA","channel":"changes","cursor":0,"establishedAt":`; string(raw[:len(wantPrefix)]) != wantPrefix {
		t.Fatalf("body key order = %q, want prefix %q", raw, wantPrefix)
	}
}

// A disconnect removes the entry immediately, with no persistence.
func TestManageSubscriptionsDisconnectRemovesEntry(t *testing.T) {
	srv, _ := newWSTestServer(t)
	registerAndSeedDevice(t, srv, "dev-1", "doc", 0)

	c, hs := dialWS(t, docSubscribeURL(srv, "doc", "dev-1", "0"))
	if hs.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("handshake = %d", hs.StatusCode)
	}
	waitForSubscriptionCount(t, srv.URL, "dev-1", 1)

	c.close()
	waitForSubscriptionCount(t, srv.URL, "dev-1", 0)
}

// A successful cancel ends exactly the target connection with 4410, answers
// the compact deletion line, removes the entry, and leaves another
// subscriber of the same document fully alive.
func TestManageSubscriptionsCancelEndsOneConnection4410(t *testing.T) {
	srv, _ := newWSTestServer(t)
	setupSession(t, srv, "dev-1", "sess-1", "doc", 0)
	registerDeviceViaHTTP(t, srv, "dev-2")

	target, hs := dialWS(t, subscribeURL(srv, "sess-1", "doc", "0"))
	if hs.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("target handshake = %d", hs.StatusCode)
	}
	other, hs := dialWS(t, docSubscribeURL(srv, "doc", "dev-2", "0"))
	if hs.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("other handshake = %d", hs.StatusCode)
	}
	defer other.close()

	rows := waitForSubscriptionCount(t, srv.URL, "dev-1", 1)
	id := rows[0].SubscriptionID

	status, raw := httpDelete(t, srv.URL, "/v1/devices/dev-1/subscriptions/"+id)
	if status != http.StatusOK {
		t.Fatalf("cancel status = %d, want 200, body = %s", status, raw)
	}
	if got, want := string(raw), `{"subscriptionId":"`+id+`","deleted":true}`+"\n"; got != want {
		t.Fatalf("cancel body = %q, want %q", got, want)
	}

	// The target connection closes with 4410 and pushes stop.
	target.setReadDeadline(2 * time.Second)
	if code := target.readCloseCode(); code != wsCloseSubscriptionCanceled {
		t.Fatalf("target close code = %d, want 4410", code)
	}
	target.close()

	// The entry is gone at once.
	waitForSubscriptionCount(t, srv.URL, "dev-1", 0)

	// The other device's subscription on the same document is untouched: it
	// still answers a ping with a pong and receives a later commit.
	other.sendPing()
	fin, opcode, _ := other.readFrame()
	if !fin || opcode != wsOpcodePong {
		t.Fatalf("other subscription is not alive: fin=%v opcode=%d", fin, opcode)
	}
	postDocChange(t, srv, "dev-2", "doc", "after-cancel", map[string]any{"n": 9})
	other.setReadDeadline(2 * time.Second)
	if change := other.readChange(); change.ID != "after-cancel" {
		t.Fatalf("other subscriber got change %q, want after-cancel", change.ID)
	}
}

// A repeated cancel, an unknown id and another device's id are all 404 and
// change nothing; an unregistered caller is a 404 as well, before any
// subscription content could leak.
func TestManageSubscriptionsCancelFailures404(t *testing.T) {
	srv, _ := newWSTestServer(t)
	setupSession(t, srv, "dev-1", "sess-1", "doc", 0)
	registerDeviceViaHTTP(t, srv, "dev-2")

	target, hs := dialWS(t, subscribeURL(srv, "sess-1", "doc", "0"))
	if hs.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("handshake = %d", hs.StatusCode)
	}
	defer target.close()
	id := waitForSubscriptionCount(t, srv.URL, "dev-1", 1)[0].SubscriptionID

	assert404 := func(path string) {
		t.Helper()
		status, raw := httpDelete(t, srv.URL, path)
		if status != http.StatusNotFound {
			t.Fatalf("DELETE %s = %d, want 404, body = %s", path, status, raw)
		}
		var body map[string]string
		if err := json.Unmarshal(raw, &body); err != nil || body["error"] == "" {
			t.Fatalf("DELETE %s body = %q, want a JSON error", path, raw)
		}
	}

	// Another device cannot end dev-1's subscription and learns nothing.
	assert404("/v1/devices/dev-2/subscriptions/" + id)
	// Unknown id under a registered device.
	assert404("/v1/devices/dev-1/subscriptions/does-not-exist")
	// Unregistered caller, even naming a live id.
	assert404("/v1/devices/ghost/subscriptions/" + id)

	// The target is still live (the failed cancels changed nothing): pong.
	target.sendPing()
	fin, opcode, _ := target.readFrame()
	if !fin || opcode != wsOpcodePong {
		t.Fatalf("target closed by a failed cancel: fin=%v opcode=%d", fin, opcode)
	}

	// The real cancel succeeds once.
	if status, raw := httpDelete(t, srv.URL, "/v1/devices/dev-1/subscriptions/"+id); status != http.StatusOK {
		t.Fatalf("cancel = %d body = %s", status, raw)
	}
	target.setReadDeadline(2 * time.Second)
	if code := target.readCloseCode(); code != wsCloseSubscriptionCanceled {
		t.Fatalf("close code = %d, want 4410", code)
	}

	// A repeated cancel after the connection ended is the same 404.
	assert404("/v1/devices/dev-1/subscriptions/" + id)
}

// A state-subscription cancel uses the same 4410 path; 4403 and 1001 remain
// the only revoke/shutdown codes and are exercised by the existing suites.
func TestManageSubscriptionsCancelStateChannel4410(t *testing.T) {
	srv, _ := newWSTestServer(t)
	setupSession(t, srv, "dev-1", "sess-1", "doc", 0)

	c, hs := dialWS(t, crdtSubscribeURL(srv, "sess-1", "doc"))
	if hs.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("handshake = %d", hs.StatusCode)
	}
	id := waitForSubscriptionCount(t, srv.URL, "dev-1", 1)[0].SubscriptionID
	if status, _ := httpDelete(t, srv.URL, "/v1/devices/dev-1/subscriptions/"+id); status != http.StatusOK {
		t.Fatalf("cancel status = %d", status)
	}
	c.setReadDeadline(2 * time.Second)
	if code := c.readCloseCode(); code != wsCloseSubscriptionCanceled {
		t.Fatalf("state subscription close code = %d, want 4410", code)
	}
	c.close()
	waitForSubscriptionCount(t, srv.URL, "dev-1", 0)
}

// Request shape is checked first (400), then device existence (404), then
// subscription ownership (404). Every shape failure is a JSON 400 — never a
// redirect or HTML — including for an unregistered device.
func TestManageSubscriptionsRequestShapeAndOrder(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")

	assert400 := func(method, path string) {
		t.Helper()
		r := newJSONRequest(method, path, "", "")
		w := serveRecorder(h, r)
		if w.Code != http.StatusBadRequest {
			t.Fatalf("%s %s = %d, want 400, body = %s", method, path, w.Code, w.Body.String())
		}
		assertJSONError(t, w)
	}

	// Listing: GET only; malformed shapes rejected regardless of device.
	assert400(http.MethodPost, "/v1/devices/dev-1/subscriptions")
	assert400(http.MethodPut, "/v1/devices/dev-1/subscriptions")
	assert400(http.MethodDelete, "/v1/devices/dev-1/subscriptions")
	assert400(http.MethodPatch, "/v1/devices/dev-1/subscriptions")
	// Item: DELETE only.
	assert400(http.MethodGet, "/v1/devices/dev-1/subscriptions/sub-1")
	assert400(http.MethodPost, "/v1/devices/dev-1/subscriptions/sub-1")
	assert400(http.MethodPut, "/v1/devices/dev-1/subscriptions/sub-1")
	// Missing/extra segments and empty identifiers.
	assert400(http.MethodGet, "/v1/devices//subscriptions")
	assert400(http.MethodDelete, "/v1/devices//subscriptions/sub-1")
	assert400(http.MethodDelete, "/v1/devices/dev-1/subscriptions/sub-1/extra")
	assert400(http.MethodGet, "/v1/devices/dev-1/subscriptions/sub-1/extra")
	assert400(http.MethodDelete, "/v1/devices/dev-1/subscriptions/sub-1/")

	// Shape wins over device existence: the missing id segment and a method
	// mismatch against an unregistered device are still 400, not 404.
	assert400(http.MethodDelete, "/v1/devices/ghost/subscriptions")
	assert400(http.MethodPost, "/v1/devices/ghost/subscriptions")

	// A well-formed request against an unregistered device is 404, on both
	// entries.
	for _, tc := range []struct{ method, path string }{
		{http.MethodGet, "/v1/devices/ghost/subscriptions"},
		{http.MethodDelete, "/v1/devices/ghost/subscriptions/sub-1"},
	} {
		w := serveRecorder(h, newJSONRequest(tc.method, tc.path, "", ""))
		if w.Code != http.StatusNotFound {
			t.Fatalf("%s %s = %d, want 404, body = %s", tc.method, tc.path, w.Code, w.Body.String())
		}
		assertJSONError(t, w)
	}
}
