package server

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/alicegogogogogo/local-first-sync-service/internal/app"
)

// setAttachmentAccess posts a grant/revoke to the access sub-resource.
func setAttachmentAccess(t *testing.T, h http.Handler, callerID, attachmentID, targetID, action string) (*httptest.ResponseRecorder, map[string]any) {
	t.Helper()
	return postJSON(t, h, "/v1/devices/"+callerID+"/attachments/"+attachmentID+"/access", map[string]any{
		"deviceId": targetID,
		"action":   action,
	})
}

// getAttachment reads the metadata view as the given device.
func getAttachment(t *testing.T, h http.Handler, deviceID, attachmentID string) *httptest.ResponseRecorder {
	t.Helper()
	r := httptest.NewRequest(http.MethodGet, "/v1/devices/"+deviceID+"/attachments/"+attachmentID, nil)
	return serveRecorder(h, r)
}

// getChunk reads one chunk as the given device.
func getChunk(t *testing.T, h http.Handler, deviceID, attachmentID string, index int64) *httptest.ResponseRecorder {
	t.Helper()
	r := httptest.NewRequest(http.MethodGet,
		fmt.Sprintf("/v1/devices/%s/attachments/%s/chunks/%d", deviceID, attachmentID, index), nil)
	return serveRecorder(h, r)
}

// The access lifecycle over the public surface: the default boundary is 403,
// a grant opens byte-identical reads, repeats are idempotent, a revoke closes
// the reads again at once, and the grant never widens the write side.
func TestAttachmentAccessLifecycleContract(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	registerDevice(t, h, "dev-2")
	content := []byte("hello world")
	mustCreateAttachment(t, h, "dev-1", "att-1", content, 4)
	putChunk(t, h, "dev-1", "att-1", 0, []byte("hell"))
	putChunk(t, h, "dev-1", "att-1", 2, []byte("rld"))

	// Before any grant the non-creator reads are 403.
	if w := getAttachment(t, h, "dev-2", "att-1"); w.Code != http.StatusForbidden {
		t.Fatalf("default meta read = %d, want 403", w.Code)
	}
	if w := getChunk(t, h, "dev-2", "att-1", 0); w.Code != http.StatusForbidden {
		t.Fatalf("default chunk read = %d, want 403", w.Code)
	}

	// The first grant reports changed=true; the repeat is idempotent.
	w, body := setAttachmentAccess(t, h, "dev-1", "att-1", "dev-2", "grant")
	if w.Code != http.StatusOK || body["deviceId"] != "dev-2" || body["authorized"] != true || body["changed"] != true {
		t.Fatalf("grant = %d %v body=%s", w.Code, body, w.Body.String())
	}
	w, body = setAttachmentAccess(t, h, "dev-1", "att-1", "dev-2", "grant")
	if w.Code != http.StatusOK || body["authorized"] != true || body["changed"] != false {
		t.Fatalf("repeat grant = %d %v body=%s", w.Code, body, w.Body.String())
	}

	// The granted device sees exactly what the creator sees, byte for byte.
	creatorMeta := getAttachment(t, h, "dev-1", "att-1")
	grantedMeta := getAttachment(t, h, "dev-2", "att-1")
	if grantedMeta.Code != http.StatusOK || grantedMeta.Body.String() != creatorMeta.Body.String() {
		t.Fatalf("granted meta = %d %s, creator sees %s", grantedMeta.Code, grantedMeta.Body.String(), creatorMeta.Body.String())
	}
	creatorChunk := getChunk(t, h, "dev-1", "att-1", 0)
	grantedChunk := getChunk(t, h, "dev-2", "att-1", 0)
	if grantedChunk.Code != http.StatusOK || grantedChunk.Body.String() != creatorChunk.Body.String() ||
		grantedChunk.Header().Get("Content-Type") != "application/octet-stream" {
		t.Fatalf("granted chunk = %d %q", grantedChunk.Code, grantedChunk.Body.String())
	}

	// A chunk that never arrived is a 404 and an out-of-range index a 400,
	// exactly as the creator sees them.
	if w := getChunk(t, h, "dev-2", "att-1", 1); w.Code != http.StatusNotFound {
		t.Fatalf("granted missing chunk = %d, want 404", w.Code)
	}
	assertJSONError(t, getChunk(t, h, "dev-2", "att-1", 1))
	if w := getChunk(t, h, "dev-2", "att-1", 3); w.Code != http.StatusBadRequest {
		t.Fatalf("granted out-of-range chunk = %d, want 400", w.Code)
	}
	assertJSONError(t, getChunk(t, h, "dev-2", "att-1", 3))

	// The grant is read-only: chunk writes and the finish stay 403 and store
	// nothing.
	w, _ = putChunk(t, h, "dev-2", "att-1", 1, []byte("o wo"))
	if w.Code != http.StatusForbidden {
		t.Fatalf("granted chunk write = %d, want 403", w.Code)
	}
	w, _ = completeAttachment(t, h, "dev-2", "att-1")
	if w.Code != http.StatusForbidden {
		t.Fatalf("granted complete = %d, want 403", w.Code)
	}
	if w := getAttachment(t, h, "dev-1", "att-1"); !strings.Contains(w.Body.String(), `"receivedChunks":[0,2]`) {
		t.Fatalf("rejected writes stored something: %s", w.Body.String())
	}

	// Revoke reports changed=true once, then is idempotent; reads return to
	// 403 immediately.
	w, body = setAttachmentAccess(t, h, "dev-1", "att-1", "dev-2", "revoke")
	if w.Code != http.StatusOK || body["authorized"] != false || body["changed"] != true {
		t.Fatalf("revoke = %d %v body=%s", w.Code, body, w.Body.String())
	}
	w, body = setAttachmentAccess(t, h, "dev-1", "att-1", "dev-2", "revoke")
	if w.Code != http.StatusOK || body["authorized"] != false || body["changed"] != false {
		t.Fatalf("repeat revoke = %d %v body=%s", w.Code, body, w.Body.String())
	}
	if w := getAttachment(t, h, "dev-2", "att-1"); w.Code != http.StatusForbidden {
		t.Fatalf("meta read after revoke = %d, want 403", w.Code)
	}
	if w := getChunk(t, h, "dev-2", "att-1", 0); w.Code != http.StatusForbidden {
		t.Fatalf("chunk read after revoke = %d, want 403", w.Code)
	}
}

// The access entry's failure matrix: non-creator callers are 403, unknown
// attachments and unregistered targets are 404, malformed requests are 400 —
// and none of them changes any grant.
func TestAttachmentAccessFailureContract(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	registerDevice(t, h, "dev-2")
	registerDevice(t, h, "dev-3")
	mustCreateAttachment(t, h, "dev-1", "att-1", []byte("hello world"), 4)

	// A non-creator may not change access, and the failed call changes
	// nothing: dev-3 stays unauthorized afterwards.
	w, _ := setAttachmentAccess(t, h, "dev-2", "att-1", "dev-3", "grant")
	if w.Code != http.StatusForbidden {
		t.Fatalf("non-creator access call = %d, want 403", w.Code)
	}
	assertJSONError(t, w)
	if w := getAttachment(t, h, "dev-3", "att-1"); w.Code != http.StatusForbidden {
		t.Fatalf("read after failed grant = %d, want 403", w.Code)
	}

	// Unknown attachment: 404, no state change.
	w, _ = setAttachmentAccess(t, h, "dev-1", "nope", "dev-2", "grant")
	if w.Code != http.StatusNotFound {
		t.Fatalf("unknown attachment = %d, want 404", w.Code)
	}
	assertJSONError(t, w)
	if w := getAttachment(t, h, "dev-2", "att-1"); w.Code != http.StatusForbidden {
		t.Fatalf("read after failed unknown-attachment grant = %d, want 403", w.Code)
	}

	// Unregistered target device: 404.
	w, _ = setAttachmentAccess(t, h, "dev-1", "att-1", "ghost", "grant")
	if w.Code != http.StatusNotFound {
		t.Fatalf("unknown target = %d, want 404", w.Code)
	}
	assertJSONError(t, w)

	// Malformed requests: 400 JSON, zero writes.
	accessURL := "/v1/devices/dev-1/attachments/att-1/access"
	for _, tc := range []struct {
		name        string
		contentType string
		body        string
	}{
		{"wrong content type", "text/plain", `{"deviceId":"dev-2","action":"grant"}`},
		{"malformed json", "application/json", `{`},
		{"trailing content", "application/json", `{"deviceId":"dev-2","action":"grant"}x`},
		{"missing deviceId", "application/json", `{"action":"grant"}`},
		{"empty deviceId", "application/json", `{"deviceId":"","action":"grant"}`},
		{"numeric deviceId", "application/json", `{"deviceId":1,"action":"grant"}`},
		{"missing action", "application/json", `{"deviceId":"dev-2"}`},
		{"unknown action", "application/json", `{"deviceId":"dev-2","action":"allow"}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := newJSONRequest(http.MethodPost, accessURL, tc.body, tc.contentType)
			w := serveRecorder(h, r)
			if w.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400 body=%s", w.Code, w.Body.String())
			}
			assertJSONError(t, w)
		})
	}

	// Zero writes: dev-2 is still unauthorized after every rejection.
	if w := getAttachment(t, h, "dev-2", "att-1"); w.Code != http.StatusForbidden {
		t.Fatalf("read after rejected requests = %d, want 403", w.Code)
	}
}

// A grant on a sealed attachment opens reads of the finished content; the
// revoke deletes nothing — the creator still reads the same bytes and the
// digest-reuse judgment is untouched.
func TestAttachmentAccessOnSealedContent(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	registerDevice(t, h, "dev-2")
	content := []byte("hello world")
	mustCreateAttachment(t, h, "dev-1", "att-1", content, 4)
	putChunk(t, h, "dev-1", "att-1", 0, []byte("hell"))
	putChunk(t, h, "dev-1", "att-1", 1, []byte("o wo"))
	putChunk(t, h, "dev-1", "att-1", 2, []byte("rld"))
	w, body := completeAttachment(t, h, "dev-1", "att-1")
	if w.Code != http.StatusOK || body["complete"] != true {
		t.Fatalf("complete = %d %v", w.Code, body)
	}

	w, body = setAttachmentAccess(t, h, "dev-1", "att-1", "dev-2", "grant")
	if w.Code != http.StatusOK || body["changed"] != true {
		t.Fatalf("grant = %d %v", w.Code, body)
	}
	// The granted device reassembles the sealed content byte for byte.
	var got strings.Builder
	for i := int64(0); i < 3; i++ {
		w := getChunk(t, h, "dev-2", "att-1", i)
		if w.Code != http.StatusOK {
			t.Fatalf("sealed chunk %d = %d, want 200", i, w.Code)
		}
		got.WriteString(w.Body.String())
	}
	if got.String() != string(content) {
		t.Fatalf("granted reassembly = %q, want %q", got.String(), content)
	}

	// Revoke: the reads close, the sealed content stays.
	w, _ = setAttachmentAccess(t, h, "dev-1", "att-1", "dev-2", "revoke")
	if w.Code != http.StatusOK {
		t.Fatalf("revoke = %d", w.Code)
	}
	if w := getChunk(t, h, "dev-2", "att-1", 0); w.Code != http.StatusForbidden {
		t.Fatalf("sealed read after revoke = %d, want 403", w.Code)
	}
	if w := getChunk(t, h, "dev-1", "att-1", 0); w.Code != http.StatusOK || w.Body.String() != "hell" {
		t.Fatalf("creator read after revoke = %d %q", w.Code, w.Body.String())
	}

	// The reuse judgment is unaffected: a second identical upload still
	// reuses the stored content.
	mustCreateAttachment(t, h, "dev-2", "att-2", content, 4)
	putChunk(t, h, "dev-2", "att-2", 0, []byte("hell"))
	putChunk(t, h, "dev-2", "att-2", 1, []byte("o wo"))
	putChunk(t, h, "dev-2", "att-2", 2, []byte("rld"))
	w, body = completeAttachment(t, h, "dev-2", "att-2")
	if w.Code != http.StatusOK || body["reused"] != true {
		t.Fatalf("reuse after access churn = %d %v, want reused=true", w.Code, body)
	}
}

// The grant state and its idempotency judgments survive a restart; so does
// the read boundary in both directions.
func TestAttachmentAccessSurvivesRestartContract(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "sync.db")
	open := func(t *testing.T) (http.Handler, *app.App) {
		t.Helper()
		s, err := app.Open(dbPath)
		if err != nil {
			t.Fatal(err)
		}
		return NewHandler(s), s
	}

	h, s := open(t)
	registerDevice(t, h, "dev-1")
	registerDevice(t, h, "dev-2")
	content := []byte("hello world")
	mustCreateAttachment(t, h, "dev-1", "att-1", content, 4)
	putChunk(t, h, "dev-1", "att-1", 0, []byte("hell"))
	w, body := setAttachmentAccess(t, h, "dev-1", "att-1", "dev-2", "grant")
	if w.Code != http.StatusOK || body["changed"] != true {
		t.Fatalf("grant = %d %v", w.Code, body)
	}
	_ = s.Close()

	h, s = open(t)
	// The grant holds after the restart; re-granting is idempotent.
	if w := getChunk(t, h, "dev-2", "att-1", 0); w.Code != http.StatusOK || w.Body.String() != "hell" {
		t.Fatalf("granted read after restart = %d %q", w.Code, w.Body.String())
	}
	w, body = setAttachmentAccess(t, h, "dev-1", "att-1", "dev-2", "grant")
	if w.Code != http.StatusOK || body["changed"] != false {
		t.Fatalf("re-grant after restart = %d %v, want changed=false", w.Code, body)
	}
	// Revoke persists too.
	w, body = setAttachmentAccess(t, h, "dev-1", "att-1", "dev-2", "revoke")
	if w.Code != http.StatusOK || body["changed"] != true {
		t.Fatalf("revoke = %d %v", w.Code, body)
	}
	_ = s.Close()

	h, s = open(t)
	defer func() { _ = s.Close() }()
	if w := getChunk(t, h, "dev-2", "att-1", 0); w.Code != http.StatusForbidden {
		t.Fatalf("read after restart = %d, want 403", w.Code)
	}
	w, body = setAttachmentAccess(t, h, "dev-1", "att-1", "dev-2", "revoke")
	if w.Code != http.StatusOK || body["changed"] != false {
		t.Fatalf("re-revoke after restart = %d %v, want changed=false", w.Code, body)
	}
}

// The attachment access ledger and the document permission ledger are
// independent: revoking one never changes the other's verdict.
func TestAttachmentAccessIndependentOfDocumentPermissions(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	registerDevice(t, h, "dev-2")
	content := []byte("hello world")
	mustCreateAttachment(t, h, "dev-1", "att-1", content, 4)
	putChunk(t, h, "dev-1", "att-1", 0, []byte("hell"))

	w, _ := setAttachmentAccess(t, h, "dev-1", "att-1", "dev-2", "grant")
	if w.Code != http.StatusOK {
		t.Fatalf("grant = %d", w.Code)
	}

	// Revoking the document permission leaves the attachment grant intact.
	w, _ = postJSON(t, h, "/v1/documents/doc-1/permissions", map[string]any{
		"deviceId": "dev-2", "action": "revoke",
	})
	if w.Code != http.StatusOK {
		t.Fatalf("document revoke = %d", w.Code)
	}
	if w := getChunk(t, h, "dev-2", "att-1", 0); w.Code != http.StatusOK {
		t.Fatalf("attachment read after document revoke = %d, want 200", w.Code)
	}

	// And revoking the attachment access leaves the document verdict alone:
	// the session-scoped read still answers 403 from the document ledger, not
	// from anything the attachment entry did.
	w, _ = setAttachmentAccess(t, h, "dev-1", "att-1", "dev-2", "revoke")
	if w.Code != http.StatusOK {
		t.Fatalf("attachment revoke = %d", w.Code)
	}
	w, body := postJSON(t, h, "/v1/documents/doc-1/permissions", map[string]any{
		"deviceId": "dev-2", "action": "revoke",
	})
	if w.Code != http.StatusOK || body["changed"] != false {
		t.Fatalf("document re-revoke = %d %v, want changed=false (untouched by attachment revoke)", w.Code, body)
	}
}
