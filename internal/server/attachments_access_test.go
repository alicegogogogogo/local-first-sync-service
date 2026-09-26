package server

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/alicegogogogogo/local-first-sync-service/internal/app"
)

// grantAccess posts to the access endpoint and returns the recorder and
// decoded body.
func setAccess(t *testing.T, h http.Handler, callerDevice, attachmentID, targetDevice, action string) (*httptest.ResponseRecorder, map[string]any) {
	t.Helper()
	return postJSON(t, h, fmt.Sprintf("/v1/devices/%s/attachments/%s/access", callerDevice, attachmentID),
		map[string]any{"deviceId": targetDevice, "action": action})
}

func getAttachmentMeta(t *testing.T, h http.Handler, deviceID, attachmentID string) *httptest.ResponseRecorder {
	t.Helper()
	r := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/v1/devices/%s/attachments/%s", deviceID, attachmentID), nil)
	return serveRecorder(h, r)
}

func getChunk(t *testing.T, h http.Handler, deviceID, attachmentID string, index int64) *httptest.ResponseRecorder {
	t.Helper()
	r := httptest.NewRequest(http.MethodGet,
		fmt.Sprintf("/v1/devices/%s/attachments/%s/chunks/%d", deviceID, attachmentID, index), nil)
	return serveRecorder(h, r)
}

// The access lifecycle: a granted device reads exactly what the creator sees,
// a revoke returns it to 403 at once, and repeating an action is idempotent.
func TestAttachmentAccessGrantReadRevokeContract(t *testing.T) {
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

	// Before any grant every other device is unauthorized: 403 on both reads.
	if w := getAttachmentMeta(t, h, "dev-2", "att-1"); w.Code != http.StatusForbidden {
		t.Fatalf("meta before grant = %d, want 403", w.Code)
	}
	if w := getChunk(t, h, "dev-2", "att-1", 0); w.Code != http.StatusForbidden {
		t.Fatalf("chunk before grant = %d, want 403", w.Code)
	}

	// The first grant is the first write; repeating it is idempotent.
	w, body = setAccess(t, h, "dev-1", "att-1", "dev-2", "grant")
	if w.Code != http.StatusOK || body["deviceId"] != "dev-2" ||
		body["authorized"] != true || body["changed"] != true {
		t.Fatalf("grant = %d %v body=%s", w.Code, body, w.Body.String())
	}
	w, body = setAccess(t, h, "dev-1", "att-1", "dev-2", "grant")
	if w.Code != http.StatusOK || body["authorized"] != true || body["changed"] != false {
		t.Fatalf("grant retry = %d %v", w.Code, body)
	}

	// The granted device sees byte-for-byte what the creator sees.
	creatorMeta := getAttachmentMeta(t, h, "dev-1", "att-1")
	grantedMeta := getAttachmentMeta(t, h, "dev-2", "att-1")
	if grantedMeta.Code != http.StatusOK || grantedMeta.Body.String() != creatorMeta.Body.String() {
		t.Fatalf("granted meta = %d %q, creator %d %q",
			grantedMeta.Code, grantedMeta.Body.String(), creatorMeta.Code, creatorMeta.Body.String())
	}
	for i := int64(0); i < 3; i++ {
		creatorChunk := getChunk(t, h, "dev-1", "att-1", i)
		grantedChunk := getChunk(t, h, "dev-2", "att-1", i)
		if grantedChunk.Code != http.StatusOK || grantedChunk.Body.String() != creatorChunk.Body.String() {
			t.Fatalf("granted chunk %d = %d %q, creator %d %q",
				i, grantedChunk.Code, grantedChunk.Body.String(), creatorChunk.Code, creatorChunk.Body.String())
		}
	}

	// Revoke flips the state once; the granted device's reads are 403 again at
	// once, while the creator keeps reading the sealed content unchanged.
	w, body = setAccess(t, h, "dev-1", "att-1", "dev-2", "revoke")
	if w.Code != http.StatusOK || body["authorized"] != false || body["changed"] != true {
		t.Fatalf("revoke = %d %v", w.Code, body)
	}
	w, body = setAccess(t, h, "dev-1", "att-1", "dev-2", "revoke")
	if w.Code != http.StatusOK || body["changed"] != false {
		t.Fatalf("revoke retry = %d %v", w.Code, body)
	}
	if w := getAttachmentMeta(t, h, "dev-2", "att-1"); w.Code != http.StatusForbidden {
		t.Fatalf("meta after revoke = %d, want 403", w.Code)
	}
	if w := getChunk(t, h, "dev-2", "att-1", 0); w.Code != http.StatusForbidden {
		t.Fatalf("chunk after revoke = %d, want 403", w.Code)
	}
	if w := getChunk(t, h, "dev-1", "att-1", 0); w.Code != http.StatusOK || w.Body.String() != "hell" {
		t.Fatalf("creator chunk after revoke = %d %q", w.Code, w.Body.String())
	}

	// The revoke deleted nothing: the sealed content is still reused by digest.
	mustCreateAttachment(t, h, "dev-2", "att-2", content, 4)
	putChunk(t, h, "dev-2", "att-2", 0, []byte("hell"))
	putChunk(t, h, "dev-2", "att-2", 1, []byte("o wo"))
	putChunk(t, h, "dev-2", "att-2", 2, []byte("rld"))
	w, body = completeAttachment(t, h, "dev-2", "att-2")
	if w.Code != http.StatusOK || body["reused"] != true {
		t.Fatalf("dedup after revoke = %d %v", w.Code, body)
	}
}

// Only the creator may change access; unknown attachments and unregistered
// targets are 404, malformed requests 400 — all zero-write.
func TestAttachmentAccessRejectsNonCreatorAndBadInput(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	registerDevice(t, h, "dev-2")
	content := []byte("hello world")
	mustCreateAttachment(t, h, "dev-1", "att-1", content, 4)
	putChunk(t, h, "dev-1", "att-1", 0, []byte("hell"))

	// A non-creator caller gets a 403 and changes nothing.
	w, _ := setAccess(t, h, "dev-2", "att-1", "dev-2", "grant")
	if w.Code != http.StatusForbidden {
		t.Fatalf("non-creator access change = %d, want 403", w.Code)
	}
	assertJSONError(t, w)

	// An unknown attachment is a 404 for any caller.
	w, _ = setAccess(t, h, "dev-1", "nope", "dev-2", "grant")
	if w.Code != http.StatusNotFound {
		t.Fatalf("unknown attachment = %d, want 404", w.Code)
	}
	assertJSONError(t, w)

	// An unregistered target device is a 404.
	w, _ = setAccess(t, h, "dev-1", "att-1", "ghost", "grant")
	if w.Code != http.StatusNotFound {
		t.Fatalf("unregistered target = %d, want 404", w.Code)
	}
	assertJSONError(t, w)

	// Malformed requests are a 400 JSON error.
	const url = "/v1/devices/dev-1/attachments/att-1/access"
	cases := []struct {
		name        string
		contentType string
		body        string
	}{
		{"wrong content type", "text/plain", `{"deviceId":"dev-2","action":"grant"}`},
		{"missing content type", "", `{"deviceId":"dev-2","action":"grant"}`},
		{"malformed json", "application/json", `{`},
		{"trailing content", "application/json", `{"deviceId":"dev-2","action":"grant"}x`},
		{"empty deviceId", "application/json", `{"deviceId":"","action":"grant"}`},
		{"missing deviceId", "application/json", `{"action":"grant"}`},
		{"numeric deviceId", "application/json", `{"deviceId":7,"action":"grant"}`},
		{"null deviceId", "application/json", `{"deviceId":null,"action":"grant"}`},
		{"missing action", "application/json", `{"deviceId":"dev-2"}`},
		{"empty action", "application/json", `{"deviceId":"dev-2","action":""}`},
		{"unknown action", "application/json", `{"deviceId":"dev-2","action":"allow"}`},
		{"numeric action", "application/json", `{"deviceId":"dev-2","action":1}`},
		{"array body", "application/json", `[1,2]`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := newJSONRequest(http.MethodPost, url, tc.body, tc.contentType)
			w := serveRecorder(h, r)
			if w.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400, body = %s", w.Code, w.Body.String())
			}
			assertJSONError(t, w)
		})
	}

	// Zero writes everywhere: dev-2 is still unauthorized, so the first real
	// grant must report changed=true.
	if w := getAttachmentMeta(t, h, "dev-2", "att-1"); w.Code != http.StatusForbidden {
		t.Fatalf("zero-write violated: meta = %d, want 403", w.Code)
	}
	w, body := setAccess(t, h, "dev-1", "att-1", "dev-2", "grant")
	if w.Code != http.StatusOK || body["changed"] != true {
		t.Fatalf("grant after rejects = %d %v", w.Code, body)
	}
}

// Access only widens reads: a granted device still cannot write chunks or
// finish the upload, and its failed writes store nothing.
func TestAttachmentAccessGrantedDeviceCannotWrite(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	registerDevice(t, h, "dev-2")
	content := []byte("hello world")
	mustCreateAttachment(t, h, "dev-1", "att-1", content, 4)
	putChunk(t, h, "dev-1", "att-1", 0, []byte("hell"))

	w, body := setAccess(t, h, "dev-1", "att-1", "dev-2", "grant")
	if w.Code != http.StatusOK || body["changed"] != true {
		t.Fatalf("grant = %d %v", w.Code, body)
	}

	// The granted device reads but every write is a 403 that stores nothing.
	if w := getAttachmentMeta(t, h, "dev-2", "att-1"); w.Code != http.StatusOK {
		t.Fatalf("granted meta = %d, want 200", w.Code)
	}
	w, _ = putChunk(t, h, "dev-2", "att-1", 1, []byte("o wo"))
	if w.Code != http.StatusForbidden {
		t.Fatalf("granted chunk write = %d, want 403", w.Code)
	}
	assertJSONError(t, w)
	w, _ = completeAttachment(t, h, "dev-2", "att-1")
	if w.Code != http.StatusForbidden {
		t.Fatalf("granted complete = %d, want 403", w.Code)
	}
	assertJSONError(t, w)

	// Nothing was written: only chunk 0 was ever received.
	w = getAttachmentMeta(t, h, "dev-1", "att-1")
	if !strings.Contains(w.Body.String(), `"receivedChunks":[0]`) ||
		!strings.Contains(w.Body.String(), `"complete":false`) {
		t.Fatalf("write leaked: %s", w.Body.String())
	}
}

// A granted device's read failures match the creator's: a chunk that never
// arrived is a 404 and an illegal or out-of-range index is a 400.
func TestAttachmentAccessGrantedReadFailuresMatchCreator(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	registerDevice(t, h, "dev-2")
	content := []byte("hello world")
	mustCreateAttachment(t, h, "dev-1", "att-1", content, 4)
	putChunk(t, h, "dev-1", "att-1", 0, []byte("hell"))
	setAccess(t, h, "dev-1", "att-1", "dev-2", "grant")

	// In-range but not yet arrived: 404 for the granted device, as for the
	// creator.
	if w := getChunk(t, h, "dev-2", "att-1", 1); w.Code != http.StatusNotFound {
		t.Fatalf("missing chunk = %d, want 404", w.Code)
	}
	// Out of the declared range: 400.
	if w := getChunk(t, h, "dev-2", "att-1", 3); w.Code != http.StatusBadRequest {
		t.Fatalf("out-of-range chunk = %d, want 400", w.Code)
	}
	// Illegal index shapes: 400.
	for _, index := range []string{"-1", "x", "1.5"} {
		r := httptest.NewRequest(http.MethodGet, "/v1/devices/dev-2/attachments/att-1/chunks/"+index, nil)
		if w := serveRecorder(h, r); w.Code != http.StatusBadRequest {
			t.Fatalf("index %q = %d, want 400", index, w.Code)
		}
	}
}

// The access state and its idempotency judgments survive a restart.
func TestAttachmentAccessSurvivesRestart(t *testing.T) {
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
	putChunk(t, h, "dev-1", "att-1", 1, []byte("o wo"))
	putChunk(t, h, "dev-1", "att-1", 2, []byte("rld"))
	completeAttachment(t, h, "dev-1", "att-1")
	w, body := setAccess(t, h, "dev-1", "att-1", "dev-2", "grant")
	if w.Code != http.StatusOK || body["changed"] != true {
		t.Fatalf("grant = %d %v", w.Code, body)
	}
	_ = s.Close()

	h, s = open(t)
	defer func() { _ = s.Close() }()

	// The grant is still in effect: reads are 200 and the repeated grant is
	// still idempotent.
	if w := getChunk(t, h, "dev-2", "att-1", 0); w.Code != http.StatusOK || w.Body.String() != "hell" {
		t.Fatalf("granted chunk after restart = %d %q", w.Code, w.Body.String())
	}
	w, body = setAccess(t, h, "dev-1", "att-1", "dev-2", "grant")
	if w.Code != http.StatusOK || body["changed"] != false {
		t.Fatalf("grant retry after restart = %d %v", w.Code, body)
	}
	// The revoke commits and its own repeat is idempotent.
	w, body = setAccess(t, h, "dev-1", "att-1", "dev-2", "revoke")
	if w.Code != http.StatusOK || body["changed"] != true {
		t.Fatalf("revoke after restart = %d %v", w.Code, body)
	}
	_ = s.Close()

	h, s = open(t)
	defer func() { _ = s.Close() }()
	if w := getChunk(t, h, "dev-2", "att-1", 0); w.Code != http.StatusForbidden {
		t.Fatalf("revoked chunk after restart = %d, want 403", w.Code)
	}
	w, body = setAccess(t, h, "dev-1", "att-1", "dev-2", "revoke")
	if w.Code != http.StatusOK || body["changed"] != false {
		t.Fatalf("revoke retry after restart = %d %v", w.Code, body)
	}
}

// Concurrent grant/revoke calls on one attachment each commit completely:
// every call answers 200 and the final state is one of the two committed
// states, never a torn one.
func TestAttachmentAccessConcurrentGrantRevoke(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	registerDevice(t, h, "dev-2")
	content := []byte("data")
	mustCreateAttachment(t, h, "dev-1", "att-1", content, 2)

	const n = 40
	var wg sync.WaitGroup
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			action := "grant"
			if i%2 == 1 {
				action = "revoke"
			}
			w, _ := setAccess(t, h, "dev-1", "att-1", "dev-2", action)
			if w.Code != http.StatusOK {
				errs <- fmt.Errorf("%s %d status %d: %s", action, i, w.Code, w.Body.String())
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}

	// A final grant settles the state deterministically; the read must agree.
	w, _ := setAccess(t, h, "dev-1", "att-1", "dev-2", "grant")
	if w.Code != http.StatusOK {
		t.Fatalf("final grant = %d", w.Code)
	}
	if w := getAttachmentMeta(t, h, "dev-2", "att-1"); w.Code != http.StatusOK {
		t.Fatalf("meta after final grant = %d, want 200", w.Code)
	}
}

// Attachment access and document permissions are separate ledgers: neither
// one's grant or revoke changes the other's verdicts.
func TestAttachmentAccessIndependentOfDocumentPermissions(t *testing.T) {
	h, s := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	registerDevice(t, h, "dev-2")
	content := []byte("data")
	mustCreateAttachment(t, h, "dev-1", "att-1", content, 2)
	putChunk(t, h, "dev-1", "att-1", 0, []byte("da"))
	putChunk(t, h, "dev-1", "att-1", 1, []byte("ta"))

	// Granting attachment access leaves the document permission ledger at its
	// default, and revoking the document permission does not touch the
	// attachment grant.
	setAccess(t, h, "dev-1", "att-1", "dev-2", "grant")
	if ok, err := s.DocumentAuthorized("doc-1", "dev-2"); err != nil || !ok {
		t.Fatalf("document permission moved by attachment grant: %v %v", ok, err)
	}
	w, body := postJSON(t, h, "/v1/documents/doc-1/permissions",
		map[string]any{"deviceId": "dev-2", "action": "revoke"})
	if w.Code != http.StatusOK || body["changed"] != true {
		t.Fatalf("document revoke = %d %v", w.Code, body)
	}
	if w := getAttachmentMeta(t, h, "dev-2", "att-1"); w.Code != http.StatusOK {
		t.Fatalf("attachment read after document revoke = %d, want 200", w.Code)
	}

	// Revoking attachment access leaves the document permission revoked.
	setAccess(t, h, "dev-1", "att-1", "dev-2", "revoke")
	if ok, err := s.DocumentAuthorized("doc-1", "dev-2"); err != nil || ok {
		t.Fatalf("document permission moved by attachment revoke: %v %v", ok, err)
	}
}
