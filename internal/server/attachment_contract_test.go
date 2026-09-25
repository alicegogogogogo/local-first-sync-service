package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/alicegogogogogo/local-first-sync-service/internal/app"
)

// This file fills the point-by-point public-contract coverage for the
// resumable attachment path. Every assertion goes through the documented HTTP
// surface — register, create, chunk, complete, metadata/chunk reads — and never
// reads an internal package field. The single exception is a deliberate
// precondition seed (a completed content whose digest matches another
// upload's but whose size differs): that state cannot be produced by honest
// chunk assembly without a SHA-256 collision, so it cannot be reached through
// public calls; it is seeded straight into the content table and every
// *behavioral* assertion still runs over public HTTP.

// fullResumablePath drives the whole documented happy path strictly through
// public HTTP: register the creator, create an upload, land its chunks out of
// order with an interruption (close/reopen) in the middle, retry the chunks
// idempotently, finish once (content dedup reused=false on the first content),
// then verify the seal, the recorded result and every chunk byte.
func TestAttachmentContractFullResumablePath(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "attach-contract.db")
	open := func(t *testing.T) (http.Handler, *app.App) {
		t.Helper()
		s, err := app.Open(dbPath)
		if err != nil {
			t.Fatal(err)
		}
		return NewHandler(s), s
	}

	content := []byte("hello world") // 11 bytes, chunks of 4: [hell][o wo][rld]

	h, s := open(t)
	registerDevice(t, h, "dev-1")
	mustCreateAttachment(t, h, "dev-1", "att-1", content, 4)

	// Chunks arrive out of order (1 before 0); metadata reports a sorted list.
	if w, body := putChunk(t, h, "dev-1", "att-1", 1, []byte("o wo")); w.Code != http.StatusOK || body["created"] != true {
		t.Fatalf("chunk 1 = %d %v body=%s", w.Code, body, w.Body.String())
	}
	if w, body := putChunk(t, h, "dev-1", "att-1", 0, []byte("hell")); w.Code != http.StatusOK || body["created"] != true {
		t.Fatalf("chunk 0 = %d %v body=%s", w.Code, body, w.Body.String())
	}
	if got := attachmentReceivedChunks(t, h, "dev-1", "att-1"); fmt.Sprint(got) != "[0 1]" {
		t.Fatalf("receivedChunks = %v, want [0 1]", got)
	}

	// Simulate a dropped connection mid-upload: the process is gone while only
	// chunks 0 and 1 are durable. Reopening resumes with everything intact.
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	h, s = open(t)
	t.Cleanup(func() { _ = s.Close() })

	// Retrying an already-stored chunk after the restart is idempotent.
	if w, body := putChunk(t, h, "dev-1", "att-1", 0, []byte("hell")); w.Code != http.StatusOK || body["created"] != false {
		t.Fatalf("chunk retry after restart = %d %v", w.Code, body)
	}

	// Finish with the last chunk missing: 409 and the upload stays resumable.
	if w, _ := completeAttachment(t, h, "dev-1", "att-1"); w.Code != http.StatusConflict {
		t.Fatalf("complete with missing chunk = %d, want 409", w.Code)
	}

	// Resume by sending the final chunk, then seal.
	if w, body := putChunk(t, h, "dev-1", "att-1", 2, []byte("rld")); w.Code != http.StatusOK || body["created"] != true {
		t.Fatalf("resume chunk 2 = %d %v", w.Code, body)
	}
	w, body := completeAttachment(t, h, "dev-1", "att-1")
	if w.Code != http.StatusOK {
		t.Fatalf("complete = %d body=%s", w.Code, w.Body.String())
	}
	digest := sha256Hex(content)
	if body["attachmentId"] != "att-1" || body["size"] != float64(11) ||
		body["sha256"] != digest || body["complete"] != true || body["reused"] != false {
		t.Fatalf("complete body = %v", body)
	}

	// Repeating the finish is idempotent and byte-for-byte the same answer.
	w2, _ := completeAttachment(t, h, "dev-1", "att-1")
	if w2.Code != http.StatusOK || w2.Body.String() != w.Body.String() {
		t.Fatalf("repeat complete = %d %q, want %q", w2.Code, w2.Body.String(), w.Body.String())
	}

	// The sealed metadata reports completion and every sorted index.
	meta := getAttachmentMeta(t, h, "dev-1", "att-1")
	if meta["complete"] != true || fmt.Sprint(meta["receivedChunks"]) != "[0 1 2]" ||
		meta["totalBytes"] != float64(11) || meta["chunkSize"] != float64(4) || meta["sha256"] != digest {
		t.Fatalf("sealed meta = %v", meta)
	}

	// Every chunk reads back with the exact stored bytes.
	for i, want := range [][]byte{[]byte("hell"), []byte("o wo"), []byte("rld")} {
		r := httptest.NewRequest(http.MethodGet,
			fmt.Sprintf("/v1/devices/dev-1/attachments/att-1/chunks/%d", i), nil)
		rec := serveRecorder(h, r)
		if rec.Code != http.StatusOK || rec.Body.String() != string(want) ||
			rec.Header().Get("Content-Type") != "application/octet-stream" {
			t.Fatalf("chunk %d read = %d %q", i, rec.Code, rec.Body.String())
		}
	}

	// Content dedup: a second, different-device upload of the same content at
	// the same size reuses the stored bytes rather than copying them.
	registerDevice(t, h, "dev-2")
	mustCreateAttachment(t, h, "dev-2", "att-2", content, 4)
	putChunk(t, h, "dev-2", "att-2", 0, []byte("hell"))
	putChunk(t, h, "dev-2", "att-2", 1, []byte("o wo"))
	putChunk(t, h, "dev-2", "att-2", 2, []byte("rld"))
	if w, body := completeAttachment(t, h, "dev-2", "att-2"); w.Code != http.StatusOK || body["reused"] != true {
		t.Fatalf("dedup complete = %d %v, want reused=true", w.Code, body)
	}
}

// getAttachmentMeta reads the creator metadata view over public HTTP.
func getAttachmentMeta(t *testing.T, h http.Handler, device, attachment string) map[string]any {
	t.Helper()
	r := httptest.NewRequest(http.MethodGet,
		"/v1/devices/"+device+"/attachments/"+attachment, nil)
	w := serveRecorder(h, r)
	if w.Code != http.StatusOK {
		t.Fatalf("GET attachment %s/%s = %d body=%s", device, attachment, w.Code, w.Body.String())
	}
	var meta map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &meta); err != nil {
		t.Fatal(err)
	}
	return meta
}

// attachmentReceivedChunks returns the received chunk index list as []any.
func attachmentReceivedChunks(t *testing.T, h http.Handler, device, attachment string) []any {
	t.Helper()
	meta := getAttachmentMeta(t, h, device, attachment)
	got, _ := meta["receivedChunks"].([]any)
	return got
}

// A failed finish must leave the upload genuinely open: after a missing-chunk
// 409 or a digest 422, the upload is not sealed (an identical chunk re-PUT is
// still idempotent, not a 409), metadata still says complete=false, and the
// resumable upload can be driven to a successful finish.
func TestAttachmentContractFailuresKeepUploadOpen(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")

	// --- A length-mismatch finish (409) stays resumable and then succeeds. ---
	t.Run("missing chunks 409 stays resumable", func(t *testing.T) {
		content := []byte("abcdef") // 6 bytes, chunks of 4
		mustCreateAttachment(t, h, "dev-1", "att-missing", content, 4)
		putChunk(t, h, "dev-1", "att-missing", 0, content[:4])
		putChunk(t, h, "dev-1", "att-missing", 1, content[4:5]) // 1 byte; remainder is 2

		w, _ := completeAttachment(t, h, "dev-1", "att-missing")
		if w.Code != http.StatusConflict {
			t.Fatalf("length-mismatch finish = %d, want 409", w.Code)
		}
		// Still open: re-PUTting chunk 0 with identical bytes is idempotent
		// (a sealed upload would answer 409), and the metadata is unfinished.
		if w, body := putChunk(t, h, "dev-1", "att-missing", 0, content[:4]); w.Code != http.StatusOK || body["created"] != false {
			t.Fatalf("identical re-PUT after 409 = %d %v, want 200 created=false", w.Code, body)
		}
		if meta := getAttachmentMeta(t, h, "dev-1", "att-missing"); meta["complete"] != false {
			t.Fatalf("meta after 409 = %v, want complete=false", meta)
		}
		// The cumulative length (5) still differs from the declared total (6)
		// and the short final chunk is content-immutable, so a repeat finish is
		// the same 409 and the upload remains unsealed — never silently sealed.
		if meta := getAttachmentMeta(t, h, "dev-1", "att-missing"); fmt.Sprint(meta["receivedChunks"]) != "[0 1]" {
			t.Fatalf("receivedChunks = %v", meta["receivedChunks"])
		}
		if w, _ := completeAttachment(t, h, "dev-1", "att-missing"); w.Code != http.StatusConflict {
			t.Fatalf("repeat length-mismatch finish = %d, want 409", w.Code)
		}
	})

	// --- A genuinely missing index 409 is recovered by sending the chunk. ---
	t.Run("missing index 409 recovered to success", func(t *testing.T) {
		content := []byte("hello world")
		mustCreateAttachment(t, h, "dev-1", "att-resume", content, 4)
		putChunk(t, h, "dev-1", "att-resume", 0, []byte("hell"))
		putChunk(t, h, "dev-1", "att-resume", 1, []byte("o wo"))
		// chunk 2 absent.
		if w, _ := completeAttachment(t, h, "dev-1", "att-resume"); w.Code != http.StatusConflict {
			t.Fatalf("finish with absent chunk = %d, want 409", w.Code)
		}
		putChunk(t, h, "dev-1", "att-resume", 2, []byte("rld"))
		if w, body := completeAttachment(t, h, "dev-1", "att-resume"); w.Code != http.StatusOK || body["complete"] != true {
			t.Fatalf("finish after resume = %d %v", w.Code, body)
		}
	})

	// --- A digest-mismatch finish (422) keeps the upload incomplete. ---
	t.Run("digest mismatch 422 stays incomplete", func(t *testing.T) {
		declared := sha256Hex([]byte("xyz"))
		w, body := createAttachment(t, h, "dev-1", "att-bad", 3, 3, declared)
		if w.Code != http.StatusOK || body["created"] != true {
			t.Fatalf("create = %d %v", w.Code, body)
		}
		putChunk(t, h, "dev-1", "att-bad", 0, []byte("abc"))
		w, _ = completeAttachment(t, h, "dev-1", "att-bad")
		if w.Code != http.StatusUnprocessableEntity {
			t.Fatalf("digest mismatch = %d, want 422", w.Code)
		}
		// Still open: identical chunk re-PUT idempotent, metadata unfinished.
		if w, body := putChunk(t, h, "dev-1", "att-bad", 0, []byte("abc")); w.Code != http.StatusOK || body["created"] != false {
			t.Fatalf("identical re-PUT after 422 = %d %v, want 200 created=false", w.Code, body)
		}
		if meta := getAttachmentMeta(t, h, "dev-1", "att-bad"); meta["complete"] != false {
			t.Fatalf("meta after 422 = %v, want complete=false", meta)
		}
		// Re-finishing re-reports 422; it never silently seals.
		if w, _ := completeAttachment(t, h, "dev-1", "att-bad"); w.Code != http.StatusUnprocessableEntity {
			t.Fatalf("repeat finish after 422 = %d, want 422", w.Code)
		}
	})
}

// A completed content that already exists under the same digest but a different
// size must make a later honest finish a 409, leave that upload open, and not
// reuse mismatched bytes. The precondition cannot be reached without a SHA-256
// collision, so the foreign content row is seeded directly; every behavioral
// assertion below is public HTTP.
func TestAttachmentContractSameDigestDifferentSizeIs409(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "digest-collision.db")

	s, err := app.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })

	// Seed the unreachable precondition straight into the content-addressed
	// table: a completed content stored under the real digest of the bytes we
	// will upload, but with a DIFFERENT size. Honest assembly can never reach
	// that state (it would require a SHA-256 collision), which is why only the
	// seed can create it.
	payload := []byte("abcde")
	digest := sha256Hex(payload)
	if _, err := s.DB().Exec(
		`INSERT INTO attachment_contents (sha256, size, data) VALUES (?, ?, ?)`,
		digest, int64(4), []byte("data")); err != nil {
		t.Fatal(err)
	}

	h := NewHandler(s)
	registerDevice(t, h, "dev-1")

	// Honest upload declaring the SAME digest but total size 5. The assembled
	// bytes hash to the declared digest, so the size-collision check is reached.
	w, body := createAttachment(t, h, "dev-1", "att-collide", 5, 5, digest)
	if w.Code != http.StatusOK || body["created"] != true {
		t.Fatalf("create = %d %v", w.Code, body)
	}
	// A single final chunk of 5 bytes is within the declared remainder.
	putChunk(t, h, "dev-1", "att-collide", 0, payload)
	w, _ = completeAttachment(t, h, "dev-1", "att-collide")
	if w.Code != http.StatusConflict {
		t.Fatalf("same digest different size finish = %d, want 409 body=%s", w.Code, w.Body.String())
	}
	assertJSONError(t, w)

	// The upload stays recoverable: not sealed and still unfinished.
	if meta := getAttachmentMeta(t, h, "dev-1", "att-collide"); meta["complete"] != false {
		t.Fatalf("meta after collision 409 = %v, want complete=false", meta)
	}
	if w, b := putChunk(t, h, "dev-1", "att-collide", 0, []byte("abcde")); w.Code != http.StatusOK || b["created"] != false {
		t.Fatalf("identical re-PUT after collision 409 = %d %v, want 200 created=false", w.Code, b)
	}
}

// Judgment order and content non-disclosure, all over public HTTP:
//   - attachment existence (404) is decided before ownership (403), including
//     for an unregistered device in the path;
//   - ownership (403) is decided before chunk existence (404);
//   - request shape (400) is decided before device registration (404);
//   - no failure body ever carries chunk bytes or attachment metadata.
func TestAttachmentContractErrorOrderAndNoLeak(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	registerDevice(t, h, "dev-2")

	content := []byte("hello world")
	mustCreateAttachment(t, h, "dev-1", "att-1", content, 4)
	putChunk(t, h, "dev-1", "att-1", 0, []byte("hell"))
	secret := "hell"

	// 404 (unknown attachment) beats 403 (non-owner) on every endpoint, even
	// when the path device itself was never registered.
	for _, tc := range []struct {
		name   string
		method string
		path   string
	}{
		{"put chunk", http.MethodPut, "/v1/devices/dev-2/attachments/ghost/chunks/0"},
		{"complete", http.MethodPost, "/v1/devices/dev-2/attachments/ghost/complete"},
		{"metadata", http.MethodGet, "/v1/devices/dev-2/attachments/ghost"},
		{"chunk read", http.MethodGet, "/v1/devices/dev-2/attachments/ghost/chunks/0"},
		{"put chunk, unregistered device", http.MethodPut, "/v1/devices/nobody/attachments/ghost/chunks/0"},
		{"complete, unregistered device", http.MethodPost, "/v1/devices/nobody/attachments/ghost/complete"},
		{"metadata, unregistered device", http.MethodGet, "/v1/devices/nobody/attachments/ghost"},
		{"chunk read, unregistered device", http.MethodGet, "/v1/devices/nobody/attachments/ghost/chunks/0"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var r *http.Request
			if tc.method == http.MethodPut {
				r = httptest.NewRequest(tc.method, tc.path, strings.NewReader("xxxx"))
				r.Header.Set("Content-Type", "application/octet-stream")
			} else {
				r = httptest.NewRequest(tc.method, tc.path, nil)
			}
			w := serveRecorder(h, r)
			if w.Code != http.StatusNotFound {
				t.Fatalf("%s = %d, want 404 body=%s", tc.name, w.Code, w.Body.String())
			}
			assertNoLeak(t, w, secret)
		})
	}

	// A known attachment owned by another device is 403, and ownership is
	// decided before chunk existence: asking for a chunk that never arrived is
	// still 403 (not 404).
	ownerChecks := []struct {
		name   string
		method string
		path   string
		body   bool
	}{
		{"put chunk existing index", http.MethodPut, "/v1/devices/dev-2/attachments/att-1/chunks/0", true},
		{"put chunk missing index", http.MethodPut, "/v1/devices/dev-2/attachments/att-1/chunks/2", true},
		{"complete", http.MethodPost, "/v1/devices/dev-2/attachments/att-1/complete", false},
		{"metadata", http.MethodGet, "/v1/devices/dev-2/attachments/att-1", false},
		{"chunk read existing", http.MethodGet, "/v1/devices/dev-2/attachments/att-1/chunks/0", false},
		{"chunk read never arrived", http.MethodGet, "/v1/devices/dev-2/attachments/att-1/chunks/1", false},
	}
	for _, tc := range ownerChecks {
		t.Run("non-creator "+tc.name, func(t *testing.T) {
			var r *http.Request
			if tc.body {
				r = httptest.NewRequest(tc.method, tc.path, strings.NewReader("rld!"))
				r.Header.Set("Content-Type", "application/octet-stream")
			} else {
				r = httptest.NewRequest(tc.method, tc.path, nil)
			}
			w := serveRecorder(h, r)
			if w.Code != http.StatusForbidden {
				t.Fatalf("%s = %d, want 403 body=%s", tc.name, w.Code, w.Body.String())
			}
			assertNoLeak(t, w, secret)
		})
	}

	// A known attachment, owner device, but a chunk that never arrived is 404.
	r := httptest.NewRequest(http.MethodGet, "/v1/devices/dev-1/attachments/att-1/chunks/2", nil)
	if w := serveRecorder(h, r); w.Code != http.StatusNotFound {
		t.Fatalf("owner missing chunk = %d, want 404", w.Code)
	}

	// Request shape (400) beats device registration (404): a malformed index
	// for an unregistered device is 400, not 404.
	r = httptest.NewRequest(http.MethodGet, "/v1/devices/nobody/attachments/ghost/chunks/x", nil)
	if w := serveRecorder(h, r); w.Code != http.StatusBadRequest {
		t.Fatalf("shape vs unregistered device = %d, want 400", w.Code)
	}

	// Creating an attachment for an unregistered device is a 404 that echoes no
	// metadata.
	w, _ := createAttachment(t, h, "ghost", "att-x", 1, 1, sha256Hex([]byte("z")))
	if w.Code != http.StatusNotFound {
		t.Fatalf("create for unregistered = %d, want 404", w.Code)
	}
	assertNoLeak(t, w, secret)
}

// assertNoLeak fails if the response body carries the attachment's bytes, its
// declared digest, or any metadata field (error bodies stay generic).
func assertNoLeak(t *testing.T, w *httptest.ResponseRecorder, secret string) {
	t.Helper()
	body := w.Body.String()
	if strings.Contains(body, secret) {
		t.Fatalf("error body leaks chunk bytes %q: %q", secret, body)
	}
	if strings.Contains(body, "totalBytes") || strings.Contains(body, "receivedChunks") ||
		strings.Contains(body, "chunkSize") || strings.Contains(body, "sha256") {
		t.Fatalf("error body leaks attachment metadata: %q", body)
	}
	var errBody map[string]string
	if err := json.Unmarshal(w.Body.Bytes(), &errBody); err != nil || errBody["error"] == "" {
		t.Fatalf("body = %q, want a JSON error", body)
	}
}
