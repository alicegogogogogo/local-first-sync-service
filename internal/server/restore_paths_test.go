package server

import (
	"net/http"
	"strings"
	"testing"
)

// The document-level merge and restore endpoints are POST-only at their exact
// locations: a trailing slash or extra segment is a JSON 400 (never a
// redirect or an HTML page), and every other verb on the exact path is a JSON
// 400 rather than ServeMux's plain-text 405.
func TestDocumentMergeRestoreMalformedPathsAndMethods(t *testing.T) {
	h, _ := newTestHandler(t)
	mergeBody := `{"deviceId":"d","baseCursor":0,"change":{"id":"c","payload":{}}}`
	restoreBody := `{"deviceId":"d","changeId":"c","snapshotCursor":1}`

	for _, tc := range []struct {
		name string
		path string
		body string
	}{
		{"merge trailing slash", "/v1/documents/doc/merge/", mergeBody},
		{"merge extra segment", "/v1/documents/doc/merge/extra", mergeBody},
		{"merge two extra segments", "/v1/documents/doc/merge/extra/more", mergeBody},
		{"restore trailing slash", "/v1/documents/doc/restore/", restoreBody},
		{"restore extra segment", "/v1/documents/doc/restore/extra", restoreBody},
		{"restore two extra segments", "/v1/documents/doc/restore/extra/more", restoreBody},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := newJSONRequest(http.MethodPost, tc.path, tc.body, "application/json")
			w := serveRecorder(h, r)
			if w.Code != http.StatusBadRequest {
				t.Fatalf("POST %s status = %d, want 400, body = %q", tc.path, w.Code, w.Body.String())
			}
			assertJSONError(t, w)
			if loc := w.Header().Get("Location"); loc != "" {
				t.Fatalf("POST %s redirected to %q", tc.path, loc)
			}
			if strings.Contains(strings.ToLower(w.Body.String()), "<html") {
				t.Fatalf("POST %s leaked HTML: %q", tc.path, w.Body.String())
			}
		})
	}

	for _, tc := range []struct {
		name string
		path string
		body string
	}{
		{"merge", "/v1/documents/doc/merge", mergeBody},
		{"restore", "/v1/documents/doc/restore", restoreBody},
	} {
		for _, method := range []string{http.MethodGet, http.MethodPut, http.MethodDelete, http.MethodPatch, http.MethodOptions} {
			t.Run(tc.name+" "+method, func(t *testing.T) {
				r := newJSONRequest(method, tc.path, tc.body, "application/json")
				w := serveRecorder(h, r)
				if w.Code != http.StatusBadRequest {
					t.Fatalf("%s %s status = %d, want 400, body = %q", method, tc.path, w.Code, w.Body.String())
				}
				assertJSONError(t, w)
			})
		}
	}
}

// A document literally named "merge" or "restore" keeps its ordinary
// document-level routes; the keyword is only an endpoint word in its own
// terminal segment position.
func TestDocumentNamedMergeRestoreKeepsRoutes(t *testing.T) {
	h, _ := newTestHandler(t)

	w, _ := postJSON(t, h, "/v1/documents/merge/changes", map[string]any{
		"deviceId": "dev",
		"changes":  []any{map[string]any{"id": "c1", "payload": map[string]any{"n": 1}}},
	})
	if w.Code != http.StatusOK {
		t.Fatalf("post to doc named merge = %d %s", w.Code, w.Body.String())
	}
	w, _ = mergeBody(t, h, "restore", map[string]any{
		"deviceId":   "dev",
		"baseCursor": 0,
		"change":     map[string]any{"id": "m1", "payload": map[string]any{"a": 1}},
	})
	if w.Code != http.StatusOK {
		t.Fatalf("merge on doc named restore = %d %s", w.Code, w.Body.String())
	}
}
