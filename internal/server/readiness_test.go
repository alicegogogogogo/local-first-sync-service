package server

import (
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/alicegogogogogo/local-first-sync-service/internal/store"
)

func newGatedHandler(t *testing.T, isReady func() bool) http.Handler {
	t.Helper()
	s, err := store.Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return NewHandlerWithReadiness(s, isReady)
}

// Before readiness every route — /healthz included — is a 503 JSON error, so
// no caller can observe a successful health answer or reach a business
// endpoint while startup is incomplete.
func TestReadinessGateBlocksUntilReady(t *testing.T) {
	var ready bool
	h := newGatedHandler(t, func() bool { return ready })

	cases := []struct {
		method string
		path   string
	}{
		{http.MethodGet, "/healthz"},
		{http.MethodPost, "/v1/devices"},
		{http.MethodGet, "/v1/documents/doc1/changes"},
		{http.MethodPost, "/v1/devices/dev1/attachments"},
	}
	for _, c := range cases {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest(c.method, c.path, nil))
		if w.Code != http.StatusServiceUnavailable {
			t.Fatalf("%s %s before ready: status = %d, want %d", c.method, c.path, w.Code, http.StatusServiceUnavailable)
		}
		if got := w.Header().Get("Content-Type"); got != "application/json" {
			t.Fatalf("%s %s before ready: content type = %q", c.method, c.path, got)
		}
	}

	ready = true

	// Once ready, /healthz is the original 200 JSON contract, byte shape and
	// content type unchanged.
	w, body := doRequest(t, h, http.MethodGet, "/healthz")
	if w.Code != http.StatusOK {
		t.Fatalf("healthz after ready: status = %d, want %d", w.Code, http.StatusOK)
	}
	if body["status"] != "ok" {
		t.Fatalf("healthz after ready: body = %s", w.Body.String())
	}
	if got := w.Header().Get("Content-Type"); got != "application/json" {
		t.Fatalf("healthz after ready: content type = %q", got)
	}

	// Business endpoints are reachable only after readiness.
	w2, body2 := postJSON(t, h, "/v1/devices", map[string]string{"deviceId": "dev-ready"})
	if w2.Code != http.StatusOK || body2["created"] != true {
		t.Fatalf("register after ready: status = %d body = %s", w2.Code, w2.Body.String())
	}
}

// With a nil gate the handler behaves exactly like NewHandler: always open.
func TestReadinessGateNilMeansAlwaysReady(t *testing.T) {
	h := newGatedHandler(t, nil)
	w, _ := doRequest(t, h, http.MethodGet, "/healthz")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusOK)
	}
}
