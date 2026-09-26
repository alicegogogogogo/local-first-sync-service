package server

import (
	"net/http"
	"testing"
)

// A trimmed change id re-submitted through the ordinary post, the offline
// replay and the merge entry compares only source device and payload — never
// the restore provenance — so none of the three misreports a 409.
func TestTrimmedIDResubmissionComparesDeviceAndPayloadOnly(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")

	// c1 ordinary, r1 a restore of snapshot 1; snapshot at 3 then compact all.
	postDocChanges(t, h, "doc", "dev-1", 2)
	w, _ := postJSON(t, h, "/v1/documents/doc/snapshots", map[string]any{"cursor": 1, "state": map[string]any{"snap": 1}})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	w, _ = postJSON(t, h, "/v1/documents/doc/restore", map[string]any{
		"deviceId": "dev-1", "changeId": "r1", "snapshotCursor": 1,
	})
	if w.Code != http.StatusOK {
		t.Fatalf("restore: %s", w.Body.String())
	}
	w, _ = postJSON(t, h, "/v1/documents/doc/snapshots", map[string]any{"cursor": 3, "state": nil})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	if w := compactChanges(t, h, "doc", "dev-1"); w.Code != http.StatusOK {
		t.Fatalf("compact: %s", w.Body.String())
	}

	// Ordinary post of the trimmed restore id with the same device and the
	// restored state as payload: idempotent, not a 409.
	w, body := postJSON(t, h, "/v1/documents/doc/changes", map[string]any{
		"deviceId": "dev-1",
		"changes":  []any{map[string]any{"id": "r1", "payload": map[string]any{"snap": 1}}},
	})
	if w.Code != http.StatusOK {
		t.Fatalf("post of trimmed restore id = %d %s", w.Code, w.Body.String())
	}
	res := body["results"].([]any)[0].(map[string]any)
	if res["created"] != false || int64(res["cursor"].(float64)) != 3 {
		t.Fatalf("post result = %v, want created=false cursor 3", res)
	}

	// Offline replay of the trimmed ordinary id: idempotent as well.
	w, body = postJSON(t, h, "/v1/documents/doc/replay", map[string]any{
		"deviceId":   "dev-1",
		"operations": []any{map[string]any{"id": "c1", "payload": map[string]any{"n": 1}}},
	})
	if w.Code != http.StatusOK {
		t.Fatalf("replay of trimmed id = %d %s", w.Code, w.Body.String())
	}
	res = body["results"].([]any)[0].(map[string]any)
	if res["created"] != false || int64(res["cursor"].(float64)) != 1 {
		t.Fatalf("replay result = %v, want created=false cursor 1", res)
	}

	// Merge carrying the trimmed restore id: idempotent, no provenance check.
	w, body = postJSON(t, h, "/v1/documents/doc/merge", map[string]any{
		"deviceId":   "dev-1",
		"baseCursor": 3,
		"change":     map[string]any{"id": "r1", "payload": map[string]any{"snap": 1}},
	})
	if w.Code != http.StatusOK {
		t.Fatalf("merge of trimmed restore id = %d %s", w.Code, w.Body.String())
	}
	if body["outcome"] != "idempotent" || int64(body["cursor"].(float64)) != 3 {
		t.Fatalf("merge result = %v, want idempotent cursor 3", body)
	}

	// A differing payload through any of the three is still a 409.
	w, _ = postJSON(t, h, "/v1/documents/doc/merge", map[string]any{
		"deviceId":   "dev-1",
		"baseCursor": 3,
		"change":     map[string]any{"id": "r1", "payload": map[string]any{"snap": 99}},
	})
	if w.Code != http.StatusConflict {
		t.Fatalf("merge payload mismatch = %d, want 409", w.Code)
	}
}
