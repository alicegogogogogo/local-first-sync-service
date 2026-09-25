package events_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"sync"
	"testing"

	"github.com/alicegogogogogo/local-first-sync-service/internal/app"
	"github.com/alicegogogogogo/local-first-sync-service/internal/events"
	"github.com/alicegogogogogo/local-first-sync-service/internal/store"
)

func changes(ids ...string) []events.Change {
	out := make([]events.Change, len(ids))
	for i, id := range ids {
		out[i] = events.Change{ID: id, DeviceID: "dev-1", Payload: json.RawMessage(fmt.Sprintf(`{"n":%d}`, i+1))}
	}
	return out
}

func TestPostAndListBasic(t *testing.T) {
	s, err := app.Open("")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()

	results, err := s.PostChanges("doc-a", changes("c1", "c2", "c3"))
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 3 {
		t.Fatalf("got %d results, want 3", len(results))
	}
	for i, r := range results {
		if !r.Created || r.Cursor != int64(i+1) {
			t.Fatalf("result %d = %+v, want created cursor %d", i, r, i+1)
		}
		if r.ID != []string{"c1", "c2", "c3"}[i] {
			t.Fatalf("result %d id = %q, wrong order", i, r.ID)
		}
	}

	got, next, err := s.ListChanges("doc-a", 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 || next != 3 {
		t.Fatalf("list = %+v next=%d, want 3 rows next=3", got, next)
	}
	if got[0].DeviceID != "dev-1" {
		t.Fatalf("deviceId = %q", got[0].DeviceID)
	}
	if string(got[0].Payload) != `{"n":1}` {
		t.Fatalf("payload = %s", got[0].Payload)
	}
}

func TestIdempotentRepost(t *testing.T) {
	s, _ := app.Open("")
	defer func() { _ = s.Close() }()

	first, err := s.PostChanges("doc", []events.Change{
		{ID: "x1", DeviceID: "dev", Payload: json.RawMessage(`{"a":1,"b":2}`)},
	})
	if err != nil {
		t.Fatal(err)
	}

	// Same deviceId and semantically equal payload (reordered keys, whitespace):
	// idempotent, cursor stays the first value.
	again, err := s.PostChanges("doc", []events.Change{
		{ID: "x1", DeviceID: "dev", Payload: json.RawMessage(`{ "b": 2, "a": 1 }`)},
	})
	if err != nil {
		t.Fatalf("idempotent repost: %v", err)
	}
	if again[0].Created || again[0].Cursor != first[0].Cursor || again[0].Cursor != 1 {
		t.Fatalf("repost result = %+v, want created=false cursor=1", again[0])
	}

	// Number vs number-with-fraction decode identically.
	again2, err := s.PostChanges("doc", []events.Change{
		{ID: "x1", DeviceID: "dev", Payload: json.RawMessage(`{"a":1.0,"b":2}`)},
	})
	if err != nil {
		t.Fatalf("numeric-equivalent repost: %v", err)
	}
	if again2[0].Created {
		t.Fatalf("expected idempotent for 1 vs 1.0")
	}
}

func TestConflictZeroWrite(t *testing.T) {
	s, _ := app.Open("")
	defer func() { _ = s.Close() }()

	if _, err := s.PostChanges("doc", []events.Change{
		{ID: "x1", DeviceID: "dev", Payload: json.RawMessage(`{"v":1}`)},
	}); err != nil {
		t.Fatal(err)
	}

	// Different payload -> 409-equivalent conflict, and the new id in the same
	// batch must not be written.
	_, err := s.PostChanges("doc", []events.Change{
		{ID: "new-1", DeviceID: "dev", Payload: json.RawMessage(`{"v":2}`)},
		{ID: "x1", DeviceID: "dev", Payload: json.RawMessage(`{"v":99}`)},
	})
	var conflict *events.ErrConflict
	if !errors.As(err, &conflict) || conflict.ID != "x1" {
		t.Fatalf("want *events.ErrConflict{x1}, got %v", err)
	}
	rows, _, err := s.ListChanges("doc", 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].ID != "x1" {
		t.Fatalf("batch was not zero-write: %+v", rows)
	}

	// Different deviceId -> conflict as well.
	_, err = s.PostChanges("doc", []events.Change{
		{ID: "x1", DeviceID: "other", Payload: json.RawMessage(`{"v":1}`)},
	})
	if !errors.As(err, &conflict) {
		t.Fatalf("device mismatch want conflict, got %v", err)
	}
}

func TestListPaginationAndUnknownDocument(t *testing.T) {
	s, _ := app.Open("")
	defer func() { _ = s.Close() }()

	if _, err := s.PostChanges("doc", changes("a", "b", "c", "d", "e")); err != nil {
		t.Fatal(err)
	}

	page, next, err := s.ListChanges("doc", 0, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(page) != 2 || page[0].ID != "a" || page[1].ID != "b" || next != 2 {
		t.Fatalf("page1 = %+v next=%d", page, next)
	}

	page, next, err = s.ListChanges("doc", next, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(page) != 2 || page[0].ID != "c" || next != 4 {
		t.Fatalf("page2 = %+v next=%d", page, next)
	}

	// Known document, nothing past the last cursor -> empty page, nextCursor == after.
	page, next, err = s.ListChanges("doc", 5, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(page) != 0 || next != 5 {
		t.Fatalf("tail = %+v next=%d, want empty next=5", page, next)
	}

	// Unknown document -> empty list and cursor 0.
	page, next, err = s.ListChanges("nope", 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(page) != 0 || next != 0 {
		t.Fatalf("unknown doc = %+v next=%d, want empty next=0", page, next)
	}
}

func TestDocumentsAreIndependent(t *testing.T) {
	s, _ := app.Open("")
	defer func() { _ = s.Close() }()

	r1, _ := s.PostChanges("doc1", changes("a"))
	r2, _ := s.PostChanges("doc2", changes("a"))
	if r1[0].Cursor != 1 || r2[0].Cursor != 1 {
		t.Fatalf("cursors should restart per document: %d %d", r1[0].Cursor, r2[0].Cursor)
	}

	// Same id with different payload in another document: that document has
	// its own row and must conflict there.
	r3, err := s.PostChanges("doc2", []events.Change{{ID: "a", DeviceID: "dev-1", Payload: json.RawMessage(`{"n":2}`)}})
	if err == nil || r3 != nil {
		t.Fatalf("same doc id with different payload should conflict")
	}
}

func TestPersistenceAcrossReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sync.db")

	s, err := app.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.PostChanges("doc", []events.Change{
		{ID: "k1", DeviceID: "dev", Payload: json.RawMessage(`{"hello":"world"}`)},
		{ID: "k2", DeviceID: "dev", Payload: json.RawMessage(`[1,2,3]`)},
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen: previously committed changes and cursors must be readable, and
	// new cursors continue after the persisted high-water mark.
	s2, err := app.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s2.Close() }()

	rows, next, err := s2.ListChanges("doc", 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 || next != 2 {
		t.Fatalf("after reopen rows = %+v next=%d", rows, next)
	}
	if string(rows[0].Payload) != `{"hello":"world"}` || rows[0].Cursor != 1 {
		t.Fatalf("row0 after reopen = %+v", rows[0])
	}

	// Idempotent replay after restart.
	replay, err := s2.PostChanges("doc", []events.Change{
		{ID: "k1", DeviceID: "dev", Payload: json.RawMessage(`{"hello":"world"}`)},
	})
	if err != nil || replay[0].Created || replay[0].Cursor != 1 {
		t.Fatalf("replay after restart = %+v err=%v", replay, err)
	}

	more, err := s2.PostChanges("doc", []events.Change{
		{ID: "k3", DeviceID: "dev", Payload: json.RawMessage(`true`)},
	})
	if err != nil || !more[0].Created || more[0].Cursor != 3 {
		t.Fatalf("new cursor after restart = %+v err=%v", more, err)
	}
}

func TestConcurrentBatchesNoDuplicateCursors(t *testing.T) {
	s, _ := app.Open("")
	defer func() { _ = s.Close() }()

	const goroutines = 16
	const perBatch = 25

	var wg sync.WaitGroup
	errCh := make(chan error, goroutines)
	var mu sync.Mutex
	all := make([]events.Result, 0, goroutines*perBatch)

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			batch := make([]events.Change, perBatch)
			for i := range batch {
				id := fmt.Sprintf("g%d-i%d", g, i)
				batch[i] = events.Change{ID: id, DeviceID: "dev", Payload: json.RawMessage(fmt.Sprintf(`{"g":%d,"i":%d}`, g, i))}
			}
			results, err := s.PostChanges("doc", batch)
			if err != nil {
				errCh <- err
				return
			}
			mu.Lock()
			all = append(all, results...)
			mu.Unlock()
		}(g)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}

	if len(all) != goroutines*perBatch {
		t.Fatalf("lost records: got %d results", len(all))
	}

	seen := make(map[int64]string, len(all))
	for _, r := range all {
		if prev, ok := seen[r.Cursor]; ok {
			t.Fatalf("cursor %d assigned to both %s and %s", r.Cursor, prev, r.ID)
		}
		seen[r.Cursor] = r.ID
	}

	cursors := make([]int, 0, len(seen))
	for c := range seen {
		cursors = append(cursors, int(c))
	}
	sort.Ints(cursors)
	for i, c := range cursors {
		if c != i+1 {
			t.Fatalf("cursors not contiguous: position %d has cursor %d", i, c)
		}
	}

	rows, _, err := s.ListChanges("doc", 0, 1000)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != goroutines*perBatch {
		t.Fatalf("stored rows = %d, want %d", len(rows), goroutines*perBatch)
	}
}

func TestMergeAppliedAtCurrentCursor(t *testing.T) {
	s, _ := app.Open("")
	defer func() { _ = s.Close() }()

	// Unknown document, baseCursor 0 -> applied at cursor 1.
	r, err := s.MergeChange("doc", 0, events.Change{ID: "c1", DeviceID: "dev", Payload: json.RawMessage(`{"a":1}`)})
	if err != nil {
		t.Fatal(err)
	}
	if r.Outcome != "applied" || r.Cursor != 1 || r.Result != nil {
		t.Fatalf("first merge = %+v", r)
	}

	// baseCursor == current -> applied.
	r, err = s.MergeChange("doc", 1, events.Change{ID: "c2", DeviceID: "dev", Payload: json.RawMessage(`{"b":2}`)})
	if err != nil {
		t.Fatal(err)
	}
	if r.Outcome != "applied" || r.Cursor != 2 {
		t.Fatalf("second merge = %+v", r)
	}
}

func TestMergeUnknownDocRejectsNonZeroBase(t *testing.T) {
	s, _ := app.Open("")
	defer func() { _ = s.Close() }()

	_, err := s.MergeChange("ghost", 1, events.Change{ID: "c", DeviceID: "dev", Payload: json.RawMessage(`{"a":1}`)})
	if !errors.Is(err, events.ErrStaleCursor) {
		t.Fatalf("want events.ErrStaleCursor, got %v", err)
	}

	// baseCursor ahead of the current cursor is also 400-class.
	if _, err := s.MergeChange("doc", 0, events.Change{ID: "c1", DeviceID: "dev", Payload: json.RawMessage(`{"a":1}`)}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.MergeChange("doc", 5, events.Change{ID: "c2", DeviceID: "dev", Payload: json.RawMessage(`{"b":2}`)}); !errors.Is(err, events.ErrStaleCursor) {
		t.Fatalf("ahead cursor want events.ErrStaleCursor, got %v", err)
	}
}

func TestMergeIdempotentExisting(t *testing.T) {
	s, _ := app.Open("")
	defer func() { _ = s.Close() }()

	if _, err := s.MergeChange("doc", 0, events.Change{ID: "c1", DeviceID: "dev", Payload: json.RawMessage(`{"a":1}`)}); err != nil {
		t.Fatal(err)
	}

	// Repost identical id/payload/device, even with a stale base, is
	// idempotent and returns the original result.
	r, err := s.MergeChange("doc", 0, events.Change{ID: "c1", DeviceID: "dev", Payload: json.RawMessage(`{ "a": 1 }`)})
	if err != nil {
		t.Fatalf("idempotent merge: %v", err)
	}
	if r.Outcome != "idempotent" || r.Cursor != 1 || r.Result == nil {
		t.Fatalf("merge result = %+v", r)
	}
	if r.Result.ID != "c1" || r.Result.Created || r.Result.Cursor != 1 {
		t.Fatalf("embedded result = %+v", r.Result)
	}

	rows, next, _ := s.ListChanges("doc", 0, 100)
	if len(rows) != 1 || next != 1 {
		t.Fatalf("idempotent merge wrote a row: %+v next=%d", rows, next)
	}

	// Mismatched payload -> conflict, zero write.
	if _, err := s.MergeChange("doc", 1, events.Change{ID: "c1", DeviceID: "dev", Payload: json.RawMessage(`{"a":2}`)}); !errors.As(err, new(*events.ErrConflict)) {
		t.Fatalf("payload mismatch want conflict, got %v", err)
	}
	// Mismatched device -> conflict.
	if _, err := s.MergeChange("doc", 1, events.Change{ID: "c1", DeviceID: "other", Payload: json.RawMessage(`{"a":1}`)}); !errors.As(err, new(*events.ErrConflict)) {
		t.Fatalf("device mismatch want conflict, got %v", err)
	}
}

func TestMergeBehindDisjointObjects(t *testing.T) {
	s, _ := app.Open("")
	defer func() { _ = s.Close() }()

	// cursor 1: {"a":1}, cursor 2: {"b":2}
	if _, err := s.PostChanges("doc", []events.Change{
		{ID: "a", DeviceID: "dev", Payload: json.RawMessage(`{"a":1}`)},
		{ID: "b", DeviceID: "dev", Payload: json.RawMessage(`{"b":2}`)},
	}); err != nil {
		t.Fatal(err)
	}

	// Client saw baseCursor 1 (only "a"); new payload {"c":3} shares no
	// top-level keys with the later {"b":2} -> merged at cursor 3.
	r, err := s.MergeChange("doc", 1, events.Change{ID: "c", DeviceID: "dev", Payload: json.RawMessage(`{"c":3}`)})
	if err != nil {
		t.Fatalf("merge: %v", err)
	}
	if r.Outcome != "merged" || r.Cursor != 3 {
		t.Fatalf("merge result = %+v", r)
	}
}

func TestMergeBehindConflicts(t *testing.T) {
	// Later change reuses a top-level key -> 409, zero write.
	s, _ := app.Open("")
	defer func() { _ = s.Close() }()
	if _, err := s.PostChanges("doc", []events.Change{
		{ID: "a", DeviceID: "dev", Payload: json.RawMessage(`{"a":1}`)},
		{ID: "b", DeviceID: "dev", Payload: json.RawMessage(`{"b":2}`)},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.MergeChange("doc", 0, events.Change{ID: "x", DeviceID: "dev", Payload: json.RawMessage(`{"b":9}`)}); !errors.As(err, new(*events.ErrConflict)) {
		t.Fatalf("key clash want conflict, got %v", err)
	}
	rows, next, _ := s.ListChanges("doc", 0, 100)
	if len(rows) != 2 || next != 2 {
		t.Fatalf("conflict merge wrote a row: %+v next=%d", rows, next)
	}

	// A disjoint-object merge against the same doc is allowed (cursor 3),
	// proving the earlier 409 was specifically about the key clash.
	if r, err := s.MergeChange("doc", 0, events.Change{ID: "y", DeviceID: "dev", Payload: json.RawMessage(`{"z":3}`)}); err != nil || r.Outcome != "merged" || r.Cursor != 3 {
		t.Fatalf("disjoint merge = %+v err=%v", r, err)
	}

	// Seed a non-object later payload in a fresh doc to prove the rule.
	s2, _ := app.Open("")
	defer func() { _ = s2.Close() }()
	if _, err := s2.PostChanges("d2", []events.Change{
		{ID: "arr", DeviceID: "dev", Payload: json.RawMessage(`[1,2,3]`)},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := s2.MergeChange("d2", 0, events.Change{ID: "o", DeviceID: "dev", Payload: json.RawMessage(`{"k":1}`)}); !errors.As(err, new(*events.ErrConflict)) {
		t.Fatalf("array later want conflict, got %v", err)
	}
}

func TestMergeConcurrentSerialization(t *testing.T) {
	s, _ := app.Open("")
	defer func() { _ = s.Close() }()
	// Seed current cursor so most merges race on the same high-water mark.
	if _, err := s.MergeChange("doc", 0, events.Change{ID: "seed", DeviceID: "dev", Payload: json.RawMessage(`{"seed":0}`)}); err != nil {
		t.Fatal(err)
	}

	const n = 30
	var wg sync.WaitGroup
	errCh := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			// Every client observed cursor 1; payloads have disjoint keys.
			_, err := s.MergeChange("doc", 1, events.Change{
				ID:       fmt.Sprintf("m%d", i),
				DeviceID: "dev",
				Payload:  json.RawMessage(fmt.Sprintf(`{"k%d":%d}`, i, i)),
			})
			if err != nil {
				errCh <- err
			}
		}(i)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}

	rows, next, err := s.ListChanges("doc", 0, 1000)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != n+1 || next != n+1 {
		t.Fatalf("rows=%d next=%d want %d/%d", len(rows), next, n+1, n+1)
	}
}

func TestMergePersistsAcrossReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sync.db")

	s, err := app.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.MergeChange("doc", 0, events.Change{ID: "c1", DeviceID: "dev", Payload: json.RawMessage(`{"a":1}`)}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.MergeChange("doc", 1, events.Change{ID: "c2", DeviceID: "dev", Payload: json.RawMessage(`{"b":2}`)}); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s2, err := app.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s2.Close() }()

	rows, next, err := s2.ListChanges("doc", 0, 100)
	if err != nil || len(rows) != 2 || next != 2 {
		t.Fatalf("after reopen rows=%+v next=%d err=%v", rows, next, err)
	}

	// A merge that was valid before restart still appends at the right cursor.
	r, err := s2.MergeChange("doc", 2, events.Change{ID: "c3", DeviceID: "dev", Payload: json.RawMessage(`{"c":3}`)})
	if err != nil || r.Outcome != "applied" || r.Cursor != 3 {
		t.Fatalf("post-restart merge = %+v err=%v", r, err)
	}
}

func TestSnapshotPutGetAndIdempotentRetry(t *testing.T) {
	s, err := app.Open("")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()

	if _, err := s.PostChanges("doc", changes("c1", "c2")); err != nil {
		t.Fatal(err)
	}

	created, err := s.PutSnapshot("doc", 2, json.RawMessage(`{"text":"hello","n":1}`))
	if err != nil || !created {
		t.Fatalf("first put = created:%v err:%v", created, err)
	}

	state, err := s.GetSnapshot("doc", 2)
	if err != nil {
		t.Fatal(err)
	}
	if !store.JSONEqual(state, json.RawMessage(`{"n":1,"text":"hello"}`)) {
		t.Fatalf("state = %s", state)
	}

	// Retry with a decoded-equal state (key order, number formatting) is
	// idempotent and reports created=false.
	created, err = s.PutSnapshot("doc", 2, json.RawMessage(`{"n":1.0,"text":"hello"}`))
	if err != nil || created {
		t.Fatalf("retry = created:%v err:%v", created, err)
	}
}

func TestSnapshotConflictKeepsOriginal(t *testing.T) {
	s, err := app.Open("")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()

	if _, err := s.PostChanges("doc", changes("c1")); err != nil {
		t.Fatal(err)
	}
	if _, err := s.PutSnapshot("doc", 1, json.RawMessage(`{"v":1}`)); err != nil {
		t.Fatal(err)
	}

	_, err = s.PutSnapshot("doc", 1, json.RawMessage(`{"v":2}`))
	var conflict *events.ErrSnapshotConflict
	if !errors.As(err, &conflict) {
		t.Fatalf("conflicting put err = %v", err)
	}

	state, err := s.GetSnapshot("doc", 1)
	if err != nil || !store.JSONEqual(state, json.RawMessage(`{"v":1}`)) {
		t.Fatalf("state after conflict = %s err=%v", state, err)
	}
}

func TestSnapshotRejectsNonExistingCursor(t *testing.T) {
	s, err := app.Open("")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()

	if _, err := s.PostChanges("doc", changes("c1", "c2")); err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		name   string
		doc    string
		cursor int64
	}{
		{"unknown document", "nope", 1},
		{"cursor zero", "doc", 0},
		{"cursor ahead", "doc", 3},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := s.PutSnapshot(tc.doc, tc.cursor, json.RawMessage(`{"v":1}`))
			if !errors.Is(err, events.ErrSnapshotBase) {
				t.Fatalf("err = %v, want events.ErrSnapshotBase", err)
			}
			if _, err := s.GetSnapshot(tc.doc, tc.cursor); !errors.Is(err, events.ErrSnapshotNotFound) {
				t.Fatalf("zero-write violated: get err = %v", err)
			}
		})
	}
}

func TestSnapshotGetMissing(t *testing.T) {
	s, err := app.Open("")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()

	if _, err := s.GetSnapshot("nope", 1); !errors.Is(err, events.ErrSnapshotNotFound) {
		t.Fatalf("unknown doc err = %v", err)
	}
	if _, err := s.PostChanges("doc", changes("c1")); err != nil {
		t.Fatal(err)
	}
	if _, err := s.GetSnapshot("doc", 1); !errors.Is(err, events.ErrSnapshotNotFound) {
		t.Fatalf("known doc without snapshot err = %v", err)
	}
}

func TestSnapshotPersistsAcrossReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sync.db")

	s, err := app.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.PostChanges("doc", changes("c1", "c2")); err != nil {
		t.Fatal(err)
	}
	if _, err := s.PutSnapshot("doc", 1, json.RawMessage(`{"a":1}`)); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s2, err := app.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s2.Close() }()

	state, err := s2.GetSnapshot("doc", 1)
	if err != nil || !store.JSONEqual(state, json.RawMessage(`{"a":1}`)) {
		t.Fatalf("state after reopen = %s err=%v", state, err)
	}

	// Idempotency and conflict decisions survive the restart.
	created, err := s2.PutSnapshot("doc", 1, json.RawMessage(`{"a":1}`))
	if err != nil || created {
		t.Fatalf("retry after reopen = created:%v err:%v", created, err)
	}
	var conflict *events.ErrSnapshotConflict
	if _, err := s2.PutSnapshot("doc", 1, json.RawMessage(`{"a":9}`)); !errors.As(err, &conflict) {
		t.Fatalf("conflict after reopen err = %v", err)
	}

	// Snapshots do not move the change log.
	rows, next, err := s2.ListChanges("doc", 0, 100)
	if err != nil || len(rows) != 2 || next != 2 {
		t.Fatalf("changes after snapshots = %+v next=%d err=%v", rows, next, err)
	}
}

func TestSnapshotConcurrentPuts(t *testing.T) {
	s, err := app.Open("")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()

	if _, err := s.PostChanges("doc", changes("c1")); err != nil {
		t.Fatal(err)
	}

	// Concurrent identical puts: exactly one creator, no errors.
	var wg sync.WaitGroup
	var mu sync.Mutex
	creators := 0
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			created, err := s.PutSnapshot("doc", 1, json.RawMessage(`{"v":1}`))
			if err != nil {
				t.Error(err)
				return
			}
			if created {
				mu.Lock()
				creators++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if creators != 1 {
		t.Fatalf("creators = %d, want 1", creators)
	}
}

func TestRestoreAppendsSnapshotState(t *testing.T) {
	s, _ := app.Open("")
	defer func() { _ = s.Close() }()

	if _, err := s.PostChanges("doc", changes("c1", "c2")); err != nil {
		t.Fatal(err)
	}
	if _, err := s.PutSnapshot("doc", 2, json.RawMessage(`{"v":2}`)); err != nil {
		t.Fatal(err)
	}

	r, err := s.RestoreSnapshot("doc", "dev", "r1", 2)
	if err != nil {
		t.Fatalf("restore: %v", err)
	}
	if r.ID != "r1" || !r.Created || r.Cursor != 3 || r.RestoredFrom != 2 {
		t.Fatalf("restore result = %+v", r)
	}

	// The restore is an ordinary change carrying the snapshot state; old rows
	// are unchanged and the cursor advanced by exactly one.
	rows, next, err := s.ListChanges("doc", 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 3 || next != 3 {
		t.Fatalf("rows = %+v next=%d", rows, next)
	}
	last := rows[2]
	if last.ID != "r1" || last.DeviceID != "dev" || last.Cursor != 3 || !store.JSONEqual(last.Payload, json.RawMessage(`{"v":2}`)) {
		t.Fatalf("restored row = %+v", last)
	}
	if rows[0].ID != "c1" || rows[1].ID != "c2" {
		t.Fatalf("old rows changed: %+v", rows)
	}

	// A second restore appends again with a new cursor.
	r, err = s.RestoreSnapshot("doc", "dev", "r2", 2)
	if err != nil || !r.Created || r.Cursor != 4 {
		t.Fatalf("second restore = %+v err=%v", r, err)
	}
}

func TestRestoreSnapshotMiss(t *testing.T) {
	s, _ := app.Open("")
	defer func() { _ = s.Close() }()

	if _, err := s.PostChanges("doc", changes("c1", "c2")); err != nil {
		t.Fatal(err)
	}
	if _, err := s.PutSnapshot("doc", 2, json.RawMessage(`{"v":2}`)); err != nil {
		t.Fatal(err)
	}

	for name, docCursor := range map[string]struct {
		doc    string
		cursor int64
	}{
		"unknown document":        {"nope", 1},
		"cursor without snapshot": {"doc", 1},
		"cursor ahead":            {"doc", 99},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := s.RestoreSnapshot(docCursor.doc, "dev", "r", docCursor.cursor)
			if !errors.Is(err, events.ErrSnapshotNotFound) {
				t.Fatalf("err = %v, want events.ErrSnapshotNotFound", err)
			}
		})
	}

	// Zero writes: nothing was appended.
	rows, next, _ := s.ListChanges("doc", 0, 100)
	if len(rows) != 2 || next != 2 {
		t.Fatalf("zero-write violated: rows=%d next=%d", len(rows), next)
	}
}

func TestRestoreIdempotentRepeat(t *testing.T) {
	s, _ := app.Open("")
	defer func() { _ = s.Close() }()

	if _, err := s.PostChanges("doc", changes("c1")); err != nil {
		t.Fatal(err)
	}
	if _, err := s.PutSnapshot("doc", 1, json.RawMessage(`{"v":1}`)); err != nil {
		t.Fatal(err)
	}

	first, err := s.RestoreSnapshot("doc", "dev", "r1", 1)
	if err != nil {
		t.Fatal(err)
	}

	// Identical repeat is idempotent: created=false, first cursor, and no new
	// row even when the document advanced in the meantime.
	if _, err := s.PostChanges("doc", []events.Change{
		{ID: "later", DeviceID: "dev", Payload: json.RawMessage(`{"x":1}`)},
	}); err != nil {
		t.Fatal(err)
	}
	again, err := s.RestoreSnapshot("doc", "dev", "r1", 1)
	if err != nil {
		t.Fatalf("repeat: %v", err)
	}
	if again.Created || again.Cursor != first.Cursor || again.RestoredFrom != 1 {
		t.Fatalf("repeat result = %+v, want created=false cursor=%d", again, first.Cursor)
	}
	rows, next, _ := s.ListChanges("doc", 0, 100)
	if len(rows) != 3 || next != 3 {
		t.Fatalf("idempotent repeat appended: rows=%d next=%d", len(rows), next)
	}
}

func TestRestoreConflicts(t *testing.T) {
	s, _ := app.Open("")
	defer func() { _ = s.Close() }()

	if _, err := s.PostChanges("doc", changes("c1", "c2")); err != nil {
		t.Fatal(err)
	}
	if _, err := s.PutSnapshot("doc", 1, json.RawMessage(`{"v":1}`)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.PutSnapshot("doc", 2, json.RawMessage(`{"v":2}`)); err != nil {
		t.Fatal(err)
	}

	mustConflict := func(name, deviceID, changeID string, snapshotCursor int64) {
		t.Helper()
		before, beforeNext, _ := s.ListChanges("doc", 0, 100)
		_, err := s.RestoreSnapshot("doc", deviceID, changeID, snapshotCursor)
		var conflict *events.ErrRestoreConflict
		if !errors.As(err, &conflict) {
			t.Fatalf("%s: err = %v, want *events.ErrRestoreConflict", name, err)
		}
		after, afterNext, _ := s.ListChanges("doc", 0, 100)
		if len(after) != len(before) || afterNext != beforeNext {
			t.Fatalf("%s wrote rows: before=%d after=%d", name, len(before), len(after))
		}
	}

	// An ordinary change occupies the id.
	mustConflict("ordinary change id", "dev", "c1", 1)

	// A successful restore first.
	if _, err := s.RestoreSnapshot("doc", "dev", "r1", 1); err != nil {
		t.Fatal(err)
	}

	// Different deviceId, different source state/cursor are all 409.
	mustConflict("device mismatch", "other", "r1", 1)
	mustConflict("snapshotCursor mismatch with different state", "dev", "r1", 2)
}

func TestRestoreCursorMismatchWithEqualState(t *testing.T) {
	// A different snapshotCursor is a conflict even when the states decode
	// equal: snapshotCursor is part of the idempotency key.
	s, _ := app.Open("")
	defer func() { _ = s.Close() }()

	if _, err := s.PostChanges("doc", changes("c1", "c2")); err != nil {
		t.Fatal(err)
	}
	if _, err := s.PutSnapshot("doc", 1, json.RawMessage(`{"v":1}`)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.PutSnapshot("doc", 2, json.RawMessage(`{"v":1.0}`)); err != nil {
		t.Fatal(err)
	}

	if _, err := s.RestoreSnapshot("doc", "dev", "r1", 1); err != nil {
		t.Fatal(err)
	}
	if _, err := s.RestoreSnapshot("doc", "dev", "r1", 2); !errors.As(err, new(*events.ErrRestoreConflict)) {
		t.Fatalf("equal state, different snapshotCursor: err = %v, want conflict", err)
	}
}

func TestRestorePersistsAcrossReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sync.db")

	s, err := app.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.PostChanges("doc", changes("c1")); err != nil {
		t.Fatal(err)
	}
	if _, err := s.PutSnapshot("doc", 1, json.RawMessage(`{"v":1}`)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.RestoreSnapshot("doc", "dev", "r1", 1); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s2, err := app.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s2.Close() }()

	// Provenance survives the restart: identical replay is still idempotent.
	r, err := s2.RestoreSnapshot("doc", "dev", "r1", 1)
	if err != nil || r.Created || r.Cursor != 2 || r.RestoredFrom != 1 {
		t.Fatalf("replay after restart = %+v err=%v", r, err)
	}
	// A differing device is still a conflict after restart.
	if _, err := s2.RestoreSnapshot("doc", "other", "r1", 1); !errors.As(err, new(*events.ErrRestoreConflict)) {
		t.Fatalf("conflict after restart err = %v", err)
	}
	// The appended change reads back with the snapshot state.
	rows, next, err := s2.ListChanges("doc", 0, 100)
	if err != nil || len(rows) != 2 || next != 2 || !store.JSONEqual(rows[1].Payload, json.RawMessage(`{"v":1}`)) {
		t.Fatalf("rows after restart = %+v next=%d err=%v", rows, next, err)
	}
}

func TestRestoreConcurrent(t *testing.T) {
	s, _ := app.Open("")
	defer func() { _ = s.Close() }()

	if _, err := s.PostChanges("doc", changes("c1")); err != nil {
		t.Fatal(err)
	}
	if _, err := s.PutSnapshot("doc", 1, json.RawMessage(`{"v":1}`)); err != nil {
		t.Fatal(err)
	}

	const n = 20
	var wg sync.WaitGroup
	errCh := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			r, err := s.RestoreSnapshot("doc", "dev", fmt.Sprintf("r%d", i), 1)
			if err != nil {
				errCh <- err
				return
			}
			if !r.Created || r.RestoredFrom != 1 {
				errCh <- fmt.Errorf("restore %d bad result %+v", i, r)
			}
		}(i)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}

	rows, next, err := s.ListChanges("doc", 0, 1000)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != n+1 || next != n+1 {
		t.Fatalf("rows=%d next=%d want %d/%d", len(rows), next, n+1, n+1)
	}
	seen := map[int64]bool{}
	for _, r := range rows[1:] {
		if seen[r.Cursor] {
			t.Fatalf("duplicate cursor %d", r.Cursor)
		}
		seen[r.Cursor] = true
	}
	for c := int64(2); c <= n+1; c++ {
		if !seen[c] {
			t.Fatalf("cursors not contiguous, missing %d", c)
		}
	}

	// Concurrent identical restores: exactly one creator, rest idempotent.
	var mu sync.Mutex
	creators := 0
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, err := s.RestoreSnapshot("doc", "dev", "same", 1)
			if err != nil {
				t.Error(err)
				return
			}
			if r.Cursor != n+2 {
				t.Errorf("idempotent cursor = %d, want %d", r.Cursor, n+2)
			}
			if r.Created {
				mu.Lock()
				creators++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if creators != 1 {
		t.Fatalf("creators = %d, want 1", creators)
	}
}
