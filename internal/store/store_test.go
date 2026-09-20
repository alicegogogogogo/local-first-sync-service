package store

import (
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"sync"
	"testing"
)

func changes(ids ...string) []Change {
	out := make([]Change, len(ids))
	for i, id := range ids {
		out[i] = Change{ID: id, DeviceID: "dev-1", Payload: json.RawMessage(fmt.Sprintf(`{"n":%d}`, i+1))}
	}
	return out
}

func TestPostAndListBasic(t *testing.T) {
	s, err := Open("")
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
	s, _ := Open("")
	defer func() { _ = s.Close() }()

	first, err := s.PostChanges("doc", []Change{
		{ID: "x1", DeviceID: "dev", Payload: json.RawMessage(`{"a":1,"b":2}`)},
	})
	if err != nil {
		t.Fatal(err)
	}

	// Same deviceId and semantically equal payload (reordered keys, whitespace):
	// idempotent, cursor stays the first value.
	again, err := s.PostChanges("doc", []Change{
		{ID: "x1", DeviceID: "dev", Payload: json.RawMessage(`{ "b": 2, "a": 1 }`)},
	})
	if err != nil {
		t.Fatalf("idempotent repost: %v", err)
	}
	if again[0].Created || again[0].Cursor != first[0].Cursor || again[0].Cursor != 1 {
		t.Fatalf("repost result = %+v, want created=false cursor=1", again[0])
	}

	// Number vs number-with-fraction decode identically.
	again2, err := s.PostChanges("doc", []Change{
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
	s, _ := Open("")
	defer func() { _ = s.Close() }()

	if _, err := s.PostChanges("doc", []Change{
		{ID: "x1", DeviceID: "dev", Payload: json.RawMessage(`{"v":1}`)},
	}); err != nil {
		t.Fatal(err)
	}

	// Different payload -> 409-equivalent conflict, and the new id in the same
	// batch must not be written.
	_, err := s.PostChanges("doc", []Change{
		{ID: "new-1", DeviceID: "dev", Payload: json.RawMessage(`{"v":2}`)},
		{ID: "x1", DeviceID: "dev", Payload: json.RawMessage(`{"v":99}`)},
	})
	var conflict *ErrConflict
	if !errors.As(err, &conflict) || conflict.ID != "x1" {
		t.Fatalf("want *ErrConflict{x1}, got %v", err)
	}
	rows, _, err := s.ListChanges("doc", 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].ID != "x1" {
		t.Fatalf("batch was not zero-write: %+v", rows)
	}

	// Different deviceId -> conflict as well.
	_, err = s.PostChanges("doc", []Change{
		{ID: "x1", DeviceID: "other", Payload: json.RawMessage(`{"v":1}`)},
	})
	if !errors.As(err, &conflict) {
		t.Fatalf("device mismatch want conflict, got %v", err)
	}
}

func TestListPaginationAndUnknownDocument(t *testing.T) {
	s, _ := Open("")
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
	s, _ := Open("")
	defer func() { _ = s.Close() }()

	r1, _ := s.PostChanges("doc1", changes("a"))
	r2, _ := s.PostChanges("doc2", changes("a"))
	if r1[0].Cursor != 1 || r2[0].Cursor != 1 {
		t.Fatalf("cursors should restart per document: %d %d", r1[0].Cursor, r2[0].Cursor)
	}

	// Same id with different payload in another document: that document has
	// its own row and must conflict there.
	r3, err := s.PostChanges("doc2", []Change{{ID: "a", DeviceID: "dev-1", Payload: json.RawMessage(`{"n":2}`)}})
	if err == nil || r3 != nil {
		t.Fatalf("same doc id with different payload should conflict")
	}
}

func TestPersistenceAcrossReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sync.db")

	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.PostChanges("doc", []Change{
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
	s2, err := Open(path)
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
	replay, err := s2.PostChanges("doc", []Change{
		{ID: "k1", DeviceID: "dev", Payload: json.RawMessage(`{"hello":"world"}`)},
	})
	if err != nil || replay[0].Created || replay[0].Cursor != 1 {
		t.Fatalf("replay after restart = %+v err=%v", replay, err)
	}

	more, err := s2.PostChanges("doc", []Change{
		{ID: "k3", DeviceID: "dev", Payload: json.RawMessage(`true`)},
	})
	if err != nil || !more[0].Created || more[0].Cursor != 3 {
		t.Fatalf("new cursor after restart = %+v err=%v", more, err)
	}
}

func TestConcurrentBatchesNoDuplicateCursors(t *testing.T) {
	s, _ := Open("")
	defer func() { _ = s.Close() }()

	const goroutines = 16
	const perBatch = 25

	var wg sync.WaitGroup
	errCh := make(chan error, goroutines)
	var mu sync.Mutex
	all := make([]Result, 0, goroutines*perBatch)

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			batch := make([]Change, perBatch)
			for i := range batch {
				id := fmt.Sprintf("g%d-i%d", g, i)
				batch[i] = Change{ID: id, DeviceID: "dev", Payload: json.RawMessage(fmt.Sprintf(`{"g":%d,"i":%d}`, g, i))}
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
	s, _ := Open("")
	defer func() { _ = s.Close() }()

	// Unknown document, baseCursor 0 -> applied at cursor 1.
	r, err := s.MergeChange("doc", 0, Change{ID: "c1", DeviceID: "dev", Payload: json.RawMessage(`{"a":1}`)})
	if err != nil {
		t.Fatal(err)
	}
	if r.Outcome != "applied" || r.Cursor != 1 || r.Result != nil {
		t.Fatalf("first merge = %+v", r)
	}

	// baseCursor == current -> applied.
	r, err = s.MergeChange("doc", 1, Change{ID: "c2", DeviceID: "dev", Payload: json.RawMessage(`{"b":2}`)})
	if err != nil {
		t.Fatal(err)
	}
	if r.Outcome != "applied" || r.Cursor != 2 {
		t.Fatalf("second merge = %+v", r)
	}
}

func TestMergeUnknownDocRejectsNonZeroBase(t *testing.T) {
	s, _ := Open("")
	defer func() { _ = s.Close() }()

	_, err := s.MergeChange("ghost", 1, Change{ID: "c", DeviceID: "dev", Payload: json.RawMessage(`{"a":1}`)})
	if !errors.Is(err, ErrStaleCursor) {
		t.Fatalf("want ErrStaleCursor, got %v", err)
	}

	// baseCursor ahead of the current cursor is also 400-class.
	if _, err := s.MergeChange("doc", 0, Change{ID: "c1", DeviceID: "dev", Payload: json.RawMessage(`{"a":1}`)}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.MergeChange("doc", 5, Change{ID: "c2", DeviceID: "dev", Payload: json.RawMessage(`{"b":2}`)}); !errors.Is(err, ErrStaleCursor) {
		t.Fatalf("ahead cursor want ErrStaleCursor, got %v", err)
	}
}

func TestMergeIdempotentExisting(t *testing.T) {
	s, _ := Open("")
	defer func() { _ = s.Close() }()

	if _, err := s.MergeChange("doc", 0, Change{ID: "c1", DeviceID: "dev", Payload: json.RawMessage(`{"a":1}`)}); err != nil {
		t.Fatal(err)
	}

	// Repost identical id/payload/device, even with a stale base, is
	// idempotent and returns the original result.
	r, err := s.MergeChange("doc", 0, Change{ID: "c1", DeviceID: "dev", Payload: json.RawMessage(`{ "a": 1 }`)})
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
	if _, err := s.MergeChange("doc", 1, Change{ID: "c1", DeviceID: "dev", Payload: json.RawMessage(`{"a":2}`)}); !errors.As(err, new(*ErrConflict)) {
		t.Fatalf("payload mismatch want conflict, got %v", err)
	}
	// Mismatched device -> conflict.
	if _, err := s.MergeChange("doc", 1, Change{ID: "c1", DeviceID: "other", Payload: json.RawMessage(`{"a":1}`)}); !errors.As(err, new(*ErrConflict)) {
		t.Fatalf("device mismatch want conflict, got %v", err)
	}
}

func TestMergeBehindDisjointObjects(t *testing.T) {
	s, _ := Open("")
	defer func() { _ = s.Close() }()

	// cursor 1: {"a":1}, cursor 2: {"b":2}
	if _, err := s.PostChanges("doc", []Change{
		{ID: "a", DeviceID: "dev", Payload: json.RawMessage(`{"a":1}`)},
		{ID: "b", DeviceID: "dev", Payload: json.RawMessage(`{"b":2}`)},
	}); err != nil {
		t.Fatal(err)
	}

	// Client saw baseCursor 1 (only "a"); new payload {"c":3} shares no
	// top-level keys with the later {"b":2} -> merged at cursor 3.
	r, err := s.MergeChange("doc", 1, Change{ID: "c", DeviceID: "dev", Payload: json.RawMessage(`{"c":3}`)})
	if err != nil {
		t.Fatalf("merge: %v", err)
	}
	if r.Outcome != "merged" || r.Cursor != 3 {
		t.Fatalf("merge result = %+v", r)
	}
}

func TestMergeBehindConflicts(t *testing.T) {
	// Later change reuses a top-level key -> 409, zero write.
	s, _ := Open("")
	defer func() { _ = s.Close() }()
	if _, err := s.PostChanges("doc", []Change{
		{ID: "a", DeviceID: "dev", Payload: json.RawMessage(`{"a":1}`)},
		{ID: "b", DeviceID: "dev", Payload: json.RawMessage(`{"b":2}`)},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.MergeChange("doc", 0, Change{ID: "x", DeviceID: "dev", Payload: json.RawMessage(`{"b":9}`)}); !errors.As(err, new(*ErrConflict)) {
		t.Fatalf("key clash want conflict, got %v", err)
	}
	rows, next, _ := s.ListChanges("doc", 0, 100)
	if len(rows) != 2 || next != 2 {
		t.Fatalf("conflict merge wrote a row: %+v next=%d", rows, next)
	}

	// A disjoint-object merge against the same doc is allowed (cursor 3),
	// proving the earlier 409 was specifically about the key clash.
	if r, err := s.MergeChange("doc", 0, Change{ID: "y", DeviceID: "dev", Payload: json.RawMessage(`{"z":3}`)}); err != nil || r.Outcome != "merged" || r.Cursor != 3 {
		t.Fatalf("disjoint merge = %+v err=%v", r, err)
	}

	// Seed a non-object later payload in a fresh doc to prove the rule.
	s2, _ := Open("")
	defer func() { _ = s2.Close() }()
	if _, err := s2.PostChanges("d2", []Change{
		{ID: "arr", DeviceID: "dev", Payload: json.RawMessage(`[1,2,3]`)},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := s2.MergeChange("d2", 0, Change{ID: "o", DeviceID: "dev", Payload: json.RawMessage(`{"k":1}`)}); !errors.As(err, new(*ErrConflict)) {
		t.Fatalf("array later want conflict, got %v", err)
	}
}

func TestMergeConcurrentSerialization(t *testing.T) {
	s, _ := Open("")
	defer func() { _ = s.Close() }()
	// Seed current cursor so most merges race on the same high-water mark.
	if _, err := s.MergeChange("doc", 0, Change{ID: "seed", DeviceID: "dev", Payload: json.RawMessage(`{"seed":0}`)}); err != nil {
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
			_, err := s.MergeChange("doc", 1, Change{
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

	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.MergeChange("doc", 0, Change{ID: "c1", DeviceID: "dev", Payload: json.RawMessage(`{"a":1}`)}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.MergeChange("doc", 1, Change{ID: "c2", DeviceID: "dev", Payload: json.RawMessage(`{"b":2}`)}); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s2, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s2.Close() }()

	rows, next, err := s2.ListChanges("doc", 0, 100)
	if err != nil || len(rows) != 2 || next != 2 {
		t.Fatalf("after reopen rows=%+v next=%d err=%v", rows, next, err)
	}

	// A merge that was valid before restart still appends at the right cursor.
	r, err := s2.MergeChange("doc", 2, Change{ID: "c3", DeviceID: "dev", Payload: json.RawMessage(`{"c":3}`)})
	if err != nil || r.Outcome != "applied" || r.Cursor != 3 {
		t.Fatalf("post-restart merge = %+v err=%v", r, err)
	}
}
