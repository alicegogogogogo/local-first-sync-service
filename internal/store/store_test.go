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

func mergeChange(id, device string, payload string) Change {
	return Change{ID: id, DeviceID: device, Payload: json.RawMessage(payload)}
}

func TestMergeAppliedNewDocument(t *testing.T) {
	s, _ := Open("")
	defer func() { _ = s.Close() }()

	res, err := s.MergeChange("doc", "dev", 0, mergeChange("c1", "dev", `{"a":1}`))
	if err != nil {
		t.Fatal(err)
	}
	if res.Outcome != MergeApplied || res.Cursor != 1 || res.Result != nil {
		t.Fatalf("result = %+v", res)
	}

	// Base at the current high-water mark applies again.
	res, err = s.MergeChange("doc", "dev", 1, mergeChange("c2", "dev", `{"b":2}`))
	if err != nil || res.Outcome != MergeApplied || res.Cursor != 2 {
		t.Fatalf("at-head result = %+v err=%v", res, err)
	}
}

func TestMergeUnknownDocumentRejectsNonZeroBase(t *testing.T) {
	s, _ := Open("")
	defer func() { _ = s.Close() }()

	_, err := s.MergeChange("ghost", "dev", 1, mergeChange("c1", "dev", `{"a":1}`))
	var bad *ErrInvalidCursor
	if !errors.As(err, &bad) || bad.BaseCursor != 1 || bad.CurrentCursor != 0 {
		t.Fatalf("want ErrInvalidCursor, got %v", err)
	}
	rows, _, _ := s.ListChanges("ghost", 0, 10)
	if len(rows) != 0 {
		t.Fatalf("unknown-doc rejection wrote rows: %+v", rows)
	}
}

func TestMergeBaseBeyondCurrentRejected(t *testing.T) {
	s, _ := Open("")
	defer func() { _ = s.Close() }()
	if _, err := s.PostChanges("doc", changes("a")); err != nil {
		t.Fatal(err)
	}
	_, err := s.MergeChange("doc", "dev", 5, mergeChange("c", "dev", `{"a":1}`))
	var bad *ErrInvalidCursor
	if !errors.As(err, &bad) {
		t.Fatalf("want ErrInvalidCursor, got %v", err)
	}
}

func TestMergeIdempotentExisting(t *testing.T) {
	s, _ := Open("")
	defer func() { _ = s.Close() }()

	if _, err := s.PostChanges("doc", []Change{
		{ID: "x", DeviceID: "dev", Payload: json.RawMessage(`{"k":"v"}`)},
	}); err != nil {
		t.Fatal(err)
	}

	// Same device/payload, lagging base: still idempotent, original result.
	res, err := s.MergeChange("doc", "dev", 0, mergeChange("x", "dev", `{ "k": "v" }`))
	if err != nil {
		t.Fatal(err)
	}
	if res.Outcome != MergeIdempotent || res.Cursor != 1 || res.Result == nil {
		t.Fatalf("idempotent result = %+v", res)
	}
	if res.Result.Created || res.Result.Cursor != 1 || res.Result.ID != "x" {
		t.Fatalf("embedded result = %+v", res.Result)
	}

	// Different payload -> conflict, even with current base.
	_, err = s.MergeChange("doc", "dev", 1, mergeChange("x", "dev", `{"k":"other"}`))
	var conflict *ErrMergeConflict
	if !errors.As(err, &conflict) || conflict.ID != "x" {
		t.Fatalf("payload mismatch want ErrMergeConflict{x}, got %v", err)
	}
	// Different device -> conflict.
	_, err = s.MergeChange("doc", "other", 1, mergeChange("x", "other", `{"k":"v"}`))
	if !errors.As(err, &conflict) {
		t.Fatalf("device mismatch want ErrMergeConflict, got %v", err)
	}

	rows, _, _ := s.ListChanges("doc", 0, 100)
	if len(rows) != 1 {
		t.Fatalf("conflict paths wrote rows: %+v", rows)
	}
}

func TestMergeLaggardDisjointFieldsMerged(t *testing.T) {
	s, _ := Open("")
	defer func() { _ = s.Close() }()

	// Current state: c1 {a}, c2 {b} at cursors 1,2. Client saw cursor 0 and
	// sends {c}; no key collisions with intervening changes -> merged at 3.
	if _, err := s.PostChanges("doc", []Change{
		{ID: "c1", DeviceID: "dev", Payload: json.RawMessage(`{"a":1}`)},
		{ID: "c2", DeviceID: "dev", Payload: json.RawMessage(`{"b":2}`)},
	}); err != nil {
		t.Fatal(err)
	}
	res, err := s.MergeChange("doc", "dev", 0, mergeChange("c3", "dev", `{"c":3}`))
	if err != nil {
		t.Fatalf("disjoint merge: %v", err)
	}
	if res.Outcome != MergeMerged || res.Cursor != 3 {
		t.Fatalf("merged result = %+v", res)
	}
}

func TestMergeLaggardCollisionRejectedZeroWrite(t *testing.T) {
	s, _ := Open("")
	defer func() { _ = s.Close() }()

	if _, err := s.PostChanges("doc", []Change{
		{ID: "c1", DeviceID: "dev", Payload: json.RawMessage(`{"a":1}`)},
		{ID: "c2", DeviceID: "dev", Payload: json.RawMessage(`{"nested":{"x":1},"b":2}`)},
	}); err != nil {
		t.Fatal(err)
	}

	// Top-level key "a" collides with intervening c1.
	_, err := s.MergeChange("doc", "dev", 0, mergeChange("c3", "dev", `{"a":99,"z":1}`))
	var conflict *ErrMergeConflict
	if !errors.As(err, &conflict) {
		t.Fatalf("collision want ErrMergeConflict, got %v", err)
	}
	rows, _, _ := s.ListChanges("doc", 0, 100)
	if len(rows) != 2 {
		t.Fatalf("collision wrote rows: %+v", rows)
	}

	// Only changes strictly after baseCursor count: base 1 skips c1, so an
	// "a" field now merges (c2's keys are nested/b).
	res, err := s.MergeChange("doc", "dev", 1, mergeChange("c4", "dev", `{"a":99}`))
	if err != nil || res.Outcome != MergeMerged || res.Cursor != 3 {
		t.Fatalf("post-base merge = %+v err=%v", res, err)
	}
}

func TestMergeLaggardNonObjectInterveningRejected(t *testing.T) {
	s, _ := Open("")
	defer func() { _ = s.Close() }()

	if _, err := s.PostChanges("doc", []Change{
		{ID: "c1", DeviceID: "dev", Payload: json.RawMessage(`{"a":1}`)},
		{ID: "c2", DeviceID: "dev", Payload: json.RawMessage(`[1,2,3]`)},
	}); err != nil {
		t.Fatal(err)
	}

	_, err := s.MergeChange("doc", "dev", 0, mergeChange("c3", "dev", `{"z":1}`))
	var conflict *ErrMergeConflict
	if !errors.As(err, &conflict) {
		t.Fatalf("non-object intervening want ErrMergeConflict, got %v", err)
	}
	rows, _, _ := s.ListChanges("doc", 0, 100)
	if len(rows) != 2 {
		t.Fatalf("rejected merge wrote rows: %+v", rows)
	}
}

func TestMergeRejectsNonObjectPayload(t *testing.T) {
	s, _ := Open("")
	defer func() { _ = s.Close() }()
	for _, payload := range []string{`[1]`, `"x"`, `1`, `true`, `null`} {
		if _, err := s.MergeChange("doc", "dev", 0, mergeChange("c", "dev", payload)); err == nil {
			t.Fatalf("payload %s should be rejected", payload)
		}
	}
}

func TestMergePersistenceAcrossReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sync.db")

	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.PostChanges("doc", []Change{
		{ID: "c1", DeviceID: "dev", Payload: json.RawMessage(`{"a":1}`)},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.MergeChange("doc", "dev", 0, mergeChange("c2", "dev", `{"b":2}`)); err != nil {
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
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 || next != 2 {
		t.Fatalf("after reopen rows = %+v next=%d", rows, next)
	}

	// Cursor 3 follows the persisted high-water mark; unknown-doc rule
	// survives restart too.
	res, err := s2.MergeChange("doc", "dev", 2, mergeChange("c3", "dev", `{"c":3}`))
	if err != nil || res.Outcome != MergeApplied || res.Cursor != 3 {
		t.Fatalf("post-restart apply = %+v err=%v", res, err)
	}
	if _, err := s2.MergeChange("brand-new", "dev", 1, mergeChange("x", "dev", `{"a":1}`)); err == nil {
		t.Fatalf("unknown doc non-zero base must be rejected after restart")
	}
}

func TestConcurrentMergesNoLostOrDuplicatedRows(t *testing.T) {
	s, _ := Open("")
	defer func() { _ = s.Close() }()

	const goroutines = 24
	var wg sync.WaitGroup
	errCh := make(chan error, goroutines)
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			// Every writer believes it is at cursor 0 with a unique field.
			// Field disjointness is independent of commit order, so each must
			// merge exactly once with a unique cursor.
			ch := Change{
				ID:       fmt.Sprintf("m%d", g),
				DeviceID: "dev",
				Payload:  json.RawMessage(fmt.Sprintf(`{"f%d":%d}`, g, g)),
			}
			res, err := s.MergeChange("doc", "dev", 0, ch)
			if err != nil {
				errCh <- fmt.Errorf("merge %d: %w", g, err)
				return
			}
			if res.Outcome != MergeMerged && res.Outcome != MergeApplied {
				errCh <- fmt.Errorf("merge %d unexpected outcome %s", g, res.Outcome)
			}
		}(g)
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
	if len(rows) != goroutines || next != goroutines {
		t.Fatalf("rows=%d next=%d want %d/%d", len(rows), next, goroutines, goroutines)
	}
	seen := make(map[int64]string)
	for _, r := range rows {
		if prev, dup := seen[r.Cursor]; dup {
			t.Fatalf("cursor %d reused by %s and %s", r.Cursor, prev, r.ID)
		}
		seen[r.Cursor] = r.ID
	}
}

func TestConcurrentMergesCollisionStillRejected(t *testing.T) {
	s, _ := Open("")
	defer func() { _ = s.Close() }()

	if _, err := s.PostChanges("doc", []Change{
		{ID: "seed", DeviceID: "dev", Payload: json.RawMessage(`{"a":1}`)},
	}); err != nil {
		t.Fatal(err)
	}

	const goroutines = 16
	var wg sync.WaitGroup
	applied := make(chan int64, goroutines)
	rejected := 0
	var mu sync.Mutex
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			// All laggards touch field "a": at most one may succeed, and even
			// that only if it commits while no later "a" exists — since the
			// seed already has "a" after base 0, every one must be rejected.
			_, err := s.MergeChange("doc", "dev", 0, Change{
				ID:       fmt.Sprintf("collide%d", g),
				DeviceID: "dev",
				Payload:  json.RawMessage(`{"a":2}`),
			})
			mu.Lock()
			defer mu.Unlock()
			if err == nil {
				applied <- 1
			} else {
				rejected++
			}
		}(g)
	}
	wg.Wait()
	close(applied)
	for range applied {
		t.Fatalf("laggard merge over existing field a must never apply")
	}
	if rejected != goroutines {
		t.Fatalf("rejected=%d want %d", rejected, goroutines)
	}
	rows, _, _ := s.ListChanges("doc", 0, 1000)
	if len(rows) != 1 {
		t.Fatalf("collision merges wrote rows: %+v", rows)
	}
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
