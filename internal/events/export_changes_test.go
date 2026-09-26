package events_test

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"strconv"
	"sync"
	"testing"

	"github.com/alicegogogogogo/local-first-sync-service/internal/app"
	"github.com/alicegogogogogo/local-first-sync-service/internal/events"
)

func TestExportChangesRange(t *testing.T) {
	s, err := app.Open("")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()

	doc := "doc-export"
	if _, err := s.PostChanges(doc, changes("c1", "c2", "c3", "c4")); err != nil {
		t.Fatal(err)
	}

	// Whole range, no bounds: ascending and verbatim.
	got, err := s.ExportChanges(doc, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 4 {
		t.Fatalf("export len = %d, want 4: %+v", len(got), got)
	}
	for i, c := range got {
		if c.Cursor != int64(i+1) {
			t.Fatalf("item %d cursor = %d, want %d", i, c.Cursor, i+1)
		}
		if c.ID != fmt.Sprintf("c%d", i+1) {
			t.Fatalf("item %d id = %q", i, c.ID)
		}
		if c.DeviceID != "dev-1" {
			t.Fatalf("item %d deviceId = %q", i, c.DeviceID)
		}
	}

	// Closed interval: both endpoints included.
	got, err = s.ExportChanges(doc, 2, ptrInt64(3))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].Cursor != 2 || got[1].Cursor != 3 {
		t.Fatalf("[2,3] = %+v", got)
	}

	// A degenerate interval still includes the single matching cursor.
	got, err = s.ExportChanges(doc, 2, ptrInt64(2))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Cursor != 2 || got[0].ID != "c2" {
		t.Fatalf("[2,2] = %+v", got)
	}

	// Lower bound only.
	got, err = s.ExportChanges(doc, 3, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].Cursor != 3 || got[1].Cursor != 4 {
		t.Fatalf("from=3 = %+v", got)
	}

	// A range without a change is an empty non-nil list, not an error.
	got, err = s.ExportChanges(doc, 99, ptrInt64(200))
	if err != nil {
		t.Fatal(err)
	}
	if got == nil || len(got) != 0 {
		t.Fatalf("empty range = %+v, want empty non-nil", got)
	}

	// An unknown document is likewise an empty list.
	got, err = s.ExportChanges("never-heard-of-it", 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got == nil || len(got) != 0 {
		t.Fatalf("unknown doc = %+v, want empty non-nil", got)
	}
}

func TestExportChangesMatchesPagedRead(t *testing.T) {
	s, err := app.Open("")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()

	doc := "doc-match"
	if _, err := s.PostChanges(doc, changes("c1", "c2", "c3", "c4", "c5")); err != nil {
		t.Fatal(err)
	}

	exported, err := s.ExportChanges(doc, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	// Reassemble the same rows through the paged read and require the export
	// to be byte-identical to it, cursor by cursor.
	paged := make([]struct {
		id      string
		device  string
		payload string
		cursor  int64
	}, 0, len(exported))
	var after int64
	for {
		page, next, err := s.ListChanges(doc, after, 2)
		if err != nil {
			t.Fatal(err)
		}
		for _, c := range page {
			paged = append(paged, struct {
				id      string
				device  string
				payload string
				cursor  int64
			}{c.ID, c.DeviceID, string(c.Payload), c.Cursor})
		}
		if next == after || len(page) == 0 {
			break
		}
		after = next
	}
	if len(paged) != len(exported) {
		t.Fatalf("paged len = %d, export len = %d", len(paged), len(exported))
	}
	for i := range paged {
		c := exported[i]
		if c.Cursor != paged[i].cursor || c.ID != paged[i].id ||
			c.DeviceID != paged[i].device || string(c.Payload) != paged[i].payload {
			t.Fatalf("row %d differs: export id=%s dev=%s payload=%s cursor=%d vs paged %+v",
				i, c.ID, c.DeviceID, c.Payload, c.Cursor, paged[i])
		}
	}
}

func TestExportChangesExcludesCompacted(t *testing.T) {
	s, err := app.Open("")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()

	const doc = "doc-trimmed"
	if _, err := s.RegisterDevice("dev-1"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.PostChanges(doc, changes("c1", "c2", "c3", "c4")); err != nil {
		t.Fatal(err)
	}
	if _, err := s.PutSnapshot(doc, 2, json.RawMessage(`{"s":1}`)); err != nil {
		t.Fatal(err)
	}
	boundary, removed, err := s.CompactChanges(doc, "dev-1")
	if err != nil || boundary != 2 || removed != 2 {
		t.Fatalf("compact = boundary %d removed %d err %v", boundary, removed, err)
	}

	// Unbounded export sees only the online tail.
	got, err := s.ExportChanges(doc, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].Cursor != 3 || got[1].Cursor != 4 {
		t.Fatalf("tail export = %+v", got)
	}

	// An interval entirely inside the trimmed region is an empty success.
	got, err = s.ExportChanges(doc, 0, ptrInt64(2))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("fully trimmed interval = %+v, want empty", got)
	}

	// An interval straddling the boundary returns only its online rows.
	got, err = s.ExportChanges(doc, 1, ptrInt64(3))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Cursor != 3 {
		t.Fatalf("straddling interval = %+v, want cursor 3 only", got)
	}

	// Rows above the trim point are still exported exactly.
	got, err = s.ExportChanges(doc, 4, ptrInt64(4))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].ID != "c4" {
		t.Fatalf("point export = %+v", got)
	}
}

func TestExportChangesIsReadOnly(t *testing.T) {
	s, err := app.Open("")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()

	doc := "doc-ro"
	if _, err := s.PostChanges(doc, changes("c1")); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 3; i++ {
		if _, err := s.ExportChanges(doc, 0, nil); err != nil {
			t.Fatal(err)
		}
	}

	rows, next, err := s.ListChanges(doc, 0, 100)
	if err != nil || len(rows) != 1 || next != 1 {
		t.Fatalf("changes after export = %+v next=%d err=%v", rows, next, err)
	}
}

func TestExportChangesConcurrentBatch(t *testing.T) {
	s, err := app.Open("")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()

	doc := "doc-conc"
	// Each writer posts batches of ten; one batch is one serialized
	// transaction, so an export must observe each batch either with all ten
	// rows on contiguous cursors or with none.
	const writers, batches, size = 8, 5, 10

	batchChanges := func(w, b int) []events.Change {
		items := make([]events.Change, size)
		for j := range items {
			items[j] = events.Change{
				ID:       fmt.Sprintf("w%d-b%d-c%d", w, b, j),
				DeviceID: "dev-1",
				Payload:  json.RawMessage(fmt.Sprintf(`{"n":%d}`, w*1000+b*10+j)),
			}
		}
		return items
	}

	var wg sync.WaitGroup
	wg.Add(writers)
	for w := 0; w < writers; w++ {
		go func(w int) {
			defer wg.Done()
			for b := 0; b < batches; b++ {
				if _, err := s.PostChanges(doc, batchChanges(w, b)); err != nil {
					t.Errorf("writer %d batch %d: %v", w, b, err)
					return
				}
			}
		}(w)
	}

	// Concurrently export: every result is self-consistent — ascending,
	// unique cursors, complete JSON payloads — and every observed batch is
	// either wholly present or wholly absent.
	for i := 0; i < 200; i++ {
		got, err := s.ExportChanges(doc, 0, nil)
		if err != nil {
			t.Fatal(err)
		}
		byBatch := map[string]int{}
		var prev int64
		for _, c := range got {
			if c.Cursor <= prev {
				t.Fatalf("non-ascending/duplicate cursor %d after %d", c.Cursor, prev)
			}
			prev = c.Cursor
			var m map[string]any
			if err := json.Unmarshal(c.Payload, &m); err != nil || m["n"] == nil {
				t.Fatalf("half/corrupt row at cursor %d: %s", c.Cursor, c.Payload)
			}
			var w, b, j int
			if _, err := fmt.Sscanf(c.ID, "w%d-b%d-c%d", &w, &b, &j); err != nil {
				t.Fatalf("unexpected id %q", c.ID)
			}
			byBatch[strconv.Itoa(w)+"-"+strconv.Itoa(b)]++
		}
		for key, n := range byBatch {
			if n != size {
				t.Fatalf("batch %s half-visible: %d of %d rows", key, n, size)
			}
		}
	}
	wg.Wait()

	got, err := s.ExportChanges(doc, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != writers*batches*size {
		t.Fatalf("final export len = %d, want %d", len(got), writers*batches*size)
	}
}

func TestExportChangesPersistenceAcrossRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "export-changes.db")

	s, err := app.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	doc := "doc-restart"
	if _, err := s.PostChanges(doc, changes("c1", "c2", "c3")); err != nil {
		t.Fatal(err)
	}

	before, err := s.ExportChanges(doc, 0, nil)
	if err != nil {
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

	after, err := s2.ExportChanges(doc, 1, ptrInt64(3))
	if err != nil {
		t.Fatal(err)
	}
	if len(before) != len(after) {
		t.Fatalf("after restart len = %d, want %d", len(after), len(before))
	}
	for i := range before {
		if before[i].Cursor != after[i].Cursor ||
			before[i].ID != after[i].ID ||
			before[i].DeviceID != after[i].DeviceID ||
			string(before[i].Payload) != string(after[i].Payload) {
			t.Fatalf("row %d changed across restart: %+v vs %+v", i, before[i], after[i])
		}
	}
}
