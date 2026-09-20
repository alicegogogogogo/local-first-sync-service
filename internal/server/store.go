package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"sync"
)

// ErrConflict is returned when a batch contains an id that already exists
// with a different deviceId or payload.
var ErrConflict = errors.New("change conflicts with existing record")

// Change is a single persisted mutation of a document.
type Change struct {
	ID       string          `json:"id"`
	DeviceID string          `json:"deviceId"`
	Payload  json.RawMessage `json:"payload"`
	Cursor   int64           `json:"cursor"`
}

// ChangeInput is a validated change submitted in a POST batch.
type ChangeInput struct {
	ID      string
	Payload json.RawMessage
}

// ApplyResult describes the outcome for one submitted change id.
type ApplyResult struct {
	ID      string `json:"id"`
	Created bool   `json:"created"`
	Cursor  int64  `json:"cursor"`
}

type document struct {
	NextCursor int64    `json:"nextCursor"`
	Changes    []Change `json:"changes"`
	byID       map[string]int
}

// Store persists document changes and cursors. When path is empty the store
// is in-memory only; otherwise state is loaded from and atomically written
// to path after every applied batch.
type Store struct {
	mu   sync.Mutex
	path string
	docs map[string]*document
}

// NewStore opens the store at path (empty path means in-memory only),
// loading any previously persisted state.
func NewStore(path string) (*Store, error) {
	s := &Store{path: path, docs: map[string]*document{}}
	if path == "" {
		return s, nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("create state dir: %w", err)
	}
	data, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		return s, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read state: %w", err)
	}
	var docs map[string]*document
	if err := json.Unmarshal(data, &docs); err != nil {
		return nil, fmt.Errorf("decode state: %w", err)
	}
	for _, doc := range docs {
		doc.byID = make(map[string]int, len(doc.Changes))
		for i, c := range doc.Changes {
			doc.byID[c.ID] = i
		}
	}
	s.docs = docs
	return s, nil
}

// Apply validates a batch against the current state and, if valid, appends
// the new changes atomically and persists them. Ids that already exist with
// the same deviceId and an equal decoded payload are idempotent hits; any
// other duplicate aborts the whole batch with ErrConflict and writes nothing.
func (s *Store) Apply(documentID, deviceID string, items []ChangeInput) ([]ApplyResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	doc := s.docs[documentID]
	cursor := int64(0)
	if doc != nil {
		cursor = doc.NextCursor
	}

	results := make([]ApplyResult, len(items))
	for i, it := range items {
		if doc != nil {
			if idx, ok := doc.byID[it.ID]; ok {
				existing := doc.Changes[idx]
				if existing.DeviceID != deviceID || !jsonValueEqual(existing.Payload, it.Payload) {
					return nil, ErrConflict
				}
				results[i] = ApplyResult{ID: it.ID, Created: false, Cursor: existing.Cursor}
				continue
			}
		}
		cursor++
		results[i] = ApplyResult{ID: it.ID, Created: true, Cursor: cursor}
	}

	if doc == nil {
		doc = &document{byID: map[string]int{}}
		s.docs[documentID] = doc
	}
	for i, it := range items {
		if !results[i].Created {
			continue
		}
		doc.byID[it.ID] = len(doc.Changes)
		doc.Changes = append(doc.Changes, Change{
			ID:       it.ID,
			DeviceID: deviceID,
			Payload:  it.Payload,
			Cursor:   results[i].Cursor,
		})
	}
	doc.NextCursor = cursor

	if err := s.persistLocked(); err != nil {
		return nil, fmt.Errorf("persist state: %w", err)
	}
	return results, nil
}

// List returns up to limit changes of the document with cursor > after,
// ordered by cursor ascending, plus the cursor to resume from. Unknown
// documents yield an empty slice and nextCursor 0; known documents with no
// matching changes yield nextCursor == after.
func (s *Store) List(documentID string, after int64, limit int) ([]Change, int64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	doc := s.docs[documentID]
	if doc == nil {
		return []Change{}, 0
	}
	out := make([]Change, 0, limit)
	for _, c := range doc.Changes {
		if c.Cursor <= after {
			continue
		}
		out = append(out, c)
		if len(out) == limit {
			break
		}
	}
	next := after
	if len(out) > 0 {
		next = out[len(out)-1].Cursor
	}
	return out, next
}

func (s *Store) persistLocked() error {
	if s.path == "" {
		return nil
	}
	data, err := json.Marshal(s.docs)
	if err != nil {
		return err
	}
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, s.path)
}

func jsonValueEqual(a, b json.RawMessage) bool {
	var av, bv any
	if err := json.Unmarshal(a, &av); err != nil {
		return false
	}
	if err := json.Unmarshal(b, &bv); err != nil {
		return false
	}
	return reflect.DeepEqual(av, bv)
}
