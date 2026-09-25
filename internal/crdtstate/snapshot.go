package crdtstate

// GetSnapshot returns the document's merged CRDT state together with the
// number of stored operations and tombstones the merge is derived from. A
// document with no committed operations yields ErrNotFound (the caller
// answers 404). The read is pure: it advances no cursor, writes nothing and
// registers no subscription.
func (s *Service) GetSnapshot(documentID string) (Snapshot, error) {
	state, err := s.GetState(documentID)
	if err != nil {
		return Snapshot{}, err
	}
	snapshot := Snapshot{Type: state.Type, Value: state.Value}

	count := func(query string) (int64, error) {
		var n int64
		if err := s.db.QueryRow(query, documentID).Scan(&n); err != nil {
			return 0, err
		}
		return n, nil
	}

	switch state.Type {
	case TypeCounter, TypeGSet:
		operations, err := count(`SELECT COUNT(*) FROM crdt_ops WHERE document_id = ?`)
		if err != nil {
			return Snapshot{}, err
		}
		snapshot.Operations = operations
	case TypeRegister:
		operations, err := count(`SELECT COUNT(*) FROM crdt_register_ops WHERE document_id = ?`)
		if err != nil {
			return Snapshot{}, err
		}
		snapshot.Operations = operations
	case TypeORSet:
		operations, err := count(`SELECT COUNT(*) FROM crdt_orset_tags WHERE document_id = ?`)
		if err != nil {
			return Snapshot{}, err
		}
		tombstones, err := count(`SELECT COUNT(*) FROM crdt_orset_tombstones WHERE document_id = ?`)
		if err != nil {
			return Snapshot{}, err
		}
		snapshot.Operations = operations
		snapshot.Tombstones = tombstones
	}
	return snapshot, nil
}

// snapshotLocked is the post-compaction twin of GetSnapshot: the service lock
// is already held (so no submission or compaction can interleave) and the
// commit is durable before it is called.
func (s *Service) snapshotLocked(documentID string) (Snapshot, error) {
	return s.GetSnapshot(documentID)
}
