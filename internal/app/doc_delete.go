package app

import (
	"github.com/alicegogogogogo/local-first-sync-service/internal/crdt"
	"github.com/alicegogogogogo/local-first-sync-service/internal/events"
)

// DeleteDocument removes one document's whole durable footprint in a single
// serialized transaction and then ends its live subscriptions. The calling
// device is declared by the HTTP layer (the document resource path plus a
// query parameter), so this boundary introduces no credential of its own.
//
// The fixed verdict order matches every other gated endpoint:
//
//   - an unregistered device yields store.ErrDeviceNotFound and writes nothing;
//   - a device whose permission for the document was revoked yields
//     store.ErrPermissionDenied and writes nothing;
//   - a document with no durable trace in any layer — it was never created, or
//     was already deleted — yields events.ErrDocumentNotFound and writes
//     nothing.
//
// On a hit the transaction removes, together and atomically: the online and
// compacted-away (retained-summary) change log, snapshots and restore
// provenance and the change compaction boundary; every CRDT table including
// the type row and retained operation identities; and the document's whole
// permission ledger. The change cursor space is consequently reset for the
// name: a same-named document created afterward is brand new, allocating its
// first cursor at 1 and inheriting no history. After the commit every live
// subscription on the document across both push channels ends immediately
// with the document-deleted close; other documents and other devices are
// untouched.
func (a *App) DeleteDocument(deviceID, documentID string) error {
	tx, err := a.Store.DB().Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	// Gate first, exactly as the change and CRDT services do inside their own
	// transactions: an unregistered or revoked device never observes whether
	// the document exists and the wipe cannot start.
	if err := a.Authz.AuthorizedTx(tx, documentID, deviceID); err != nil {
		return err
	}

	// Existence is the union of all three layers' durable rows: a document
	// with CRDT state but no change log, or only a permission deviation row, is
	// still known and must delete successfully.
	changeExists, err := events.DocumentExistsTx(tx, documentID)
	if err != nil {
		return err
	}
	crdtExists, err := crdt.DocumentExistsTx(tx, documentID)
	if err != nil {
		return err
	}
	permissionExists, err := a.Authz.DocumentExistsTx(tx, documentID)
	if err != nil {
		return err
	}
	if !changeExists && !crdtExists && !permissionExists {
		return events.ErrDocumentNotFound
	}

	if err := events.DeleteDocumentTx(tx, documentID); err != nil {
		return err
	}
	if err := crdt.DeleteDocumentTx(tx, documentID); err != nil {
		return err
	}
	if err := a.Authz.DeleteDocumentPermissionsTx(tx, documentID); err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	// Only after the wipe is durably committed do the live connections end: a
	// reconnect racing the signal reads the already-empty document and can
	// never observe a half-cleared one.
	a.events.SignalDocumentDeleted(documentID)
	a.crdt.SignalDocumentDeleted(documentID)
	return nil
}
