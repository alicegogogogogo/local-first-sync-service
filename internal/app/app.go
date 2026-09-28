// Package app is the composition root: it opens the one shared database and
// constructs the three independently reusable internal services — the change
// event service, the permission service and the CRDT state service — wiring
// them together only through their internal interfaces.
//
// The services share one SQLite connection (the kernel opens a single
// pooled connection with immediate transactions), so a change commit, a CRDT
// submission or compaction, and a permission change are serialized on the
// same connection and leave no half-written batch. The change event and CRDT
// services take their registration-and-authorization verdict from the
// permission service inside their own transactions (events.Gate / crdt.Gate),
// and the permission service fans a committed revoke out to their
// subscription registries through authz.RevokeSink. No service reaches into
// another service's tables or fields.
//
// App is the single object the entry and HTTP layers use. It promotes each
// service's input/output boundary as its methods while hiding the wiring; it
// adds no authentication and exposes no endpoint of its own.
package app

import (
	"context"
	"encoding/json"
	"time"

	"github.com/alicegogogogogo/local-first-sync-service/internal/authz"
	"github.com/alicegogogogogo/local-first-sync-service/internal/crdt"
	"github.com/alicegogogogogo/local-first-sync-service/internal/events"
	"github.com/alicegogogogogo/local-first-sync-service/internal/push"
	"github.com/alicegogogogogo/local-first-sync-service/internal/store"
)

// App composes the durable kernel and the three internal services over one
// shared database connection.
type App struct {
	*store.Store

	events *events.Service
	crdt   *crdt.Service

	// pushSubs is the process-wide, in-memory registry of the live push
	// connections across both channels. It backs the device-scoped
	// subscription management endpoints and holds no durable state.
	pushSubs *push.Registry

	// Authz is the permission service. It is exposed so the HTTP layer can
	// grant/revoke and read authorization directly; the other services reach
	// it through their gates rather than this field.
	Authz *authz.Service
}

// Open opens (creating if needed) the SQLite database at path and constructs
// every internal service over it. The empty path opens a private in-memory
// database useful for tests. A construction failure closes the kernel and
// leaves nothing behind.
func Open(path string) (*App, error) {
	kernel, err := store.Open(path)
	if err != nil {
		return nil, err
	}

	// The permission service is built first: its gate is a constructor
	// dependency of the change event and CRDT services.
	permissions, err := authz.New(kernel)
	if err != nil {
		_ = kernel.Close()
		return nil, err
	}
	eventService, err := events.New(kernel, permissions)
	if err != nil {
		_ = kernel.Close()
		return nil, err
	}
	crdtService, err := crdt.New(kernel, permissions)
	if err != nil {
		_ = kernel.Close()
		return nil, err
	}

	// A committed revoke ends the affected live subscriptions in both
	// push channels. The dependency is an interface, registered once here, so
	// the permission service never names either subscriber concretely.
	permissions.AddRevokeSink(eventsRevokeSink{eventService})
	permissions.AddRevokeSink(crdtService)

	return &App{
		Store:    kernel,
		events:   eventService,
		crdt:     crdtService,
		Authz:    permissions,
		pushSubs: push.NewRegistry(),
	}, nil
}

// eventsRevokeSink adapts the change event service's revoke hook to the
// permission service's RevokeSink interface.
type eventsRevokeSink struct{ e *events.Service }

func (s eventsRevokeSink) PermissionRevoked(documentID, deviceID string) {
	s.e.SignalRevoked(documentID, deviceID)
}

// Events returns the change event service.
func (a *App) Events() *events.Service { return a.events }

// CRDT returns the CRDT state service.
func (a *App) CRDT() *crdt.Service { return a.crdt }

// ---- Change event service boundary (promoted so the entry layer has one
// object; each method delegates verbatim to the events service). ----

// InterruptWaits wakes every parked long poll and every live change-log and
// CRDT subscription without closing the database, so an orderly shutdown
// drains waiting connections (long polls answer 503; subscriptions finish
// with the going-away close) instead of holding the drain hostage.
func (a *App) InterruptWaits() {
	a.events.InterruptWaits()
	a.crdt.InterruptWaits()
}

// Closing reports whether shutdown has begun, covering both push channels.
func (a *App) Closing() bool {
	return a.events.Closing() || a.crdt.Closing()
}

// Close interrupts the in-memory observers, waits for every subscription
// handler to unregister and then releases the shared database handle.
func (a *App) Close() error {
	a.events.InterruptWaits()
	a.crdt.InterruptWaits()
	a.events.WaitDrained()
	a.crdt.WaitDrained()
	return a.Store.Close()
}

// PostChanges delegates to the change event service.
func (a *App) PostChanges(documentID string, changes []events.Change) ([]events.Result, error) {
	return a.events.PostChanges(documentID, changes)
}

// ReplayChanges delegates to the change event service.
func (a *App) ReplayChanges(documentID string, changes []events.Change) ([]events.Result, error) {
	return a.events.ReplayChanges(documentID, changes)
}

// PostSessionChanges commits a batch whose device identity was resolved from
// a session by the HTTP layer, delegating to the change event service's
// gated commit: the registration/permission verdict is taken inside the same
// serialized transaction as the write, so a revoked device commits nothing.
func (a *App) PostSessionChanges(documentID string, changes []events.Change) ([]events.Result, error) {
	return a.events.CommitAuthorized(documentID, changes)
}

// ReplaySessionChanges replays an offline batch whose device identity was
// resolved from a session by the HTTP layer; it is the session counterpart
// to ReplayChanges and commits through the same gated transaction the
// session-scoped batch commit uses, so the two paths cannot drift.
func (a *App) ReplaySessionChanges(documentID string, changes []events.Change) ([]events.Result, error) {
	return a.events.CommitAuthorized(documentID, changes)
}

// MergeChange delegates to the change event service.
func (a *App) MergeChange(documentID string, baseCursor int64, c events.Change) (events.MergeResult, error) {
	return a.events.MergeChange(documentID, baseCursor, c)
}

// MergeSessionChange merges a single change whose device identity was resolved
// from a session by the HTTP layer, delegating to the change event service's
// gated merge: the registration/permission verdict is taken inside the same
// serialized transaction as the merge judgment and append, so a revoked device
// writes nothing and a rejected request observes no change content.
func (a *App) MergeSessionChange(documentID string, baseCursor int64, c events.Change) (events.MergeResult, error) {
	return a.events.MergeChangeAuthorized(documentID, baseCursor, c)
}

// PutSnapshot delegates to the change event service.
func (a *App) PutSnapshot(documentID string, cursor int64, state json.RawMessage) (bool, error) {
	return a.events.PutSnapshot(documentID, cursor, state)
}

// GetSnapshot delegates to the change event service.
func (a *App) GetSnapshot(documentID string, cursor int64) (json.RawMessage, error) {
	return a.events.GetSnapshot(documentID, cursor)
}

// ExportSnapshots delegates to the change event service. A nil to means the
// interval has no upper bound.
func (a *App) ExportSnapshots(documentID string, from int64, to *int64) ([]events.ExportedSnapshot, error) {
	return a.events.ExportSnapshots(documentID, from, to)
}

// RestoreSnapshot delegates to the change event service.
func (a *App) RestoreSnapshot(documentID, deviceID, changeID string, snapshotCursor int64) (events.RestoreResult, error) {
	return a.events.RestoreSnapshot(documentID, deviceID, changeID, snapshotCursor)
}

// RestoreSessionSnapshot restores a snapshot as an ordinary change whose
// device identity was resolved from a session by the HTTP layer, delegating to
// the change event service's gated restore: the registration/permission
// verdict is taken inside the same serialized transaction as the snapshot
// lookup and the append, so a revoked device writes nothing and a rejected
// request observes no snapshot or change content.
func (a *App) RestoreSessionSnapshot(documentID, deviceID, changeID string, snapshotCursor int64) (events.RestoreResult, error) {
	return a.events.RestoreSnapshotAuthorized(documentID, deviceID, changeID, snapshotCursor)
}

// PutSnapshotVersion binds an existing snapshot cursor to a stable version
// name, delegating to the change event service's gated transaction: the
// registration/permission verdict is taken before the snapshot and version
// are observed, so a rejected request exposes neither.
func (a *App) PutSnapshotVersion(documentID, deviceID, name string, cursor int64) (events.VersionResult, error) {
	return a.events.PutSnapshotVersion(documentID, deviceID, name, cursor)
}

// ListSnapshotVersions delegates to the change event service.
func (a *App) ListSnapshotVersions(documentID, deviceID string) ([]events.SnapshotVersion, error) {
	return a.events.ListSnapshotVersions(documentID, deviceID)
}

// GetSnapshotVersionState delegates to the change event service, resolving a
// version name to its snapshot cursor and stored state.
func (a *App) GetSnapshotVersionState(documentID, deviceID, name string) (int64, json.RawMessage, error) {
	return a.events.GetSnapshotVersionState(documentID, deviceID, name)
}

// RebindSnapshotVersion moves a version name onto another existing snapshot,
// delegating to the change event service's gated transaction.
func (a *App) RebindSnapshotVersion(documentID, deviceID, name string, cursor int64) (events.VersionResult, error) {
	return a.events.RebindSnapshotVersion(documentID, deviceID, name, cursor)
}

// DeleteSnapshotVersion hard-deletes one version marker, delegating to the
// change event service's gated transaction; the snapshot itself is untouched.
func (a *App) DeleteSnapshotVersion(documentID, deviceID, name string) error {
	return a.events.DeleteSnapshotVersion(documentID, deviceID, name)
}

// RestoreSnapshotVersion restores the snapshot a version name points at as an
// ordinary change, delegating to the change event service's gated restore.
func (a *App) RestoreSnapshotVersion(documentID, deviceID, changeID, name string) (events.RestoreResult, error) {
	return a.events.RestoreSnapshotVersion(documentID, deviceID, changeID, name)
}

// ListChanges delegates to the change event service.
func (a *App) ListChanges(documentID string, after, limit int64) ([]events.ListedChange, int64, error) {
	return a.events.ListChanges(documentID, after, limit)
}

// ExportChanges delegates to the change event service. A nil to means the
// interval has no upper bound.
func (a *App) ExportChanges(documentID string, from int64, to *int64) ([]events.ListedChange, error) {
	return a.events.ExportChanges(documentID, from, to)
}

// GetChangesByIDs delegates to the change event service's read-only batch
// lookup: per-id found/compacted/missing answers in request order, gated on
// the calling device's registration and document permission.
func (a *App) GetChangesByIDs(documentID, deviceID string, ids []string) ([]events.ChangeLookup, error) {
	return a.events.GetChangesByIDs(documentID, deviceID, ids)
}

// WaitForChanges delegates to the change event service.
func (a *App) WaitForChanges(ctx context.Context, documentID string, after, limit int64, wait time.Duration) ([]events.ListedChange, int64, bool, error) {
	return a.events.WaitForChanges(ctx, documentID, after, limit, wait)
}

// DocumentExists delegates to the change event service.
func (a *App) DocumentExists(documentID string) (bool, error) {
	return a.events.DocumentExists(documentID)
}

// CompactChanges delegates to the change event service.
func (a *App) CompactChanges(documentID, deviceID string) (boundary, removed int64, err error) {
	return a.events.CompactChanges(documentID, deviceID)
}

// CompactSessionChanges compacts a document's change log for a caller whose
// device identity was resolved from a session by the HTTP layer, delegating to
// the change event service's gated compaction: the registration/permission
// verdict is taken inside the same serialized transaction as the trim, so a
// revoked device removes nothing and a rejected request observes no change
// content. It is the session counterpart to CompactChanges; the boundary,
// retained summaries and persistence semantics are identical.
func (a *App) CompactSessionChanges(documentID, deviceID string) (boundary, removed int64, err error) {
	return a.events.CompactChanges(documentID, deviceID)
}

// AddSubscription delegates to the change event service.
func (a *App) AddSubscription(documentID, deviceID string) (wakes <-chan struct{}, revoked <-chan struct{}, deleted <-chan struct{}, unregister func()) {
	return a.events.AddSubscription(documentID, deviceID)
}

// DeregisterDevice removes a registered device together with every record it
// owns — its sessions, the attachments it created (chunks, sealed state and
// unfinished uploads included), the access grants it held, and its
// document-permission rows — all in one serialized transaction that commits
// synchronously. An unknown or already deregistered device yields
// store.ErrDeviceNotFound and writes nothing, so a repeat deregistration
// misses exactly the way a never-registered id does and concurrent calls take
// effect at most once. Shared digest-addressed content is reclaimed only when
// the last completed attachment referencing it is gone; other devices'
// changes, snapshots and document content are untouched.
//
// After the commit the same id may register again as a brand-new device — no
// session, permission or attachment is inherited — and every live
// subscription the device held across both push channels is ended
// immediately, stickily.
func (a *App) DeregisterDevice(deviceID string) error {
	tx, err := a.Store.DB().Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	if err := store.DeleteDeviceTx(tx, deviceID); err != nil {
		return err
	}
	if err := a.Authz.DeleteDevicePermissionsTx(tx, deviceID); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}

	// End the device's live subscriptions only after the cascade is durable:
	// a reconnect attempt after the close finds the backing session gone
	// (404), so the connection cannot immediately re-establish itself.
	a.events.SignalDeviceDeregistered(deviceID)
	a.crdt.SignalDeviceDeregistered(deviceID)
	return nil
}

// ---- Document-level deletion. ----

// DeleteDocument removes a registered device's whole document in one
// serialized, synchronously committed transaction. The calling device is
// declared by deviceId — no new authentication is introduced.
//
// The fixed verdict order is taken inside the transaction, before any cleanup:
//
//   - an unregistered device yields store.ErrDeviceNotFound (404) and writes
//     nothing;
//   - a device whose permission for the document was revoked yields
//     store.ErrPermissionDenied (403) and no cleanup runs;
//   - a document with no durable data anywhere — never created or already
//     deleted — yields events.ErrDocumentNotFound (404) and writes nothing.
//
// On success every row of the document is removed as one judgment: its online
// and trimmed change log, the retained idempotency summaries, its snapshots
// and named snapshot-version markers, its CRDT state and the per-document
// permission ledger. The same id is a brand-new document afterward — the
// cursor space restarts at 1 — and inherits no history or version names.
// Concurrent deletes commit at most once because the existence verdict and
// the deletions share one immediate transaction on the single connection.
//
// Only after the commit is durable are the document's live subscriptions
// ended (close code 4420 on both push channels) and dropped from the
// management registry; other documents' and devices' connections are
// untouched.
func (a *App) DeleteDocument(documentID, deviceID string) error {
	tx, err := a.Store.DB().Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	// Registration first, then permission, both before any document content
	// is observed: a rejected delete reveals nothing and cleans nothing.
	if err := a.Authz.AuthorizedTx(tx, documentID, deviceID); err != nil {
		return err
	}

	eventsExist, err := a.events.DocumentDataExistsTx(tx, documentID)
	if err != nil {
		return err
	}
	crdtExist, err := a.crdt.DocumentDataExistsTx(tx, documentID)
	if err != nil {
		return err
	}
	// A ledger-only deviation row also counts as document data, so a document
	// whose only durable trace is a grant/revoke still deletes cleanly and the
	// re-created id inherits no authorization state.
	ledgerExist, err := a.Authz.DocumentHasRowsTx(tx, documentID)
	if err != nil {
		return err
	}
	if !eventsExist && !crdtExist && !ledgerExist {
		return events.ErrDocumentNotFound
	}

	if err := a.events.DeleteDocumentDataTx(tx, documentID); err != nil {
		return err
	}
	if err := a.crdt.DeleteDocumentDataTx(tx, documentID); err != nil {
		return err
	}
	if err := a.Authz.DeleteDocumentPermissionsTx(tx, documentID); err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	// End the document's live subscriptions on both channels and drop them
	// from the management registry only after the cleanup is durable: the
	// close is one-shot per connection, so a client re-subscribing to the
	// now-fresh id opens a new connection and never reopens the ended ones.
	a.events.SignalDocumentDeleted(documentID)
	a.crdt.SignalDocumentDeleted(documentID)
	a.CloseDocumentPush(documentID)
	return nil
}

// ---- Permission service boundary. ----

// SetDocumentPermission delegates to the permission service.
func (a *App) SetDocumentPermission(documentID, deviceID string, authorized bool) (bool, error) {
	return a.Authz.SetDocumentPermission(documentID, deviceID, authorized)
}

// DocumentAuthorized delegates to the permission service.
func (a *App) DocumentAuthorized(documentID, deviceID string) (bool, error) {
	return a.Authz.DocumentAuthorized(documentID, deviceID)
}

// ListDocumentPermissions delegates to the permission service.
func (a *App) ListDocumentPermissions(documentID string, limit, offset int64) ([]authz.PermissionEntry, error) {
	return a.Authz.ListDocumentPermissions(documentID, limit, offset)
}

// ---- CRDT state service boundary. ----

// SubmitCRDTOps delegates to the CRDT state service.
func (a *App) SubmitCRDTOps(documentID, declaredType string, ops []crdt.Op) ([]crdt.Result, error) {
	return a.crdt.SubmitOps(documentID, declaredType, ops)
}

// GetCRDTState delegates to the CRDT state service.
func (a *App) GetCRDTState(documentID string) (crdt.State, error) {
	return a.crdt.GetState(documentID)
}

// CompactCRDT delegates to the CRDT state service.
func (a *App) CompactCRDT(documentID, deviceID string) (crdt.Snapshot, error) {
	return a.crdt.Compact(documentID, deviceID)
}

// CompactSessionCRDT compacts a document's CRDT state for a caller whose
// device identity was resolved from a session by the HTTP layer, delegating
// to the CRDT state service's gated compaction: the registration/permission
// verdict is taken inside the same serialized transaction as the trim, so a
// revoked device trims nothing and a rejected request observes no state
// content. It is the session counterpart to CompactCRDT; the trimming,
// retained identities, snapshot body and persistence semantics are
// identical.
func (a *App) CompactSessionCRDT(documentID, deviceID string) (crdt.Snapshot, error) {
	return a.crdt.Compact(documentID, deviceID)
}

// GetCRDTSnapshot delegates to the CRDT state service.
func (a *App) GetCRDTSnapshot(documentID string) (crdt.Snapshot, error) {
	return a.crdt.GetSnapshot(documentID)
}

// GetCRDTOpsByIDs delegates to the CRDT state service's read-only batch
// lookup: per-id found/compacted/missing answers in request order, gated on
// the calling device's registration and document permission.
func (a *App) GetCRDTOpsByIDs(documentID, deviceID string, ids []string) ([]crdt.OpLookup, error) {
	return a.crdt.GetOpsByIDs(documentID, deviceID, ids)
}

// ListCRDTOps delegates to the CRDT state service's read-only ordered
// listing: one limit/offset page of the document's online operations in
// ascending id order plus the total number of operations compaction trimmed,
// gated on the calling device's registration and document permission.
func (a *App) ListCRDTOps(documentID, deviceID string, limit, offset int64) (crdt.OpList, error) {
	return a.crdt.ListOps(documentID, deviceID, limit, offset)
}

// OpenCRDTSubscription delegates to the CRDT state service.
func (a *App) OpenCRDTSubscription(documentID, deviceID string) (*crdt.State, *crdt.Subscription, func(), error) {
	return a.crdt.OpenSubscription(documentID, deviceID)
}

// AddCRDTSubscription delegates to the CRDT state service.
func (a *App) AddCRDTSubscription(documentID, deviceID string) (*crdt.Subscription, func()) {
	return a.crdt.AddSubscription(documentID, deviceID)
}

// RegisterPush records one freshly established push connection in the
// process-wide in-memory registry. The returned subscription carries the
// server-allocated id and the one-shot active-cancel signal; the connection
// layer removes it when the connection ends.
func (a *App) RegisterPush(deviceID, documentID string, kind push.Kind, cursor int64) *push.Subscription {
	return a.pushSubs.Register(deviceID, documentID, kind, cursor)
}

// UnregisterPush removes an ended push connection.
func (a *App) UnregisterPush(subscriptionID string) {
	a.pushSubs.Unregister(subscriptionID)
}

// ListPushSubscriptions returns the device's live push connections across
// both channels in establishment order. It is a pure memory view.
func (a *App) ListPushSubscriptions(deviceID string) []push.Info {
	return a.pushSubs.List(deviceID)
}

// CancelPushSubscription actively ends one of the device's live push
// connections. It returns false when the id is unknown, already gone, or owned
// by another device; those cases are one indistinguishable 404 at the edge.
func (a *App) CancelPushSubscription(deviceID, subscriptionID string) bool {
	return a.pushSubs.Cancel(deviceID, subscriptionID)
}

// CloseDocumentPush ends every live push connection subscribed to documentID,
// across both channels and every owning device, after its deletion committed.
// Other documents' connections are untouched.
func (a *App) CloseDocumentPush(documentID string) {
	a.pushSubs.CloseDocument(documentID)
}
