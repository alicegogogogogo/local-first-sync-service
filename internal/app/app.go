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
	"github.com/alicegogogogogo/local-first-sync-service/internal/store"
)

// App composes the durable kernel and the three internal services over one
// shared database connection.
type App struct {
	*store.Store

	events *events.Service
	crdt   *crdt.Service

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
		Store:  kernel,
		events: eventService,
		crdt:   crdtService,
		Authz:  permissions,
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

// MergeChange delegates to the change event service.
func (a *App) MergeChange(documentID string, baseCursor int64, c events.Change) (events.MergeResult, error) {
	return a.events.MergeChange(documentID, baseCursor, c)
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

// ListChanges delegates to the change event service.
func (a *App) ListChanges(documentID string, after, limit int64) ([]events.ListedChange, int64, error) {
	return a.events.ListChanges(documentID, after, limit)
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

// AddSubscription delegates to the change event service.
func (a *App) AddSubscription(documentID, deviceID string) (<-chan struct{}, <-chan struct{}, func()) {
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

// GetCRDTSnapshot delegates to the CRDT state service.
func (a *App) GetCRDTSnapshot(documentID string) (crdt.Snapshot, error) {
	return a.crdt.GetSnapshot(documentID)
}

// OpenCRDTSubscription delegates to the CRDT state service.
func (a *App) OpenCRDTSubscription(documentID, deviceID string) (*crdt.State, *crdt.Subscription, func(), error) {
	return a.crdt.OpenSubscription(documentID, deviceID)
}

// AddCRDTSubscription delegates to the CRDT state service.
func (a *App) AddCRDTSubscription(documentID, deviceID string) (*crdt.Subscription, func()) {
	return a.crdt.AddSubscription(documentID, deviceID)
}
