package events_test

import (
	"database/sql"
	"errors"
	"testing"

	"github.com/alicegogogogogo/local-first-sync-service/internal/events"
	"github.com/alicegogogogogo/local-first-sync-service/internal/store"
)

// fakeGate answers the change event service's Gate seam without the
// permission service, proving the service is reusable on its own and that
// replay consults the gate before resolving any change id.
type fakeGate struct{ err error }

func (g fakeGate) DeviceAuthorizedTx(_ *sql.Tx, _, _ string) error { return g.err }

// The service constructs directly over a kernel handle and commits a normal
// batch without any gate (ordinary post never consults registration).
func TestServiceStandalonePostAndRead(t *testing.T) {
	kernel, err := store.Open("")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = kernel.Close() }()
	svc, err := events.New(kernel, nil)
	if err != nil {
		t.Fatal(err)
	}

	results, err := svc.PostChanges("doc", []events.Change{
		{ID: "c1", DeviceID: "dev", Payload: []byte(`{"n":1}`)},
		{ID: "c2", DeviceID: "dev", Payload: []byte(`[1,true,null]`)},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 2 || results[0].Cursor != 1 || results[1].Cursor != 2 ||
		!results[0].Created || !results[1].Created {
		t.Fatalf("results = %+v", results)
	}

	// Boundary: an empty after page on an unknown document is empty/0.
	if rows, next, err := svc.ListChanges("unknown", 0, 10); err != nil || len(rows) != 0 || next != 0 {
		t.Fatalf("unknown doc rows=%v next=%d err=%v", rows, next, err)
	}
	// Boundary: a known document caught up echoes after without advancing it.
	if rows, next, err := svc.ListChanges("doc", 2, 10); err != nil || len(rows) != 0 || next != 2 {
		t.Fatalf("caught-up rows=%v next=%d err=%v", rows, next, err)
	}
}

// A gate denial (unregistered device) rejects the replay with 404 semantics
// and writes nothing, even though the id is new.
func TestServiceStandaloneReplayGateNotFound(t *testing.T) {
	kernel, _ := store.Open("")
	defer func() { _ = kernel.Close() }()
	svc, _ := events.New(kernel, fakeGate{err: store.ErrDeviceNotFound})

	_, err := svc.ReplayChanges("doc", []events.Change{
		{ID: "c1", DeviceID: "ghost", Payload: []byte(`{"n":1}`)},
	})
	if !errors.Is(err, store.ErrDeviceNotFound) {
		t.Fatalf("replay err = %v, want ErrDeviceNotFound", err)
	}
	if ok, _ := svc.DocumentExists("doc"); ok {
		t.Fatal("rejected replay wrote change content")
	}
}

// A revoked gate rejects the replay with 403 semantics and writes nothing.
func TestServiceStandaloneReplayGateRevoked(t *testing.T) {
	kernel, _ := store.Open("")
	defer func() { _ = kernel.Close() }()
	svc, _ := events.New(kernel, fakeGate{err: store.ErrPermissionDenied})

	_, err := svc.ReplayChanges("doc", []events.Change{
		{ID: "c1", DeviceID: "dev", Payload: []byte(`{"n":1}`)},
	})
	if !errors.Is(err, store.ErrPermissionDenied) {
		t.Fatalf("replay err = %v, want ErrPermissionDenied", err)
	}
	if ok, _ := svc.DocumentExists("doc"); ok {
		t.Fatal("rejected replay wrote change content")
	}
}

// A content conflict on a replay returns *ErrConflict and leaves the batch
// untouched, with the gate consulted first.
func TestServiceStandaloneReplayConflict(t *testing.T) {
	kernel, _ := store.Open("")
	defer func() { _ = kernel.Close() }()
	svc, _ := events.New(kernel, fakeGate{})

	if _, err := svc.PostChanges("doc", []events.Change{
		{ID: "c1", DeviceID: "dev", Payload: []byte(`{"n":1}`)},
	}); err != nil {
		t.Fatal(err)
	}
	_, err := svc.ReplayChanges("doc", []events.Change{
		{ID: "c1", DeviceID: "dev", Payload: []byte(`{"n":2}`)},
	})
	var conflict *events.ErrConflict
	if !errors.As(err, &conflict) || conflict.ID != "c1" {
		t.Fatalf("replay err = %v, want ErrConflict{c1}", err)
	}
}

// An idempotent replay through an allowing gate reports created=false and the
// original cursor.
func TestServiceStandaloneReplayIdempotent(t *testing.T) {
	kernel, _ := store.Open("")
	defer func() { _ = kernel.Close() }()
	svc, _ := events.New(kernel, fakeGate{})

	if _, err := svc.PostChanges("doc", []events.Change{
		{ID: "c1", DeviceID: "dev", Payload: []byte(`{"a":1,"b":2}`)},
	}); err != nil {
		t.Fatal(err)
	}
	results, err := svc.ReplayChanges("doc", []events.Change{
		{ID: "c1", DeviceID: "dev", Payload: []byte(`{"b":2,"a":1}`)},
	})
	if err != nil {
		t.Fatal(err)
	}
	if results[0].Created || results[0].Cursor != 1 {
		t.Fatalf("idempotent results = %+v", results)
	}
}
