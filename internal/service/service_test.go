package service_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/alicegogogogogo/local-first-sync-service/internal/service"
)

const helloWorldSHA256 = "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"

type running struct {
	baseURL string
	dataDir string
	logs    *bytes.Buffer
	cancel  context.CancelFunc
	done    chan error
}

func (r *running) stop(t *testing.T) {
	t.Helper()
	r.cancel()
	select {
	case err := <-r.done:
		if err != nil {
			t.Fatalf("service stopped with error: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("service did not stop within 10s")
	}
}

// startService runs one instance in-process and waits for its startup line.
func startService(t *testing.T, addr, dataPath string) *running {
	t.Helper()
	if dataPath == "" {
		dataPath = filepath.Join(t.TempDir(), "sync.db")
	}
	var logs bytes.Buffer
	ctx, cancel := context.WithCancel(context.Background())
	readyCh := make(chan string, 1)
	done := make(chan error, 1)
	go func() {
		done <- service.Run(ctx, service.Options{
			Addr:     addr,
			DataPath: dataPath,
			Logger:   log.New(&logs, "", 0),
			OnReady:  func(actual string) { readyCh <- actual },
		})
	}()
	select {
	case actual := <-readyCh:
		return &running{
			baseURL: "http://" + actual,
			dataDir: dataPath,
			logs:    &logs,
			cancel:  cancel,
			done:    done,
		}
	case err := <-done:
		cancel()
		t.Fatalf("service failed to start: %v", err)
	case <-time.After(10 * time.Second):
		cancel()
		t.Fatal("service did not become ready within 10s")
	}
	return nil
}

// runServiceExpectError starts an instance that must fail before readiness.
func runServiceExpectError(t *testing.T, addr, dataPath string) error {
	t.Helper()
	var logs bytes.Buffer
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- service.Run(ctx, service.Options{
			Addr:     addr,
			DataPath: dataPath,
			Logger:   log.New(&logs, "", 0),
		})
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("service.Run returned nil, want startup error")
		}
		return err
	case <-time.After(10 * time.Second):
		t.Fatal("service did not fail within 10s")
	}
	return nil
}

func doJSON(t *testing.T, method, url string, body any) (int, map[string]any) {
	t.Helper()
	var rdr io.Reader
	if body != nil {
		raw, _ := json.Marshal(body)
		rdr = bytes.NewReader(raw)
	}
	req, err := http.NewRequest(method, url, rdr)
	if err != nil {
		t.Fatal(err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	return doRequest(t, req)
}

func doRaw(t *testing.T, method, url string, ctype string, body []byte) (int, []byte) {
	t.Helper()
	req, err := http.NewRequest(method, url, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", ctype)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, url, err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, raw
}

func doRequest(t *testing.T, req *http.Request) (int, map[string]any) {
	t.Helper()
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", req.Method, req.URL, err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	var decoded map[string]any
	_ = json.Unmarshal(raw, &decoded)
	return resp.StatusCode, decoded
}

func freePort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	return ln.Addr().(*net.TCPAddr).Port
}

// :0 allocates a real dialable port; the startup line reports that port and
// the process's own data path, and /healthz answers on the reported endpoint.
func TestEphemeralAddrReportsActualEndpointAndHealth(t *testing.T) {
	dataPath := filepath.Join(t.TempDir(), "nested", "sync.db")
	r := startService(t, "127.0.0.1:0", dataPath)
	defer r.stop(t)

	want := fmt.Sprintf("sync service listening on %s (data: %s)", strings.TrimPrefix(r.baseURL, "http://"), dataPath)
	if got := r.logs.String(); !strings.Contains(got, want) {
		t.Fatalf("startup log = %q, want line %q", got, want)
	}

	status, body := doJSON(t, http.MethodGet, r.baseURL+"/healthz", nil)
	if status != http.StatusOK || body["status"] != "ok" {
		t.Fatalf("healthz = %d %v, want 200 status=ok", status, body)
	}
}

// A fixed address binds exactly that port. A second instance on the busy port
// must fail with a listen error before touching its data directory, while the
// first instance keeps serving.
func TestFixedPortConflictFailsBeforeStoreOpen(t *testing.T) {
	port := freePort(t)
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	first := startService(t, addr, "")
	defer first.stop(t)

	secondData := filepath.Join(t.TempDir(), "second", "sync.db")
	err := runServiceExpectError(t, addr, secondData)
	if !strings.Contains(err.Error(), "listen") {
		t.Fatalf("conflict error = %q, want a listen error", err.Error())
	}
	// Bind happens before the data directory is made, so a failed startup
	// leaves no files behind.
	if _, statErr := os.Stat(filepath.Dir(secondData)); !os.IsNotExist(statErr) {
		t.Fatalf("second instance created data directory despite bind failure: %v", statErr)
	}

	status, body := doJSON(t, http.MethodGet, first.baseURL+"/healthz", nil)
	if status != http.StatusOK || body["status"] != "ok" {
		t.Fatalf("first instance health after clash = %d %v", status, body)
	}
}

// Malformed SYNC_ADDR values are startup errors with no fallback address.
func TestInvalidAddrFails(t *testing.T) {
	for _, bad := range []string{"no-port", "127.0.0.1:70000", "127.0.0.1:abc", "::"} {
		err := runServiceExpectError(t, bad, filepath.Join(t.TempDir(), "sync.db"))
		if !strings.Contains(err.Error(), "invalid SYNC_ADDR") {
			t.Fatalf("addr %q error = %q, want invalid SYNC_ADDR", bad, err.Error())
		}
	}
}

// A SYNC_DATA path whose parent cannot be created (a component is a regular
// file) fails startup non-zero and releases the listener it had bound.
func TestUncreatableDataPathFailsAndReleasesListener(t *testing.T) {
	blocker := filepath.Join(t.TempDir(), "blocker")
	if err := os.WriteFile(blocker, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	port := freePort(t)
	addr := fmt.Sprintf("127.0.0.1:%d", port)

	err := runServiceExpectError(t, addr, filepath.Join(blocker, "sync.db"))
	if !strings.Contains(err.Error(), "create data directory") && !strings.Contains(err.Error(), "open store") {
		t.Fatalf("error = %q, want data directory/store failure", err.Error())
	}

	// The bound listener must have been closed: the same fixed port binds again.
	ln, listenErr := net.Listen("tcp", addr)
	if listenErr != nil {
		t.Fatalf("listener not released after failed startup: %v", listenErr)
	}
	_ = ln.Close()
}

// Two instances started simultaneously on :0 with separate SYNC_DATA paths
// keep independent registration, session and permission state.
func TestTwoInstancesIsolatedByDataPath(t *testing.T) {
	a := startService(t, "127.0.0.1:0", filepath.Join(t.TempDir(), "a", "sync.db"))
	defer a.stop(t)
	b := startService(t, "127.0.0.1:0", filepath.Join(t.TempDir(), "b", "sync.db"))
	defer b.stop(t)
	if strings.TrimPrefix(a.baseURL, "http://") == strings.TrimPrefix(b.baseURL, "http://") {
		t.Fatalf("both instances bound the same endpoint: %s", a.baseURL)
	}

	// The same device id registers independently in each database.
	for _, r := range []*running{a, b} {
		status, body := doJSON(t, http.MethodPost, r.baseURL+"/v1/devices", map[string]string{"deviceId": "same"})
		if status != http.StatusOK || body["created"] != true {
			t.Fatalf("register in %s: %d %v, want 200 created=true", r.baseURL, status, body)
		}
	}

	// A revokes permission for document "plan" and opens a session there.
	status, body := doJSON(t, http.MethodPost, a.baseURL+"/v1/documents/plan/permissions",
		map[string]string{"deviceId": "same", "action": "revoke"})
	if status != http.StatusOK || body["changed"] != true {
		t.Fatalf("revoke in A: %d %v", status, body)
	}
	if status, body = doJSON(t, http.MethodPost, a.baseURL+"/v1/devices/same/sessions",
		map[string]string{"sessionId": "sess-a"}); status != http.StatusOK || body["created"] != true {
		t.Fatalf("session in A: %d %v", status, body)
	}
	status, body = doJSON(t, http.MethodGet, a.baseURL+"/v1/sessions/sess-a/documents/plan/changes", nil)
	if status != http.StatusForbidden {
		t.Fatalf("session read in A: %d %v, want 403", status, body)
	}

	// B must not see A's session or A's revocation: a fresh session there is
	// authorized and reads an empty change list.
	status, body = doJSON(t, http.MethodGet, b.baseURL+"/v1/sessions/sess-a/documents/plan/changes", nil)
	if status != http.StatusNotFound {
		t.Fatalf("B saw A's session: %d %v, want 404", status, body)
	}
	if status, body = doJSON(t, http.MethodPost, b.baseURL+"/v1/devices/same/sessions",
		map[string]string{"sessionId": "sess-b"}); status != http.StatusOK || body["created"] != true {
		t.Fatalf("session in B: %d %v, want 200 created=true", status, body)
	}
	status, body = doJSON(t, http.MethodGet, b.baseURL+"/v1/sessions/sess-b/documents/plan/changes", nil)
	if status != http.StatusOK {
		t.Fatalf("session read in B: %d %v, want 200", status, body)
	}
	if changes, ok := body["changes"].([]any); !ok || len(changes) != 0 {
		t.Fatalf("changes in B = %v, want empty list", body["changes"])
	}
	if body["nextCursor"] != float64(0) {
		t.Fatalf("nextCursor in B = %v, want 0", body["nextCursor"])
	}
}

// After graceful shutdown a restart on the same fixed address and data path
// sees the committed device and sealed attachment state; the fixed address is
// honored again on restart.
func TestRestartSameAddrAndDataPersists(t *testing.T) {
	port := freePort(t)
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	dataPath := filepath.Join(t.TempDir(), "sync.db")

	first := startService(t, addr, dataPath)

	// Register a device and finish a one-chunk attachment.
	status, body := doJSON(t, http.MethodPost, first.baseURL+"/v1/devices", map[string]string{"deviceId": "dev-1"})
	if status != http.StatusOK || body["created"] != true {
		t.Fatalf("register: %d %v", status, body)
	}
	content := []byte("hello world")
	if sum := sha256.Sum256(content); hex.EncodeToString(sum[:]) != helloWorldSHA256 {
		t.Fatal("test fixture sha256 mismatch")
	}
	status, body = doJSON(t, http.MethodPost, first.baseURL+"/v1/devices/dev-1/attachments", map[string]any{
		"attachmentId": "att-1",
		"totalBytes":   len(content),
		"chunkSize":    len(content),
		"sha256":       helloWorldSHA256,
	})
	if status != http.StatusOK || body["created"] != true {
		t.Fatalf("create attachment: %d %v", status, body)
	}
	if code, _ := doRaw(t, http.MethodPut, first.baseURL+"/v1/devices/dev-1/attachments/att-1/chunks/0",
		"application/octet-stream", content); code != http.StatusOK {
		t.Fatalf("put chunk: %d", code)
	}
	status, body = doJSON(t, http.MethodPost, first.baseURL+"/v1/devices/dev-1/attachments/att-1/complete", nil)
	if status != http.StatusOK || body["complete"] != true {
		t.Fatalf("complete: %d %v", status, body)
	}
	first.stop(t)

	// Restart honors the same address constraint: the port is free again and
	// a new instance binds it, refusing to move to a default port.
	second := startService(t, addr, dataPath)
	defer second.stop(t)
	if second.baseURL != "http://"+addr {
		t.Fatalf("restart bound %s, want %s", second.baseURL, addr)
	}

	status, body = doJSON(t, http.MethodPost, second.baseURL+"/v1/devices", map[string]string{"deviceId": "dev-1"})
	if status != http.StatusOK || body["created"] != false {
		t.Fatalf("re-register after restart: %d %v, want created=false", status, body)
	}
	status, body = doJSON(t, http.MethodGet, second.baseURL+"/v1/devices/dev-1/attachments/att-1", nil)
	if status != http.StatusOK || body["complete"] != true {
		t.Fatalf("attachment after restart: %d %v, want complete=true", status, body)
	}
	code, raw := doRaw(t, http.MethodGet, second.baseURL+"/v1/devices/dev-1/attachments/att-1/chunks/0", "", nil)
	if code != http.StatusOK || !bytes.Equal(raw, content) {
		t.Fatalf("chunk after restart: %d %q, want hello world", code, raw)
	}
}

// Once shutdown starts, new connections are refused and Run returns cleanly;
// the same port is bindable by the next process.
func TestShutdownStopsAccepting(t *testing.T) {
	port := freePort(t)
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	r := startService(t, addr, "")
	r.cancel()
	select {
	case err := <-r.done:
		if err != nil {
			t.Fatalf("Run after shutdown: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return after shutdown")
	}

	if _, err := net.DialTimeout("tcp", addr, time.Second); err == nil {
		t.Fatal("listener still accepting connections after shutdown")
	}
}
