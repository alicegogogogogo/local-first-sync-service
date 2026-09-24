package service_test

import (
	"bytes"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

var (
	binaryPath string
	buildOnce  sync.Once
	buildErr   error
)

// buildBinary compiles the real syncd entrypoint once for the process tests.
func buildBinary(t *testing.T) string {
	t.Helper()
	buildOnce.Do(func() {
		dir, err := os.MkdirTemp("", "syncd-bin")
		if err != nil {
			buildErr = err
			return
		}
		binaryPath = filepath.Join(dir, "syncd")
		buildErr = exec.Command("go", "build", "-o", binaryPath,
			"github.com/alicegogogogogo/local-first-sync-service/cmd/syncd").Run()
	})
	if buildErr != nil {
		t.Fatalf("build syncd: %v", buildErr)
	}
	return binaryPath
}

type process struct {
	cmd     *exec.Cmd
	out     *safeBuffer
	addr    string
	dataDir string
	exited  chan error // closed once Wait returns, carrying the exit error
}

type safeBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (s *safeBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.Write(p)
}

func (s *safeBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.String()
}

var listeningLine = regexp.MustCompile(`sync service listening on (\S+) \(data: (.*)\)`)

// startProcess launches syncd with the given environment and waits for the
// startup line, returning the actual endpoint parsed from it.
func startProcess(t *testing.T, env ...string) *process {
	t.Helper()
	p := &process{
		cmd:    exec.Command(buildBinary(t)),
		out:    &safeBuffer{},
		exited: make(chan error, 1),
	}
	p.cmd.Env = append(os.Environ(), env...)
	p.cmd.Stdout = p.out
	p.cmd.Stderr = p.out
	if err := p.cmd.Start(); err != nil {
		t.Fatalf("start syncd: %v", err)
	}
	go func() { p.exited <- p.cmd.Wait() }()

	deadline := time.Now().Add(15 * time.Second)
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for time.Now().Before(deadline) {
		if m := listeningLine.FindStringSubmatch(p.out.String()); m != nil {
			p.addr = m[1]
			p.dataDir = m[2]
			return p
		}
		select {
		case err := <-p.exited:
			t.Fatalf("syncd exited before reporting its endpoint (err=%v); output:\n%s", err, p.out.String())
		default:
		}
		<-ticker.C
	}
	_ = p.cmd.Process.Kill()
	<-p.exited
	t.Fatalf("syncd never reported its endpoint; output:\n%s", p.out.String())
	return nil
}

func (p *process) terminate(t *testing.T) int {
	t.Helper()
	if err := p.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatalf("send SIGTERM: %v", err)
	}
	select {
	case err := <-p.exited:
		if ee, ok := err.(*exec.ExitError); ok {
			return ee.ExitCode()
		}
		if err != nil {
			t.Fatalf("wait for syncd: %v", err)
		}
		return 0
	case <-time.After(10 * time.Second):
		_ = p.cmd.Process.Kill()
		t.Fatalf("process did not exit within 10s; output:\n%s", p.out.String())
	}
	return -1
}

func (p *process) getHealth(t *testing.T) (int, map[string]any) {
	t.Helper()
	return processJSON(t, http.MethodGet, "http://"+p.addr+"/healthz", nil)
}

func processJSON(t *testing.T, method, url string, body any) (int, map[string]any) {
	t.Helper()
	var rdr io.Reader
	if body != nil {
		raw, _ := json.Marshal(body)
		rdr = strings.NewReader(string(raw))
	}
	req, err := http.NewRequest(method, url, rdr)
	if err != nil {
		t.Fatal(err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, url, err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	var decoded map[string]any
	_ = json.Unmarshal(raw, &decoded)
	return resp.StatusCode, decoded
}

// The real binary with :0 reports the allocated endpoint and data path in its
// startup log, and that endpoint serves /healthz.
func TestProcessEphemeralAddrFromLog(t *testing.T) {
	dataPath := filepath.Join(t.TempDir(), "sync.db")
	p := startProcess(t, "SYNC_ADDR=127.0.0.1:0", "SYNC_DATA="+dataPath)
	defer p.cmd.Process.Kill()

	if p.dataDir != dataPath {
		t.Fatalf("logged data path = %q, want %q", p.dataDir, dataPath)
	}
	host, portStr, err := net.SplitHostPort(p.addr)
	if err != nil {
		t.Fatalf("logged addr %q: %v", p.addr, err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil || port == 0 {
		t.Fatalf("logged port = %q, want a non-zero allocated port", portStr)
	}
	if host != "127.0.0.1" {
		t.Fatalf("logged host = %q, want 127.0.0.1", host)
	}

	status, body := p.getHealth(t)
	if status != http.StatusOK || body["status"] != "ok" {
		t.Fatalf("healthz = %d %v, want 200 status=ok", status, body)
	}

	if code := p.terminate(t); code != 0 {
		t.Fatalf("exit code after SIGTERM = %d, want 0; output:\n%s", code, p.out.String())
	}
}

// A malformed SYNC_ADDR exits non-zero with a clear error and no listening line.
func TestProcessInvalidAddrExitsNonZero(t *testing.T) {
	for _, bad := range []string{"not-an-address", "127.0.0.1:99999"} {
		cmd := exec.Command(buildBinary(t))
		cmd.Env = append(os.Environ(),
			"SYNC_ADDR="+bad,
			"SYNC_DATA="+filepath.Join(t.TempDir(), "sync.db"))
		var out bytes.Buffer
		cmd.Stdout = &out
		cmd.Stderr = &out
		err := cmd.Run()
		if err == nil {
			t.Fatalf("SYNC_ADDR=%q exited zero, want non-zero", bad)
		}
		if ee, ok := err.(*exec.ExitError); ok && ee.ExitCode() == 0 {
			t.Fatalf("SYNC_ADDR=%q exited zero, want non-zero", bad)
		}
		if !strings.Contains(out.String(), "invalid SYNC_ADDR") {
			t.Fatalf("SYNC_ADDR=%q output = %q, want invalid SYNC_ADDR", bad, out.String())
		}
		if listeningLine.MatchString(out.String()) {
			t.Fatalf("SYNC_ADDR=%q printed a listening line despite failure", bad)
		}
	}
}

// A fixed-port clash makes the second process exit non-zero; the first keeps
// serving. Restarting after the first stops binds the same address again.
func TestProcessPortConflictAndRestart(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	if err := ln.Close(); err != nil {
		t.Fatal(err)
	}
	dataPath := filepath.Join(t.TempDir(), "sync.db")

	first := startProcess(t, "SYNC_ADDR="+addr, "SYNC_DATA="+dataPath)
	if first.addr != addr {
		t.Fatalf("first instance addr = %q, want %q", first.addr, addr)
	}

	conflict := exec.Command(buildBinary(t))
	var out bytes.Buffer
	conflict.Env = append(os.Environ(),
		"SYNC_ADDR="+addr,
		"SYNC_DATA="+filepath.Join(t.TempDir(), "sync.db"))
	conflict.Stdout = &out
	conflict.Stderr = &out
	if err := conflict.Run(); err == nil {
		t.Fatal("conflicting process exited zero, want non-zero")
	} else if ee, ok := err.(*exec.ExitError); ok && ee.ExitCode() == 0 {
		t.Fatal("conflicting process exited zero, want non-zero")
	}
	if !strings.Contains(out.String(), "listen on "+addr) {
		t.Fatalf("conflict output = %q, want listen error for %s", out.String(), addr)
	}

	status, body := first.getHealth(t)
	if status != http.StatusOK || body["status"] != "ok" {
		t.Fatalf("first instance health during clash: %d %v", status, body)
	}
	if code := first.terminate(t); code != 0 {
		t.Fatalf("first instance exit code = %d", code)
	}

	// Restart with the same fixed address constraint succeeds now the port is free.
	restart := startProcess(t, "SYNC_ADDR="+addr, "SYNC_DATA="+dataPath)
	if code := restart.terminate(t); code != 0 {
		t.Fatalf("restart exit code = %d; output:\n%s", code, restart.out.String())
	}
}

// An unwritable SYNC_DATA exits non-zero with a clear error.
func TestProcessBadDataPathExitsNonZero(t *testing.T) {
	blocker := filepath.Join(t.TempDir(), "blocker")
	if err := os.WriteFile(blocker, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(buildBinary(t))
	var out bytes.Buffer
	cmd.Env = append(os.Environ(),
		"SYNC_ADDR=127.0.0.1:0",
		"SYNC_DATA="+filepath.Join(blocker, "dir", "sync.db"))
	cmd.Stdout = &out
	cmd.Stderr = &out
	if err := cmd.Run(); err == nil {
		t.Fatal("bad SYNC_DATA exited zero, want non-zero")
	} else if ee, ok := err.(*exec.ExitError); ok && ee.ExitCode() == 0 {
		t.Fatal("bad SYNC_DATA exited zero, want non-zero")
	}
	combined := out.String()
	if !strings.Contains(combined, "create data directory") && !strings.Contains(combined, "open store") {
		t.Fatalf("output = %q, want data directory or store open error", combined)
	}
	if listeningLine.MatchString(combined) {
		t.Fatal("bad SYNC_DATA printed a listening line despite failure")
	}
}

// A committed device survives SIGTERM and is visible after a restart against
// the same data path; the stop is graceful and exits zero.
func TestProcessSignalPersistsCommittedState(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	dataPath := filepath.Join(t.TempDir(), "sync.db")

	first := startProcess(t, "SYNC_ADDR="+addr, "SYNC_DATA="+dataPath)
	status, body := processJSON(t, http.MethodPost, "http://"+first.addr+"/v1/devices",
		map[string]string{"deviceId": "persisted"})
	if status != http.StatusOK || body["created"] != true {
		t.Fatalf("register: %d %v", status, body)
	}
	if code := first.terminate(t); code != 0 {
		t.Fatalf("SIGTERM exit code = %d, want 0", code)
	}

	second := startProcess(t, "SYNC_ADDR="+addr, "SYNC_DATA="+dataPath)
	defer second.cmd.Process.Kill()
	status, body = processJSON(t, http.MethodPost, "http://"+second.addr+"/v1/devices",
		map[string]string{"deviceId": "persisted"})
	if status != http.StatusOK || body["created"] != false {
		t.Fatalf("re-register after restart: %d %v, want created=false", status, body)
	}
}

// Two real processes started at once on :0 with distinct data paths each get
// their own endpoint from their own log line, and a device registered in one
// is absent from the other.
func TestProcessTwoEphemeralInstances(t *testing.T) {
	a := startProcess(t, "SYNC_ADDR=127.0.0.1:0", "SYNC_DATA="+filepath.Join(t.TempDir(), "a.db"))
	defer a.cmd.Process.Kill()
	b := startProcess(t, "SYNC_ADDR=127.0.0.1:0", "SYNC_DATA="+filepath.Join(t.TempDir(), "b.db"))
	defer b.cmd.Process.Kill()
	if a.addr == b.addr {
		t.Fatalf("both processes report endpoint %s", a.addr)
	}

	if status, body := processJSON(t, http.MethodPost, "http://"+a.addr+"/v1/devices",
		map[string]string{"deviceId": "shared-name"}); status != http.StatusOK || body["created"] != true {
		t.Fatalf("register in A: %d %v", status, body)
	}
	if status, body := processJSON(t, http.MethodPost, "http://"+b.addr+"/v1/devices",
		map[string]string{"deviceId": "shared-name"}); status != http.StatusOK || body["created"] != true {
		t.Fatalf("register in B: %d %v, want independent created=true", status, body)
	}
	if status, body := processJSON(t, http.MethodPost, "http://"+a.addr+"/v1/devices",
		map[string]string{"deviceId": "shared-name"}); status != http.StatusOK || body["created"] != false {
		t.Fatalf("repeat register in A: %d %v, want created=false", status, body)
	}
	if code := a.terminate(t); code != 0 {
		t.Fatalf("A exit = %d", code)
	}
	if code := b.terminate(t); code != 0 {
		t.Fatalf("B exit = %d", code)
	}
}
