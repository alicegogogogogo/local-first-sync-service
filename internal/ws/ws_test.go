package ws

import (
	"bufio"
	"crypto/rand"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// rawClient opens a WebSocket against h and returns the conn plus a buffered
// reader positioned right after the 101 response headers.
func rawClient(t *testing.T, h http.Handler) (net.Conn, *bufio.Reader) {
	t.Helper()
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)

	conn, err := net.Dial("tcp", srv.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	keyBuf := make([]byte, 16)
	if _, err := rand.Read(keyBuf); err != nil {
		t.Fatal(err)
	}
	key := base64.StdEncoding.EncodeToString(keyBuf)
	fmt.Fprintf(conn, "GET / HTTP/1.1\r\nHost: x\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: %s\r\nSec-WebSocket-Version: 13\r\n\r\n", key)
	br := bufio.NewReader(conn)
	resp, err := http.ReadResponse(br, nil)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 101 {
		t.Fatalf("status = %d, want 101", resp.StatusCode)
	}
	return conn, br
}

func clientFrame(mask bool, opcode byte, payload []byte) []byte {
	var b []byte
	b = append(b, 0x80|opcode)
	if mask {
		b = append(b, 0x80|byte(len(payload)))
		m := []byte{1, 2, 3, 4}
		b = append(b, m...)
		for i := range payload {
			b = append(b, payload[i]^m[i%4])
		}
	} else {
		b = append(b, byte(len(payload)))
		b = append(b, payload...)
	}
	return b
}

func readServerFrame(t *testing.T, br *bufio.Reader) (byte, []byte) {
	t.Helper()
	var head [2]byte
	if _, err := io.ReadFull(br, head[:]); err != nil {
		t.Fatal(err)
	}
	op := head[0] & 0x0F
	n := int(head[1] & 0x7F)
	p := make([]byte, n)
	if _, err := io.ReadFull(br, p); err != nil {
		t.Fatal(err)
	}
	return op, p
}

func TestValidateUpgradeHeaders(t *testing.T) {
	good := httptest.NewRequest(http.MethodGet, "/", nil)
	good.Header.Set("Connection", "keep-alive, Upgrade")
	good.Header.Set("Upgrade", "WebSocket")
	good.Header.Set("Sec-WebSocket-Version", "13")
	good.Header.Set("Sec-WebSocket-Key", base64.StdEncoding.EncodeToString(make([]byte, 16)))
	if err := ValidateUpgradeHeaders(good); err != nil {
		t.Fatalf("valid handshake rejected: %v", err)
	}

	cases := []func(*http.Request){
		func(r *http.Request) { r.Header.Del("Connection") },
		func(r *http.Request) { r.Header.Set("Connection", "close") },
		func(r *http.Request) { r.Header.Set("Upgrade", "h2c") },
		func(r *http.Request) { r.Header.Set("Sec-WebSocket-Version", "8") },
		func(r *http.Request) { r.Header.Set("Sec-WebSocket-Key", "short") },
		func(r *http.Request) { r.Header.Del("Sec-WebSocket-Key") },
	}
	for i, mutate := range cases {
		r := httptest.NewRequest(http.MethodGet, "/", nil)
		r.Header.Set("Connection", "Upgrade")
		r.Header.Set("Upgrade", "websocket")
		r.Header.Set("Sec-WebSocket-Version", "13")
		r.Header.Set("Sec-WebSocket-Key", base64.StdEncoding.EncodeToString(make([]byte, 16)))
		mutate(r)
		if err := ValidateUpgradeHeaders(r); err == nil {
			t.Fatalf("case %d accepted an invalid handshake", i)
		}
	}
}

func TestHandshakeAndTextFrame(t *testing.T) {
	accepted := make(chan *Conn, 1)
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := Accept(w, r)
		if err != nil {
			t.Errorf("accept: %v", err)
			return
		}
		accepted <- c
		<-c.Done()
	})
	conn, br := rawClient(t, h)
	c := <-accepted
	defer c.Close()

	if err := c.WriteText([]byte(`{"id":"c1","cursor":1}`)); err != nil {
		t.Fatal(err)
	}
	op, p := readServerFrame(t, br)
	if op != opText || string(p) != `{"id":"c1","cursor":1}` {
		t.Fatalf("frame = op %d payload %q", op, p)
	}
	_ = conn
}

// A client ping is answered with a pong carrying the same bytes.
func TestServerAnswersPingWithPong(t *testing.T) {
	accepted := make(chan *Conn, 1)
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := Accept(w, r)
		if err != nil {
			t.Errorf("accept: %v", err)
			return
		}
		accepted <- c
		<-c.Done()
	})
	conn, br := rawClient(t, h)
	c := <-accepted
	defer c.Close()

	if _, err := conn.Write(clientFrame(true, opPing, []byte("ping-data"))); err != nil {
		t.Fatal(err)
	}
	op, p := readServerFrame(t, br)
	if op != opPong || string(p) != "ping-data" {
		t.Fatalf("pong = op %d payload %q", op, p)
	}
}

// A client close is echoed, and Conn.Done fires.
func TestClientCloseEchoed(t *testing.T) {
	accepted := make(chan *Conn, 1)
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := Accept(w, r)
		if err != nil {
			t.Errorf("accept: %v", err)
			return
		}
		accepted <- c
		<-c.Done()
	})
	conn, br := rawClient(t, h)
	c := <-accepted

	payload := make([]byte, 2)
	binary.BigEndian.PutUint16(payload, 1000)
	if _, err := conn.Write(clientFrame(true, opClose, payload)); err != nil {
		t.Fatal(err)
	}
	op, p := readServerFrame(t, br)
	if op != opClose || len(p) < 2 || binary.BigEndian.Uint16(p[:2]) != 1000 {
		t.Fatalf("echoed close = op %d payload %v", op, p)
	}
	select {
	case <-c.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("Done did not fire after client close")
	}
}

// WriteClose sends the requested code and reason.
func TestWriteCloseCodeAndReason(t *testing.T) {
	accepted := make(chan *Conn, 1)
	release := make(chan struct{})
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := Accept(w, r)
		if err != nil {
			t.Errorf("accept: %v", err)
			return
		}
		accepted <- c
		<-release
	})
	_, br := rawClient(t, h)
	c := <-accepted
	go func() {
		_ = c.WriteClose(4403, "revoked")
		close(release)
	}()
	op, p := readServerFrame(t, br)
	if op != opClose || binary.BigEndian.Uint16(p[:2]) != 4403 || string(p[2:]) != "revoked" {
		t.Fatalf("close = op %d payload %q", op, p)
	}
	select {
	case <-c.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("Done did not fire after WriteClose")
	}
}
