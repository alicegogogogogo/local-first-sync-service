// Package ws implements the server side of the small slice of RFC 6455 the
// document subscription channel needs: the opening handshake, unmasked
// server-to-client frames, and a reader that answers pings, echoes closes and
// ignores every inbound data message (the channel pushes only; clients cannot
// commit, modify or delete anything over it).
//
// Nothing here is durable: a Conn wraps one hijacked TCP connection and a
// disconnect tears down only that connection.
package ws

import (
	"bufio"
	"crypto/sha1"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

// websocketGUID is the fixed GUID from RFC 6455 §4.2.2 used to derive the
// Sec-WebSocket-Accept value.
const websocketGUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

// Frame opcodes (RFC 6455 §5.2).
const (
	opContinuation = 0x0
	opText         = 0x1
	opBinary       = 0x2
	opClose        = 0x8
	opPing         = 0x9
	opPong         = 0xA
)

// Close codes the subscription channel sends.
const (
	// CloseGoingAway is sent to every subscription when the process receives
	// its termination signal (RFC 6455 1001).
	CloseGoingAway uint16 = 1001
	// ClosePermissionRevoked (private range, RFC 6455 §7.4.2) ends a
	// subscription whose session device loses access to the document after the
	// connection was established.
	ClosePermissionRevoked uint16 = 4403
)

// ErrBadHandshake reports that an upgrade request is missing the required
// WebSocket headers. The HTTP handler maps it to a 400 JSON error.
var ErrBadHandshake = errors.New("missing or invalid WebSocket upgrade handshake")

// ValidateUpgradeHeaders reports whether r carries a valid RFC 6455 opening
// handshake for protocol version 13: a GET carrying "Connection: upgrade",
// "Upgrade: websocket", a well-formed Sec-WebSocket-Key and
// Sec-WebSocket-Version: 13. It inspects headers only; the response is still
// an ordinary HTTP error at this point so the caller can order its 400 checks
// before resource lookups.
func ValidateUpgradeHeaders(r *http.Request) error {
	if !headerToken(r.Header, "Connection", "upgrade") {
		return ErrBadHandshake
	}
	if !strings.EqualFold(strings.TrimSpace(r.Header.Get("Upgrade")), "websocket") {
		return ErrBadHandshake
	}
	if strings.TrimSpace(r.Header.Get("Sec-WebSocket-Version")) != "13" {
		return ErrBadHandshake
	}
	key := r.Header.Get("Sec-WebSocket-Key")
	raw, err := base64.StdEncoding.DecodeString(key)
	if err != nil || len(raw) != 16 {
		return ErrBadHandshake
	}
	return nil
}

// acceptKey derives the Sec-WebSocket-Accept value for a client key.
func acceptKey(clientKey string) string {
	sum := sha1.Sum([]byte(clientKey + websocketGUID))
	return base64.StdEncoding.EncodeToString(sum[:])
}

// headerToken reports whether the comma-separated token list in header name
// contains want, compared case-insensitively (e.g. Connection: keep-alive,
// Upgrade).
func headerToken(h http.Header, name, want string) bool {
	for _, part := range strings.Split(h.Get(name), ",") {
		if strings.EqualFold(strings.TrimSpace(part), want) {
			return true
		}
	}
	return false
}

// writeTimeout bounds every server write so a wedged client cannot pin a
// graceful shutdown (the 1001 close frame) indefinitely.
const writeTimeout = 5 * time.Second

// Conn is one accepted WebSocket connection. Writes are serialized; one
// background reader services control frames. Close is safe to call more than
// once.
type Conn struct {
	rwc net.Conn
	br  *bufio.Reader
	bw  *bufio.Writer

	writeMu sync.Mutex

	// done is closed once the peer has gone: a close frame, an EOF/reset on
	// the read side, or a local Close. The push handler selects on it to
	// release the subscription immediately.
	done     chan struct{}
	doneOnce sync.Once

	closeSent bool
}

// Accept performs the opening handshake: it writes the 101 response and hijacks
// the connection, then starts the control-frame reader. The caller must have
// validated the request headers with ValidateUpgradeHeaders and completed all
// ordinary HTTP error checks (404/403) first; after Accept the connection is a
// WebSocket and no further HTTP response is possible.
func Accept(w http.ResponseWriter, r *http.Request) (*Conn, error) {
	hj, ok := w.(http.Hijacker)
	if !ok {
		return nil, errors.New("server does not support hijacking")
	}
	conn, rw, err := hj.Hijack()
	if err != nil {
		return nil, fmt.Errorf("hijack connection: %w", err)
	}

	accept := acceptKey(r.Header.Get("Sec-WebSocket-Key"))
	_ = conn.SetWriteDeadline(time.Now().Add(writeTimeout))
	fmt.Fprintf(rw, "HTTP/1.1 101 Switching Protocols\r\n")
	fmt.Fprintf(rw, "Upgrade: websocket\r\n")
	fmt.Fprintf(rw, "Connection: Upgrade\r\n")
	fmt.Fprintf(rw, "Sec-WebSocket-Accept: %s\r\n", accept)
	fmt.Fprintf(rw, "\r\n")
	if err := rw.Flush(); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("write handshake: %w", err)
	}
	_ = conn.SetWriteDeadline(time.Time{})

	c := &Conn{
		rwc:  conn,
		br:   rw.Reader,
		bw:   rw.Writer,
		done: make(chan struct{}),
	}
	go c.readLoop()
	return c, nil
}

// Done is closed when the peer disconnects, sends a close frame, or Close is
// called locally.
func (c *Conn) Done() <-chan struct{} { return c.done }

func (c *Conn) signalDone() {
	c.doneOnce.Do(func() { close(c.done) })
}

// frameHeader writes a server frame (always unmasked per RFC 6455 §5.3) with
// the given final-fragment opcode and payload length.
func writeFrameHeader(bw *bufio.Writer, opcode byte, n int) error {
	if err := bw.WriteByte(0x80 | opcode); err != nil { // FIN=1
		return err
	}
	switch {
	case n <= 125:
		if err := bw.WriteByte(byte(n)); err != nil {
			return err
		}
	case n <= 0xFFFF:
		if err := bw.WriteByte(126); err != nil {
			return err
		}
		var ext [2]byte
		binary.BigEndian.PutUint16(ext[:], uint16(n))
		if _, err := bw.Write(ext[:]); err != nil {
			return err
		}
	default:
		if err := bw.WriteByte(127); err != nil {
			return err
		}
		var ext [8]byte
		binary.BigEndian.PutUint64(ext[:], uint64(n))
		if _, err := bw.Write(ext[:]); err != nil {
			return err
		}
	}
	return nil
}

// writeFrame sends one final, unmasked frame.
func (c *Conn) writeFrame(opcode byte, payload []byte) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	_ = c.rwc.SetWriteDeadline(time.Now().Add(writeTimeout))
	defer c.rwc.SetWriteDeadline(time.Time{})
	if err := writeFrameHeader(c.bw, opcode, len(payload)); err != nil {
		return err
	}
	if len(payload) > 0 {
		if _, err := c.bw.Write(payload); err != nil {
			return err
		}
	}
	return c.bw.Flush()
}

// WriteText sends one text message, which for the subscription channel is one
// JSON change record.
func (c *Conn) WriteText(payload []byte) error {
	return c.writeFrame(opText, payload)
}

// WriteClose sends a close frame carrying code and reason and then closes the
// underlying connection. It is the orderly end used for revocation (4403) and
// shutdown (1001).
func (c *Conn) WriteClose(code uint16, reason string) error {
	c.writeMu.Lock()
	var writeErr error
	if !c.closeSent {
		_ = c.rwc.SetWriteDeadline(time.Now().Add(writeTimeout))
		payload := make([]byte, 2+len(reason))
		binary.BigEndian.PutUint16(payload, code)
		copy(payload[2:], reason)
		if err := writeFrameHeader(c.bw, opClose, len(payload)); err != nil {
			writeErr = err
		} else if _, err := c.bw.Write(payload); err != nil {
			writeErr = err
		} else if err := c.bw.Flush(); err != nil {
			writeErr = err
		}
		_ = c.rwc.SetWriteDeadline(time.Time{})
		c.closeSent = true
	}
	c.writeMu.Unlock()

	c.signalDone()
	if err := c.rwc.Close(); err != nil && writeErr == nil {
		writeErr = err
	}
	return writeErr
}

// Close tears down the connection without sending a close frame (used when the
// peer already went away or a write failed).
func (c *Conn) Close() error {
	c.signalDone()
	return c.rwc.Close()
}

// readLoop services inbound frames until the peer closes or the read fails. It
// answers pings with pongs, echoes closes, and discards data messages: the
// subscription channel is push-only.
func (c *Conn) readLoop() {
	defer c.signalDone()
	for {
		opcode, payload, err := readFrame(c.br)
		if err != nil {
			_ = c.rwc.Close()
			return
		}
		switch opcode {
		case opClose:
			// Echo the close (RFC 6455 §5.5.1) and shut down.
			c.writeMu.Lock()
			if !c.closeSent {
				if err := writeFrameHeader(c.bw, opClose, len(payload)); err == nil {
					_, _ = c.bw.Write(payload)
					_ = c.bw.Flush()
				}
				c.closeSent = true
			}
			c.writeMu.Unlock()
			_ = c.rwc.Close()
			return
		case opPing:
			// A pong echoes the ping's application data. A write failure means
			// the peer is gone; fall through to close on the next read.
			_ = c.writeFrame(opPong, payload)
		case opPong, opText, opBinary, opContinuation:
			// Pong and every inbound data message are ignored: the channel
			// carries no client-to-server semantics.
		default:
			// Unknown/reserved opcode: close per RFC 6455 §7.4.1 with 1002.
			_ = c.WriteClose(1002, "protocol error")
			return
		}
	}
}

// readFrame reads one client frame (masked per the RFC; a frame without a
// mask key is a protocol error), unmasking the payload in place. Fragmented
// frames are not reassembled — data frames are discarded — but control frames
// always arrive unfragmented, so each read stands on its own here.
func readFrame(br *bufio.Reader) (opcode byte, payload []byte, err error) {
	var head [2]byte
	if _, err = io.ReadFull(br, head[:]); err != nil {
		return 0, nil, err
	}
	opcode = head[0] & 0x0F
	masked := head[1]&0x80 != 0
	length := int64(head[1] & 0x7F)
	switch length {
	case 126:
		var ext [2]byte
		if _, err = io.ReadFull(br, ext[:]); err != nil {
			return 0, nil, err
		}
		length = int64(binary.BigEndian.Uint16(ext[:]))
	case 127:
		var ext [8]byte
		if _, err = io.ReadFull(br, ext[:]); err != nil {
			return 0, nil, err
		}
		length = int64(binary.BigEndian.Uint64(ext[:]))
	}
	var mask [4]byte
	if masked {
		if _, err = io.ReadFull(br, mask[:]); err != nil {
			return 0, nil, err
		}
	}
	if length < 0 || length > 16<<20 {
		return 0, nil, errors.New("frame payload too large")
	}
	payload = make([]byte, length)
	if length > 0 {
		if _, err = io.ReadFull(br, payload); err != nil {
			return 0, nil, err
		}
	}
	if masked {
		for i := range payload {
			payload[i] ^= mask[i%4]
		}
	} else if opcode != opPong {
		// Client frames sent without masking are a protocol violation (RFC
		// 6455 §5.1); a server-sent pong never arrives here.
		return 0, nil, errors.New("client frame was not masked")
	}
	return opcode, payload, nil
}
