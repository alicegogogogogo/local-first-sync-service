package server

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

// Minimal RFC 6455 WebSocket server-side codec. The subscription endpoint is
// the only WebSocket surface and it is push-only: the server sends text
// frames, the client may send control frames (ping/close), and any inbound
// data frame is read and discarded. No external dependency is introduced, so
// the module keeps its single SQLite dependency.

const (
	wsOpcodeContinue = 0x0
	wsOpcodeText     = 0x1
	wsOpcodeBinary   = 0x2
	wsOpcodeClose    = 0x8
	wsOpcodePing     = 0x9
	wsOpcodePong     = 0xA

	wsFlagFin  = 0x80
	wsFlagMask = 0x80

	// wsMaxInboundMessage caps a frame/assembled message sent by the client.
	// Clients are not expected to send anything but control frames; the cap
	// only protects the reader.
	wsMaxInboundMessage = 1 << 20
	// wsMaxControlPayload is the RFC cap on ping/pong/close payloads.
	wsMaxControlPayload = 125

	wsWriteTimeout = 10 * time.Second
	wsGUID         = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
)

// Close codes the subscription endpoint emits.
const (
	// wsCloseGoingAway (1001) ends every subscription when the process
	// receives its termination signal.
	wsCloseGoingAway = 1001
	// wsCloseProtocolError (1002) ends a connection whose frames violate
	// RFC 6455.
	wsCloseProtocolError = 1002
	// wsCloseInternalError (1011) ends a connection that hit an unexpected
	// server failure after the handshake.
	wsCloseInternalError = 1011
	// wsClosePermissionRevoked (4403) ends a subscription after its
	// session's device loses permission for the document.
	wsClosePermissionRevoked = 4403
)

// ErrWebSocketClosed is returned by ReadMessage when the peer sends a close
// frame; Code/Reason carry the peer's payload when present.
var ErrWebSocketClosed = errors.New("websocket close received")

type wsCloseError struct {
	Code   int
	Reason string
}

func (e *wsCloseError) Error() string {
	return fmt.Sprintf("%s: code=%d reason=%q", ErrWebSocketClosed, e.Code, e.Reason)
}
func (e *wsCloseError) Unwrap() error { return ErrWebSocketClosed }

// wsConn is one upgraded WebSocket connection. Writes are serialized; reads
// are single-owner (the read pump).
type wsConn struct {
	conn      net.Conn
	br        *bufio.Reader
	writeMu   sync.Mutex
	sentClose bool
}

// headerContainsToken reports whether a comma-list HTTP header (Connection is
// one) carries token, compared case-insensitively per RFC 7230 token rules.
func headerContainsToken(header, token string) bool {
	for _, part := range strings.Split(header, ",") {
		if strings.EqualFold(strings.TrimSpace(part), token) {
			return true
		}
	}
	return false
}

// checkWebSocketUpgrade validates the RFC 6455 opening handshake headers
// without hijacking the connection: a failed check is an ordinary 400 JSON
// error, so malformed requests never see a redirect, HTML or a 101. It does
// not touch the response otherwise.
func checkWebSocketUpgrade(r *http.Request) bool {
	if !headerContainsToken(r.Header.Get("Connection"), "upgrade") ||
		!strings.EqualFold(r.Header.Get("Upgrade"), "websocket") {
		return false
	}
	if r.Header.Get("Sec-WebSocket-Version") != "13" {
		return false
	}
	key := r.Header.Get("Sec-WebSocket-Key")
	decoded, err := base64.StdEncoding.DecodeString(key)
	if err != nil || len(decoded) != 16 {
		return false
	}
	return true
}

// secWebSocketAccept computes the Sec-WebSocket-Accept handshake value.
func secWebSocketAccept(key string) string {
	sum := sha1.Sum([]byte(key + wsGUID))
	return base64.StdEncoding.EncodeToString(sum[:])
}

// upgradeWebSocket hijacks the connection and writes the 101 response. Every
// precondition (method, cursor, session, permission, handshake headers) has
// already been checked by the caller; from here on failures are reported as
// WebSocket close frames rather than HTTP statuses.
func upgradeWebSocket(w http.ResponseWriter, r *http.Request) (*wsConn, error) {
	hj, ok := w.(http.Hijacker)
	if !ok {
		return nil, errors.New("connection does not support hijacking")
	}
	conn, brw, err := hj.Hijack()
	if err != nil {
		return nil, err
	}

	accept := secWebSocketAccept(r.Header.Get("Sec-WebSocket-Key"))
	response := "HTTP/1.1 101 Switching Protocols\r\n" +
		"Upgrade: websocket\r\n" +
		"Connection: Upgrade\r\n" +
		"Sec-WebSocket-Accept: " + accept + "\r\n\r\n"
	if _, err := io.WriteString(conn, response); err != nil {
		_ = conn.Close()
		return nil, err
	}
	// Flush anything the HTTP layer buffered before the hijack.
	if err := brw.Flush(); err != nil {
		_ = conn.Close()
		return nil, err
	}
	return &wsConn{conn: conn, br: brw.Reader}, nil
}

// readFrame reads one RFC 6455 frame, unmasking client payloads.
func (c *wsConn) readFrame() (fin bool, opcode byte, payload []byte, err error) {
	header := make([]byte, 2)
	if _, err = io.ReadFull(c.br, header); err != nil {
		return false, 0, nil, err
	}
	b0, b1 := header[0], header[1]
	if b0&0x70 != 0 {
		return false, 0, nil, wsProtocolError("reserved bits must be zero")
	}
	fin = b0&wsFlagFin != 0
	opcode = b0 & 0x0F
	masked := b1&wsFlagMask != 0
	length := int64(b1 & 0x7F)

	switch length {
	case 126:
		var ext [2]byte
		if _, err = io.ReadFull(c.br, ext[:]); err != nil {
			return false, 0, nil, err
		}
		length = int64(binary.BigEndian.Uint16(ext[:]))
	case 127:
		var ext [8]byte
		if _, err = io.ReadFull(c.br, ext[:]); err != nil {
			return false, 0, nil, err
		}
		length = int64(binary.BigEndian.Uint64(ext[:]))
		if length < 0 {
			return false, 0, nil, wsProtocolError("frame length too large")
		}
	}
	if opcode == wsOpcodeClose && length > wsMaxControlPayload {
		return false, 0, nil, wsProtocolError("close frame too large")
	}
	if opcode >= wsOpcodeClose && !fin {
		return false, 0, nil, wsProtocolError("control frame must not be fragmented")
	}
	if length > wsMaxInboundMessage {
		return false, 0, nil, wsProtocolError("frame too large")
	}

	var mask [4]byte
	if masked {
		if _, err = io.ReadFull(c.br, mask[:]); err != nil {
			return false, 0, nil, err
		}
	}
	payload = make([]byte, length)
	if length > 0 {
		if _, err = io.ReadFull(c.br, payload); err != nil {
			return false, 0, nil, err
		}
	}
	if masked {
		for i := range payload {
			payload[i] ^= mask[i%4]
		}
	} else {
		// RFC 6455 §5.1: a frame from a client MUST be masked.
		return false, 0, nil, wsProtocolError("client frame is not masked")
	}
	return fin, opcode, payload, nil
}

// ReadMessage returns the next complete data message, answering control
// frames inline: a ping is answered with a pong, a pong is ignored, and a
// close frame surfaces as ErrWebSocketClosed (the caller echoes it). Fragmented
// messages are reassembled in receive order.
func (c *wsConn) ReadMessage() (opcode byte, payload []byte, err error) {
	var assembled []byte
	started := false
	messageOpcode := byte(wsOpcodeText)

	for {
		fin, frameOpcode, framePayload, frameErr := c.readFrame()
		if frameErr != nil {
			return 0, nil, frameErr
		}

		switch frameOpcode {
		case wsOpcodeContinue:
			if !started {
				return 0, nil, wsProtocolError("continuation frame without a start frame")
			}
			assembled = append(assembled, framePayload...)
		case wsOpcodeText, wsOpcodeBinary:
			if started {
				return 0, nil, wsProtocolError("new data frame before the fragment finished")
			}
			started = true
			messageOpcode = frameOpcode
			assembled = append(assembled[:0], framePayload...)
		case wsOpcodeClose:
			closeErr := parseClosePayload(framePayload)
			if writeErr := c.writeCloseResponse(framePayload); writeErr != nil {
				_ = c.conn.Close()
			}
			return 0, nil, closeErr
		case wsOpcodePing:
			if len(framePayload) > wsMaxControlPayload {
				return 0, nil, wsProtocolError("ping payload too large")
			}
			if pingErr := c.writeControl(wsOpcodePong, framePayload); pingErr != nil {
				return 0, nil, pingErr
			}
			continue
		case wsOpcodePong:
			continue
		default:
			return 0, nil, wsProtocolError(fmt.Sprintf("unknown opcode %d", frameOpcode))
		}

		if len(assembled) > wsMaxInboundMessage {
			return 0, nil, wsProtocolError("message too large")
		}
		if fin {
			return messageOpcode, assembled, nil
		}
	}
}

func parseClosePayload(payload []byte) error {
	if len(payload) == 0 {
		return &wsCloseError{Code: wsCloseGoingAway, Reason: ""}
	}
	if len(payload) < 2 {
		return wsProtocolError("close payload too short")
	}
	code := int(binary.BigEndian.Uint16(payload[:2]))
	return &wsCloseError{Code: code, Reason: string(payload[2:])}
}

// writeFrame sends one server frame; server frames are never masked.
func (c *wsConn) writeFrame(opcode byte, fin bool, payload []byte) error {
	_ = c.conn.SetWriteDeadline(time.Now().Add(wsWriteTimeout))

	var b0 byte = opcode
	if fin {
		b0 |= wsFlagFin
	}

	var header [10]byte
	header[0] = b0
	n := 1
	switch {
	case len(payload) <= 125:
		header[1] = byte(len(payload))
		n++
	case len(payload) <= 65535:
		header[1] = 126
		binary.BigEndian.PutUint16(header[2:4], uint16(len(payload)))
		n += 3
	default:
		header[1] = 127
		binary.BigEndian.PutUint64(header[2:10], uint64(len(payload)))
		n += 9
	}
	if _, err := c.conn.Write(header[:n]); err != nil {
		return err
	}
	if len(payload) > 0 {
		if _, err := c.conn.Write(payload); err != nil {
			return err
		}
	}
	return nil
}

func (c *wsConn) writeControl(opcode byte, payload []byte) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return c.writeFrame(opcode, true, payload)
}

// WriteText sends one complete text frame.
func (c *wsConn) WriteText(payload []byte) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if c.sentClose {
		return errors.New("write after close frame")
	}
	return c.writeFrame(wsOpcodeText, true, payload)
}

// writeCloseResponse echoes a client close frame when the server has not yet
// sent its own; it is the read-pump side of the closing handshake.
func (c *wsConn) writeCloseResponse(payload []byte) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if c.sentClose {
		return nil
	}
	c.sentClose = true
	return c.writeFrame(wsOpcodeClose, true, payload)
}

// Close sends the closing handshake with code/reason exactly once. It does not
// block on the peer's echo and never reads: the handler owns one read pump,
// and concurrent readers on the buffered connection are unsafe. The handler
// waits on its read pump and then calls NetClose.
func (c *wsConn) Close(code int, reason string) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if c.sentClose {
		return nil
	}
	c.sentClose = true
	payload := make([]byte, 2+len(reason))
	binary.BigEndian.PutUint16(payload, uint16(code))
	copy(payload[2:], reason)
	return c.writeFrame(wsOpcodeClose, true, payload)
}

// NetClose drops the underlying TCP connection. The read pump unblocks with a
// read error; a double close (read pump and handler) is harmless.
func (c *wsConn) NetClose() error { return c.conn.Close() }

type wsProtocolErrorString struct{ msg string }

func (e *wsProtocolErrorString) Error() string { return "websocket protocol error: " + e.msg }

func wsProtocolError(msg string) error { return &wsProtocolErrorString{msg: msg} }
