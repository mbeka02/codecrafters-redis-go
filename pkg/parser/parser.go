package parser

import (
	"fmt"
	"strconv"
	"strings"
)

const CRLF = "\r\n"

// Parser holds the raw payload and a cursor position.
type Parser struct {
	data   string
	cursor int
}

func newParser(payload []byte) *Parser {
	return &Parser{data: string(payload), cursor: 0}
}

// readLine reads up to the next CRLF, advances the cursor past it, and
// returns the line content (without the CRLF).
func (p *Parser) readLine() (string, error) {
	idx := strings.Index(p.data[p.cursor:], CRLF)
	if idx == -1 {
		return "", fmt.Errorf("expected CRLF, none found (cursor=%d)", p.cursor)
	}
	line := p.data[p.cursor : p.cursor+idx]
	p.cursor += idx + len(CRLF)
	return line, nil
}

// readBulkString parses a RESP bulk string: $<len>\r\n<data>\r\n
func (p *Parser) readBulkString() (string, error) {
	line, err := p.readLine()
	if err != nil {
		return "", err
	}
	if len(line) == 0 || line[0] != '$' {
		return "", fmt.Errorf("expected bulk string prefix '$', got %q", line)
	}

	expectedLen, err := strconv.Atoi(line[1:])
	if err != nil {
		return "", fmt.Errorf("invalid bulk string length %q: %w", line[1:], err)
	}

	// Null bulk string ($-1)
	if expectedLen == -1 {
		return "", nil
	}

	data, err := p.readLine()
	if err != nil {
		return "", err
	}
	if len(data) != expectedLen {
		return "", fmt.Errorf("bulk string length mismatch: declared %d, got %d", expectedLen, len(data))
	}
	return data, nil
}

// Parse accepts raw bytes and parses them as a RESP array.
// Returns the server response string to write back to the client.
func Parse(payload []byte) (string, error) {
	p := newParser(payload)

	// --- 1. Parse the array header: *<count>\r\n ---
	header, err := p.readLine()
	if err != nil || len(header) == 0 || header[0] != '*' {
		return "", fmt.Errorf("malformed RESP array header: %w", err)
	}

	count, err := strconv.Atoi(header[1:])
	if err != nil {
		return "", fmt.Errorf("invalid element count %q: %w", header[1:], err)
	}
	if count == 0 {
		return "", nil
	}

	// --- 2. Read all bulk string elements into a slice ---
	elements := make([]string, 0, count)
	for i := 0; i < count; i++ {
		val, err := p.readBulkString()
		if err != nil {
			return "", fmt.Errorf("error reading element %d: %w", i, err)
		}
		elements = append(elements, val)
	}

	// --- 3. Dispatch on the command ---
	command := strings.ToUpper(elements[0])
	switch command {
	case "ECHO":
		return handleEcho(elements)
	case "PING":
		return "+PONG\r\n", nil
	default:
		return "", fmt.Errorf("unknown command: %q", command)
	}
}

// handleEcho validates args and returns the RESP bulk string response.
func handleEcho(elements []string) (string, error) {
	if len(elements) < 2 {
		return "", fmt.Errorf("ECHO requires exactly 1 argument, got %d", len(elements)-1)
	}
	msg := elements[1]
	// Encode the response as a RESP bulk string: $<len>\r\n<data>\r\n
	return fmt.Sprintf("$%d\r\n%s\r\n", len(msg), msg), nil
}
