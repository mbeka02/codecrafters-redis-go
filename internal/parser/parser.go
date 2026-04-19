package parser

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/codecrafters-io/redis-starter-go/internal/store"
)

const CRLF = "\r\n"

// Parser holds the raw payload , cursor position and a reference to the storage engine
type Parser struct {
	data   string
	cursor int
	store  *store.Store
}

func newParser(payload []byte, s *store.Store) *Parser {
	return &Parser{data: string(payload), cursor: 0, store: s}
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
func Parse(payload []byte, s *store.Store) (string, error) {
	p := newParser(payload, s)

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
		return p.handleEcho(elements)
	case "PING":
		return p.handlePing()
	case "SET":
		return p.handleSet(elements)
	case "GET":
		return p.handleGet(elements)
	case "RPUSH":
		return p.handleRPush(elements)
	case "LPUSH":
		return p.handleLPush(elements)
	case "LRANGE":
		return p.handleLRange(elements)
	case "LLEN":
		return p.handleLLen(elements)
	case "LPOP":
		return p.handleLPop(elements)
	case "BLPOP":
		return p.handleBLPop(elements)
	default:
		return "", fmt.Errorf("unknown command: %q", command)
	}
}

func (p *Parser) handlePing() (string, error) {
	return "+PONG\r\n", nil
}

// handleSet sets a key to a value and return a RESP simple string
func (p *Parser) handleSet(elements []string) (string, error) {
	if len(elements) < 3 {
		return "", fmt.Errorf("SET requires at least 2 arguments, got %d", len(elements)-1)
	}

	key := elements[1]
	val := elements[2]

	var expiresAt *time.Time // nil means no expiry

	// options are optional — only parse if present
	if len(elements) >= 5 {
		option := strings.ToUpper(elements[3])
		optionValue, err := strconv.Atoi(elements[4])
		if err != nil {
			return "", fmt.Errorf("invalid value for option %q: %w", option, err)
		}
		switch option {
		case "EX":
			t := time.Now().Add(time.Duration(optionValue) * time.Second)
			expiresAt = &t
		case "PX":
			t := time.Now().Add(time.Duration(optionValue) * time.Millisecond)
			expiresAt = &t
		default:
			return "", fmt.Errorf("unsupported SET option: %q", option)
		}
	}

	p.store.Set(key, store.Value{Data: val, ExpiresAt: expiresAt})
	return "+OK\r\n", nil
}

// handleGet gets the value using a key and return a RESP bulk string if the value exists and an empty bulk string if it doesn't
func (p *Parser) handleGet(elements []string) (string, error) {
	// fmt.Println("GET elements:", elements)
	if len(elements) < 2 {
		return "", fmt.Errorf("GET requires exactly 1 argument, got %d", len(elements)-1)
	}
	key := elements[1]
	value, exists := p.store.Get(key)
	if exists {
		return fmt.Sprintf("$%d\r\n%s\r\n", len(value.Data), value.Data), nil
	} else {
		return "$-1\r\n", nil
	}
}

// handleEcho validates args and returns the RESP bulk string response.
func (p *Parser) handleEcho(elements []string) (string, error) {
	if len(elements) < 2 {
		return "", fmt.Errorf("ECHO requires exactly 1 argument, got %d", len(elements)-1)
	}
	msg := elements[1]
	// Encode the response as a RESP bulk string: $<len>\r\n<data>\r\n
	return fmt.Sprintf("$%d\r\n%s\r\n", len(msg), msg), nil
}

func (p *Parser) handleRPush(elements []string) (string, error) {
	if len(elements) < 3 {
		return "", fmt.Errorf("RPUSH requires at least 2 arguments, got %d", len(elements)-1)
	}

	key := elements[1]
	newItems := elements[2:]

	existing, _ := p.store.Get(key) // returns zero Value if the key doesn't exist
	updated := append(existing.List, newItems...)
	p.store.Set(key, store.Value{List: updated})

	return fmt.Sprintf(":%d\r\n", len(updated)), nil
}

func (p *Parser) handleLPush(elements []string) (string, error) {
	if len(elements) < 3 {
		return "", fmt.Errorf("LPUSH requires at least 2 arguments")
	}

	key := elements[1]
	values := elements[2:]

	length := p.store.LPush(key, values)

	return fmt.Sprintf(":%d\r\n", length), nil
}

func (p *Parser) handleBLPop(elements []string) (string, error) {
	if len(elements) < 3 {
		return "", fmt.Errorf("BLPOP requires key and timeout")
	}

	key := elements[1]

	timeoutSec, err := strconv.Atoi(elements[2])
	if err != nil {
		return "", fmt.Errorf("invalid timeout")
	}

	timeout := time.Duration(timeoutSec) * time.Second

	val, ok := p.store.BLPop(key, timeout)

	if !ok {
		return "$-1\r\n", nil
	}

	return fmt.Sprintf("*2\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n",
		len(key), key, len(val), val), nil
}

// func (p *Parser) handleLPush(elements []string) (string, error) {
// 	if len(elements) < 3 {
// 		return "", fmt.Errorf("LPUSH requires at least 2 arguments, got %d", len(elements)-1)
// 	}
//
// 	key := elements[1]
// 	newItems := elements[2:]
//
// 	// order the appropriately
// 	for left, right := 0, len(newItems)-1; left < right; left, right = left+1, right-1 {
// 		newItems[left], newItems[right] = newItems[right], newItems[left]
// 	}
//
// 	existing, _ := p.store.Get(key) // returns zero Value if the key doesn't exist
// 	updated := append(newItems, existing.List...)
// 	p.store.Set(key, store.Value{List: updated})
//
// 	return fmt.Sprintf(":%d\r\n", len(updated)), nil
// }

func (p *Parser) handleLRange(elements []string) (string, error) {
	if len(elements) < 4 {
		return "", fmt.Errorf("LRange requires at least 3 arguments, got %d", len(elements)-1)
	}

	key := elements[1]
	startIdx, _ := strconv.Atoi(elements[2])
	endIdx, _ := strconv.Atoi(elements[3])

	existing, _ := p.store.Get(key)
	list := existing.List
	listLength := len(list)

	// empty list
	if listLength == 0 {
		return "*0\r\n", nil
	}

	// Handles negative indices
	if startIdx < 0 {
		startIdx += listLength
	}
	if endIdx < 0 {
		endIdx += listLength
	}

	// Clamp indices
	if startIdx < 0 {
		startIdx = 0
	}
	if endIdx >= listLength {
		endIdx = listLength - 1
	}

	if startIdx >= listLength || startIdx > endIdx {
		return "*0\r\n", nil
	}

	rangedList := list[startIdx : endIdx+1]

	// build RESP response
	bulkString := fmt.Sprintf("*%d\r\n", len(rangedList))
	for _, value := range rangedList {
		bulkString += fmt.Sprintf("$%d\r\n%s\r\n", len(value), value)
	}

	return bulkString, nil
}

func (p *Parser) handleLLen(elements []string) (string, error) {
	if len(elements) < 2 {
		return "", fmt.Errorf("LLEN requires exactly 1 argument, got %d", len(elements)-1)
	}
	key := elements[1]
	existing, _ := p.store.Get(key)
	return fmt.Sprintf(":%d\r\n", len(existing.List)), nil
}

// func (p *Parser) handleLPop(elements []string) (string, error) {
// 	if len(elements) < 2 {
// 		return "", fmt.Errorf("LPOP requires at least 1 argument")
// 	}
//
// 	key := elements[1]
//
// 	// default count = 1
// 	count := 1
// 	if len(elements) >= 3 {
// 		c, err := strconv.Atoi(elements[2])
// 		if err != nil || c <= 0 {
// 			return "", fmt.Errorf("invalid count")
// 		}
// 		count = c
// 	}
//
// 	existing, _ := p.store.Get(key)
// 	list := existing.List
//
// 	if len(list) == 0 {
// 		return "$-1\r\n", nil
// 	}
//
// 	if count > len(list) {
// 		count = len(list)
// 	}
//
// 	popped := list[:count]
// 	updated := list[count:]
//
// 	p.store.Set(key, store.Value{List: updated})
//
// 	// return array if multiple
// 	if count == 1 {
// 		element := popped[0]
// 		return fmt.Sprintf("$%d\r\n%s\r\n", len(element), element), nil
// 	}
//
// 	// RESP array
// 	response := fmt.Sprintf("*%d\r\n", len(popped))
// 	for _, el := range popped {
// 		response += fmt.Sprintf("$%d\r\n%s\r\n", len(el), el)
// 	}
//
// 	return response, nil
// }

func (p *Parser) handleLPop(elements []string) (string, error) {
	if len(elements) < 2 {
		return "", fmt.Errorf("LPOP requires at least 1 argument")
	}

	key := elements[1]

	count := 1
	if len(elements) >= 3 {
		c, err := strconv.Atoi(elements[2])
		if err != nil || c <= 0 {
			return "", fmt.Errorf("invalid count")
		}
		count = c
	}

	popped, ok := p.store.LPop(key, count)
	if !ok {
		return "$-1\r\n", nil
	}

	if len(popped) == 1 {
		el := popped[0]
		return fmt.Sprintf("$%d\r\n%s\r\n", len(el), el), nil
	}

	response := fmt.Sprintf("*%d\r\n", len(popped))
	for _, el := range popped {
		response += fmt.Sprintf("$%d\r\n%s\r\n", len(el), el)
	}

	return response, nil
}
