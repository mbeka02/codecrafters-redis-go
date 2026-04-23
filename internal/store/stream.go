package store

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
)

type StreamEntry struct {
	Id     StreamID
	Fields map[string]string
}
type StreamID struct {
	Ms  uint64
	Seq uint64
}

// Streams
func (s *Store) XRange(key string, start, end StreamID) ([]StreamEntry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	entries := s.data[key].Stream
	// binary search
	// find first entry >= start
	lo := sort.Search(len(entries), func(i int) bool {
		return !idBefore(entries[i].Id, start) // entries[i].Id >= start
	})

	// find first entry > end, everything before it is <= end
	hi := sort.Search(len(entries), func(i int) bool {
		return idBefore(end, entries[i].Id) // entries[i].Id > end
	})

	return entries[lo:hi], nil
}

func idBefore(a, b StreamID) bool {
	if a.Ms != b.Ms {
		return a.Ms < b.Ms
	}
	return a.Seq < b.Seq
}

func (s *Store) XAdd(key, rawID string, fields map[string]string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	val := s.data[key]

	var last *StreamID
	if len(val.Stream) > 0 {
		tip := val.Stream[len(val.Stream)-1].Id
		last = &tip
	}

	id, err := resolveID(rawID, last)
	if err != nil {
		return "", err
	}

	val.Stream = append(val.Stream, StreamEntry{
		Id:     id,
		Fields: fields,
	})
	s.data[key] = val

	return fmt.Sprintf("%d-%d", id.Ms, id.Seq), nil
}

func parseStreamID(raw string) (StreamID, bool, bool, error) {
	// returns: id, msAuto, seqAuto, err
	if raw == "*" {
		return StreamID{}, true, true, nil
	}

	parts := strings.SplitN(raw, "-", 2)
	if len(parts) != 2 {
		return StreamID{}, false, false, fmt.Errorf("invalid stream ID format: %q", raw)
	}

	ms, err := strconv.ParseUint(parts[0], 10, 64)
	if err != nil {
		return StreamID{}, false, false, fmt.Errorf("invalid ms part: %w", err)
	}

	if parts[1] == "*" {
		return StreamID{Ms: ms}, false, true, nil // ms fixed, seq auto
	}

	seq, err := strconv.ParseUint(parts[1], 10, 64)
	if err != nil {
		return StreamID{}, false, false, fmt.Errorf("invalid seq part: %w", err)
	}

	return StreamID{Ms: ms, Seq: seq}, false, false, nil
}

func resolveID(raw string, last *StreamID) (StreamID, error) {
	parsed, msAuto, seqAuto, err := parseStreamID(raw)
	if err != nil {
		return StreamID{}, err
	}

	now := uint64(time.Now().UnixMilli())

	if msAuto {
		parsed.Ms = now
	}

	if seqAuto {
		parsed.Seq = nextSeq(parsed.Ms, last)
	}
	// special case: 0-0 is never valid
	if parsed.Ms == 0 && parsed.Seq == 0 {
		return StreamID{}, fmt.Errorf("The ID specified in XADD must be greater than 0-0")
	}

	// validate strictly greater than last
	if last != nil {
		if parsed.Ms < last.Ms || (parsed.Ms == last.Ms && parsed.Seq <= last.Seq) {
			return StreamID{}, fmt.Errorf(
				"The ID specified in XADD is equal or smaller than the target stream top item",
			)
		}
	}

	return parsed, nil
}

func nextSeq(ms uint64, last *StreamID) uint64 {
	if last == nil {
		// ms=0 is a special case Redis handles — seq starts at 1
		if ms == 0 {
			return 1
		}
		return 0
	}
	if ms == last.Ms {
		return last.Seq + 1 // same millisecond, increment
	}
	return 0 // new millisecond, reset
}
