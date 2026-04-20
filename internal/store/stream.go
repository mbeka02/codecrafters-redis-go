package store

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

type StreamID struct {
	Ms  uint64
	Seq uint64
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
		seqAuto = true // force seq auto too
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
