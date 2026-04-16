package parser

import (
	"fmt"
	"log"
	"strconv"
	"strings"
)

var CRLF = "\r\n"

// Parse() accepts raw bytes and parses them using the RESP spec.
func Parse(payload []byte) (response string, err error) {
	// payload structure
	//*<number-of-elements>\r\n<element-1>...<element-n>
	// expect something like: *2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n. This is the RESP encoding of ["ECHO", "hey"].
	payloadString := string(payload)
	// split the payload into it's parts.
	// look for the first occurence of the CRLF , this first part is an asterisk and the number of elements in the array
	idx := strings.Index(payloadString, CRLF)
	if idx == -1 {
		return response, fmt.Errorf("Malformed RESP array")
	}
	// get the number of elements
	firstPart := payloadString[:idx]
	log.Println("First Part=>", firstPart)
	numberOfElements, _ := strconv.Atoi(firstPart[1:])
	// handle empty resp array
	if numberOfElements == 0 {
		log.Println("Empty RESP Array")
		return response, nil
	}
	remainingParts := payloadString[len(firstPart)+len(CRLF):]
	log.Println("Remaining parts:", remainingParts)
	// get the command.

	remainingPartsArray := strings.SplitN(remainingParts, CRLF, 2)
	log.Println("remainingPartsArray:", remainingPartsArray, "length:", len(remainingPartsArray))
	command := remainingPartsArray[1]
	log.Println("Command:", command)
	switch strings.ToLower(command) {
	case "echo":
		log.Println("Echo Response:", remainingPartsArray[2])
		response = remainingPartsArray[2]
	default:
		log.Println("invalid command")
		err = fmt.Errorf("invalid command")
	}
	return response, nil
}

func handleEcho() {}
