package main

import (
	"fmt"
	"log"
	"net"
	"os"

	"github.com/codecrafters-io/redis-starter-go/pkg/parser"
)

func handleConnection(conn net.Conn) {
	for {
		// defer func() {
		// 	log.Println("...closing the connection")
		// 	conn.Close()
		// }()

		log.Println("...handling connection from:", conn.RemoteAddr())
		buf := make([]byte, 1024)
		_, err := conn.Read(buf)
		if err != nil {
			log.Println("Error reading the bytes sent")
			// TODO: Handle this differently
			os.Exit(1)

		}
		// log.Println("Received:", string(buf))
		// resp := "+PONG\r\n"
		response, err := parser.Parse(buf)
		if err != nil {
			log.Println("Error parsing the request:", err)
			// TODO: Handle this differently
			os.Exit(1)

		}
		_, err = conn.Write([]byte(response))
		if err != nil {
			log.Println("Error writing bytes over the TCP connection")
			// TODO: Handle this differently
			os.Exit(1)
		}
	}
}

func main() {
	listener, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	defer listener.Close()
	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		go handleConnection(conn)
	}
}
