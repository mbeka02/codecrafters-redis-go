package main

import (
	"errors"
	"fmt"
	"log"
	"net"
	"os"

	"github.com/codecrafters-io/redis-starter-go/internal/parser"
	"github.com/codecrafters-io/redis-starter-go/internal/store"
)

func handleConnection(conn net.Conn) {
	for {
		// defer func() {
		// 	log.Println("...closing the connection")
		// 	conn.Close()
		// }()

		log.Println("...handling connection from:", conn.RemoteAddr())
		buf := make([]byte, 1024)
		n, err := conn.Read(buf)
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				log.Println("Connection closed", err)
			} else {
				log.Println("Error reading the bytes sent:", err)
			}
			return
		}
		store := store.NewStore()
		response, err := parser.Parse(buf[:n], store) // only parse upto what was read
		if err != nil {
			log.Println("parse error:", err)
			conn.Write([]byte("-ERR " + err.Error() + "\r\n")) // send RESP error to client
			continue
		}
		_, err = conn.Write([]byte(response))
		if err != nil {
			log.Println("Write error:", err)
			return
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
