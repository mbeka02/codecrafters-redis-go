package main

import (
	"fmt"
	"net"
	"os"
)

func main() {
	listener, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	conn, err := listener.Accept()
	if err != nil {
		fmt.Println("Error accepting connection: ", err.Error())
		os.Exit(1)
	}
	resp := "+PONG\r\n"
	_, err = conn.Write([]byte(resp))
	if err != nil {
		fmt.Println("Erroor writing bytes over the TCP connection")
		// TODO: Handle this differently don't just crash the program
		os.Exit(1)
	}
}
