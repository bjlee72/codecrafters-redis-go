package main

import (
	"fmt"
	"log"
	"net"
	"os"
)

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}

	c, err := l.Accept()
	if err != nil {
		fmt.Println("Error accepting connection: ", err.Error())
		os.Exit(1)
	}

	buf := make([]byte, 128)
	_, err = c.Read(buf)
	if err != nil {
		fmt.Println("read command: ", err.Error())
		os.Exit(1)
	}
	log.Printf("read command:\n%s", buf)

	c.Write([]byte("+PONG\r\n"))
}
