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

	defer c.Close()

	err = run(c)
	if err != nil {
		fmt.Println("run failed: ", err.Error())
		os.Exit(1)
	}
}

func run(c net.Conn) error {
	buf := make([]byte, 128)
	_, err := c.Read(buf)
	if err != nil {
		return fmt.Errorf("read cmd failed: %v", err)
	}

	log.Printf("read command:\n%s", buf)

	_, err = c.Write([]byte("+PONG\r\n"))
	if err != nil {
		return fmt.Errorf("write response failed: %v", err)
	}

	return nil
}
