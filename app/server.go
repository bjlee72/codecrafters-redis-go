package main

import (
	"bufio"
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

	for {
		c, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}

		// fork goroutine
		go commandLoop(c)
	}
}

func commandLoop(c net.Conn) error {
	defer c.Close()

	reader := bufio.NewReader(c)
	var taken string
	for {
		bytes, isPrefix, err := reader.ReadLine()
		if err != nil {
			return fmt.Errorf("reader.Readline: %v", err)
		}
		taken += string(bytes)
		if !isPrefix {
			err := handleCommand(taken, c)
			if err != nil {
				return fmt.Errorf("handleCommand: %v", err)
			}

			taken = ""
		}
	}
}

func handleCommand(cmd string, c net.Conn) error {
	log.Printf("read command: '%s'\n", cmd)

	if cmd == "PING" {
		err := handlePing(c)
		if err != nil {
			return fmt.Errorf("handlePing: %v", err)
		}
		return nil
	}
	return nil
}

func handlePing(c net.Conn) error {
	_, err := c.Write([]byte("+PONG\r\n"))
	if err != nil {
		return fmt.Errorf("write response failed: %v", err)
	}

	return nil
}
